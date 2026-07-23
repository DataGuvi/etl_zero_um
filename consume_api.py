import requests
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime, timedelta, date
from config import API_AUTH, API_USER_ZEROUM, API_PASS_ZEROUM, API_USER_ENERGIABET, API_PASS_ENERGIABET, API_ROTA_CSV, DB
from enums import MetabaseTable, MetabaseDatabase, MetabaseCard
from database import ConnectionDB
import logging
from logging.handlers import RotatingFileHandler
from send_email import send_email
from util import Util
import os
from sqlalchemy import create_engine
from sqlalchemy import text
import psycopg2
from agregacao_dim_usuario import executar_agregacao_dim_usuario
from db_logger import DBLogger
from agregacao_cohort_retencao import executar_agregacao_cohort_retencao, executar_kpi_diario_datatalk

class ConsumeAPI:

    LIMITE_LINHAS_METABASE = 1048575  # teto de exportação CSV do Metabase (2^20 - 1)

    TETO_CLIENT_ID_METABASE = 50_000_000
 
   
    def __init__(self, cliente,  modo="incremental", data_final=None, data_inicial_backfill=None, ids_backfill=None):
        self.engine_zro1bet = self.create_engine_zro1bet()
        self.engine_dw = self.create_engine_dw()
        handler = RotatingFileHandler(
            'etl.log', 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(handler)
        logging.basicConfig(handlers=[handler], level=logging.INFO)
        self.db_logger = DBLogger(cliente=cliente)
        if cliente == 'ZEROUM':
            self.principal_zeroum()
        elif cliente == 'ZEROUM_VALIDACAO':
            self.valida_dados('ZEROUM')
        elif cliente == 'ENERGIABET':
            self.principal_energiabet()
        elif cliente == 'ENERGIABET_VALIDACAO':
            self.valida_dados('ENERGIABET')
        elif cliente == 'sobe_dados':
            self.sobe_dados()
        elif cliente == 'ZRO_1_BET':
            self.principal_zro_1_bet(modo=modo)
        elif cliente == 'ZEROUM_SALDO':
            self.processa_saldo_diario('ZEROUM', modo=modo, data_final=data_final)
        elif cliente == 'ENERGIABET_SALDO':
            self.processa_saldo_diario('ENERGIABET', modo=modo, data_final=data_final)
        elif cliente == 'ZEROUM_VALIDA_APOSTA':
            self.valida_aposta('ZEROUM', partner_id=180)
        elif cliente == 'ENERGIABET_VALIDA_APOSTA':
            self.logger.error(
                "ENERGIABET_VALIDA_APOSTA: PartnerId da EnergiaBet na tabela Bet "
                "ainda não confirmado — chame valida_aposta('ENERGIABET', partner_id=<valor correto>) manualmente."
            )
        elif cliente == 'ZEROUM_BACKFILL_USUARIO':
            # Uso único: cobre o intervalo em que dim_usuario ficou parada por
            # causa do MEMORY_LIMIT_EXCEEDED na aposta (ver conversa) — depois
            # de confirmar que principal_zeroum roda até o fim sem erro,
            # rodar uma vez com --data-inicial-backfill=2026-07-19T00:00:00
            # (ou a data do último updated_at bem-sucedido de dim_usuario).
            if not data_inicial_backfill:
                self.logger.error("ZEROUM_BACKFILL_USUARIO requer --data-inicial-backfill (ex.: 2026-07-19T00:00:00)")
            else:
                self.backfill_dim_usuario('ZEROUM', data_inicial_backfill)
        elif cliente == 'ENERGIABET_BACKFILL_USUARIO':
            if not data_inicial_backfill:
                self.logger.error("ENERGIABET_BACKFILL_USUARIO requer --data-inicial-backfill (ex.: 2026-07-19T00:00:00)")
            else:
                self.backfill_dim_usuario('ENERGIABET', data_inicial_backfill)
        elif cliente == 'ZEROUM_BACKFILL_USUARIO_POR_IDS':
            # Uso pontual: regulariza ids específicos de dim_usuario (ex.: os
            # que ficaram com carga incompleta por falha anterior), sem
            # reprocessar uma janela de datas inteira.
            if not ids_backfill:
                self.logger.error(
                    "ZEROUM_BACKFILL_USUARIO_POR_IDS requer --ids-backfill "
                    "(lista de ids separados por vírgula, ex.: 123,456,789)"
                )
            else:
                self.backfill_dim_usuario_por_ids('ZEROUM', ids_backfill)
        elif cliente == 'ENERGIABET_BACKFILL_USUARIO_POR_IDS':
            if not ids_backfill:
                self.logger.error(
                    "ENERGIABET_BACKFILL_USUARIO_POR_IDS requer --ids-backfill "
                    "(lista de ids separados por vírgula, ex.: 123,456,789)"
                )
            else:
                self.backfill_dim_usuario_por_ids('ENERGIABET', ids_backfill)
        else:
            self.logger.error("Cliente inválido")

    

    def get_log_history(self, linhas: int = 50) -> str:
        """Lê as últimas N linhas do etl.log para compor o body dos e-mails de erro."""
        try:
            with open('etl.log', 'r', encoding='latin-1') as f:
                todas = f.readlines()
                return "".join(todas[-linhas:])
        except Exception as e:
            return f"(não foi possível ler o etl.log: {e})"

    def principal_zeroum(self):
        try:
            start_time = datetime.now()
            #data_final = "2026-02-25T00:00:00"
            #while data_final < "2026-03-07T00:00:00":
            #print("atualizando as datas")
            #data_inicial = data_final
            #data_final = (datetime.strptime(data_final, '%Y-%m-%dT%H:%M:%S') + timedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%S')
            #data_final = data_final + timedelta(days=5)
            #print("data_inicial")
            #print(data_inicial)
            #print("data_final")
            #print(data_final)
            self.logger.info("Iniciando ETL")
            self.logger.info("Fazendo a autenticação no Metabase")
            auth_id = self.conection('ZEROUM')
            #auth_id = "2a30d8ef-dcfd-4753-bb87-b06dbefdd1d8"
            ConnectionDB.conecta(DB, 'ZEROUM')
            self.logger.info("Recuperando a data base para consulta")
            data_importacao = ConnectionDB.recupera_dados('inplay.fact_user_daily', 'max(updated_at) as updated_at', '')
            print('data_importacao')
            print(data_importacao)
            data_base = data_importacao[0][0]
            #data_inicial = data_base - timedelta(days=5)
            print('data base')
            print(data_base)
            #if(data_base >= date.today()):
            #    data_final = data_base
            #else:
            #    data_final = data_base + timedelta(days=1)
            data_inicial = data_base - timedelta(hours=4)
            data_inicial = data_inicial.strftime('%Y-%m-%dT%H:%M:%S')
            #data_final = data_final.strftime('%Y-%m-%d')

            print('datas')
            print(data_inicial)

            self.logger.info("Deletando dados das stages")
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily_sport', "", self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.deleta_dados('inplay.stg_game', "", self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.deleta_dados('inplay.stg_fact_deposits_withdraws_summarized', "", self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.deleta_dados('inplay.stg_fact_casino_games_hourly', "", self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.deleta_dados('inplay.stg_usuario', "", self.logger)

            #data_inicial = "2026-03-04T00:00:00"
            #data_final = "2026-03-07T00:00:00"
            self.logger.info("Iniciando as consultas ao metabase e inserção dos dados")
            df_stg = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_Stage.value, data_inicial, 0)
            df_deposito = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_Deposito.value, data_inicial, 0)
            df_saque = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_Saque.value, data_inicial, 0)
            # Nativa com filtro de LastUpdateTime embutido no WHERE (mesmo padrão do
            # saldo diário) — o card do Metabase filtra por fora, obrigando o
            # ClickHouse a agregar a tabela Bet inteira antes de descartar quem não
            # teve atividade recente, o que estourava MEMORY_LIMIT_EXCEEDED
            # (confirmado manualmente) e provavelmente causava o
            # "Found multiple matches to update the same tuple" no merge por
            # resultado truncado/inconsistente.
            df_primeira_aposta = self.extrai_aposta_nativo(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, 180, data_inicial, "primeira")
            df_ultima_aposta = self.extrai_aposta_nativo(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, 180, data_inicial, "ultima")
            df_bonus = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_Bonus.value, data_inicial, 0)
            df_bonus_ativado = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_BonusAtivado.value, data_inicial, 0)
            
            #print("df_primeira_aposta")
            #print(df_primeira_aposta)
            """df_stg = df_stg.merge(df_deposito[['usuario', 'data_referencia', 'deposit_amount', 'deposit_quantity', 'deposit_pending_amount', 'deposit_pending_quantity', 'ftd_date']], 
                                        on=['usuario', 'data_referencia'],
                                        how='outer')
            df_stg = df_stg.merge(df_saque[['usuario', 'data_referencia', 'withdraw_amount', 'withdraw_quantity', 'withdraw_pending_amount', 'withdraw_pending_quantity', 'withdraw_denied_amount', 'withdraw_denied_quantity']], 
                                        on=['usuario', 'data_referencia'],
                                        how='outer')
            df_stg = df_stg.merge(df_bonus[['usuario', 'data_referencia', 'bonus_amount', 'bonus_quantity']], 
                                        on=['usuario', 'data_referencia'],
                                        how='outer')
            df_stg = df_stg.merge(df_primeira_aposta[['usuario', 'casino_bet_first_date', 'casino_bet_first_amount']], 
                                        on=['usuario'],
                                        how='left')
            df_stg = df_stg.merge(df_ultima_aposta[['usuario', 'casino_bet_last_date', 'casino_bet_last_amount']], 
                                        on=['usuario'],
                                        how='left')
            df_stg = df_stg.merge(df_bonus_ativado[['usuario', 'data_referencia', 'bonus_activated']], 
                                        on=['usuario', 'data_referencia'],
                                        how='left')"""
            
            #nome_arquivo = f"stg_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_stg.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"deposito_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_deposito.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"saque_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_saque.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"primeira_aposta_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_primeira_aposta.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"ultima_aposta_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_ultima_aposta.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"bonus_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_bonus.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #print("finaliza salvar os arquivos")

            df_stg = df_stg.replace({np.nan: None})
            df_deposito = df_deposito.replace({np.nan: None})
            df_saque = df_saque.replace({np.nan: None})
            df_bonus = df_bonus.replace({np.nan: None})
            df_primeira_aposta = df_primeira_aposta.replace({np.nan: None})
            df_ultima_aposta = df_ultima_aposta.replace({np.nan: None})
            df_bonus_ativado = df_bonus_ativado.replace({np.nan: None})

            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_stg, self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_stg, ['usuario', 'data_referencia'], self.logger)
            
            df_deposito = df_deposito[['usuario', 'data_referencia', 'deposit_amount', 'deposit_quantity', 'deposit_pending_amount', 'deposit_pending_quantity', 'ftd_date']]
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_deposito, self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_deposito, ['usuario', 'data_referencia'], self.logger)

            df_saque = df_saque[['usuario', 'data_referencia', 'withdraw_amount', 'withdraw_quantity', 'withdraw_pending_amount', 'withdraw_pending_quantity', 'withdraw_denied_amount', 'withdraw_denied_quantity']]
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_saque, self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_saque, ['usuario', 'data_referencia'], self.logger)

            df_bonus = df_bonus[['usuario', 'data_referencia', 'bonus_amount', 'bonus_quantity']]
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_bonus, self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_bonus, ['usuario', 'data_referencia'], self.logger)

            df_primeira_aposta = df_primeira_aposta[['usuario', 'casino_bet_first_date', 'casino_bet_first_amount']]
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_primeira_aposta, self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_primeira_aposta, ['usuario'], self.logger)

            df_ultima_aposta = df_ultima_aposta[['usuario', 'casino_bet_last_date', 'casino_bet_last_amount']]
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_ultima_aposta, self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_ultima_aposta, ['usuario'], self.logger)

            df_bonus_ativado = df_bonus_ativado[['usuario', 'data_referencia', 'bonus_activated']]
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_bonus_ativado, self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_bonus_ativado, ['usuario', 'data_referencia'], self.logger)

            df_stg_sport = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_StageSport.value, data_inicial, 0)

            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily_sport', df_stg_sport, self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily_sport', 'inplay.fact_user_daily_sport', df_stg_sport, ['usuario', 'data_referencia'], self.logger)

            df_fact_dep_saq_dias = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_DepositoSaqueDias.value, data_inicial, 0)
            df_fact_dep_saq_dias_ajust = df_fact_dep_saq_dias[['date', 'hora', 'tipo', 'qtd', 'amount', 'updated_at', 'import_date']]

            #nome_arquivo = f"df_fact_dep_saq_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_fact_dep_saq_dias_ajust.to_csv(nome_arquivo, index=False, encoding="utf-8")

            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_deposits_withdraws_summarized', df_fact_dep_saq_dias_ajust, self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.mergeia_dados('inplay.stg_fact_deposits_withdraws_summarized', 'inplay.fact_deposits_withdraws_summarized', df_fact_dep_saq_dias_ajust, ['date', 'hora', 'tipo'], self.logger)

            df_dim_game = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_Jogos.value, 0, 0)

            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.insere_dados_bulk('inplay.stg_game', df_dim_game, self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.mergeia_dados('inplay.stg_game', 'inplay.dim_game', df_dim_game, ['game_id', 'estado'], self.logger)

            # Nativa com filtro de LastUpdateTime embutido — o card original não tinha
            # NENHUM filtro de tempo, reagregando a Bet inteira (histórico completo)
            # a cada chamada. Aqui, diferente da aposta/totalizador, filtrar antes ou
            # depois do GROUP BY dá o mesmo resultado (ver docstring de
            # _sql_apostas_jogos_hora) — sem risco de mudar a semântica.
            df_fact_cassino_game_hourly = self.extrai_apostas_jogos_hora_nativo(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, 180, data_inicial)
            df_fact_cassino_game_hourly_ajust = df_fact_cassino_game_hourly[['reference', 'game_id', 'with_bonus', 'bet_amount', 'win_amount', 'bet_qty', 'win_qty', 'ggr_amount', 'updated_at', 'data_importacao']]

            #nome_arquivo = f"df_game_hourly_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_fact_cassino_game_hourly_ajust.to_csv(nome_arquivo, index=False, encoding="utf-8")

            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_casino_games_hourly', df_fact_cassino_game_hourly_ajust, self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.mergeia_dados('inplay.stg_fact_casino_games_hourly', 'inplay.fact_casino_games_hourly', df_fact_cassino_game_hourly_ajust, ['reference', 'game_id'], self.logger)

            df_usuario = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_Usuarios.value, data_inicial, 0)
            df_usuario_ajust = df_usuario[['id','core_account_status','core_user_language','core_wallet_currency','birth_date','first_name','email_verified','email',
                                        'mobile_number_verified','mobile_number','refer_id','sms_allowed','email_allowed','city','state','updated_at','registration_date','import_date',
                                        'first_deposit_date','first_deposit_amount','first_withdraw_date','first_withdraw_amount','last_deposit_date','last_deposit_amount',
                                        'last_withdraw_date','last_withdraw_amount', 'utm', 'status_usuario']]
            
            #print("🚨 INICIO DESCRIPTOGRAFIA")
            #print("ANTES:")
            #print(df_usuario_ajust[['birth_date', 'first_name', 'mobile_number']].head(5))

            util = Util()

            colunas_criptografadas = ['birth_date', 'first_name', 'mobile_number']

            for col in colunas_criptografadas:
                df_usuario_ajust[col] = df_usuario_ajust[col].apply(
                    lambda x: util.descriptografar("ZEROUM", x)
                )
            #print("DEPOIS:")
            #print(df_usuario_ajust[['birth_date', 'first_name', 'mobile_number']].head(5))

            df_usuario_ajust['birth_date'] = pd.to_datetime(
            df_usuario_ajust['birth_date'],
                format='%d-%m-%Y',
                errors='coerce'
            )

            # opcional (recomendado pra banco)
            #df_usuario_ajust['birth_date'] = df_usuario_ajust['birth_date'].dt.strftime('%Y-%m-%d')


            #print("🚨 FIM DESCRIPTOGRAFIA")

            df_usuario_totalizador = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_UsuariosTotalizador.value, data_inicial, 0)
            df_usuario_totalizador_ajust = df_usuario_totalizador[['id','total_quantity_deposit','total_amount_deposit','total_quantity_withdraw','total_amount_withdraw']]
            # Nativa com filtro por Client.LastUpdateTime embutido (mesmo padrão da
            # aposta) — o card original não tinha NENHUM filtro de tempo, agregando
            # o histórico de TODOS os clientes (9M+ linhas em Bet) antes do filtro
            # aplicado por fora. Confirmado manualmente: ~5min com filtro embutido
            # vs. >90min sem terminar sem ele.
            df_usuario_totalizador_bet = self.extrai_usuario_totalizador_bet_nativo(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, data_inicial)
            df_usuario_totalizador_bet_ajust = df_usuario_totalizador_bet[['id','total_quantity_bet','total_amount_bet','total_quantity_win','total_amount_win', 'total_quantity_bonus',
                                                                        'total_amount_bonus', 'total_ggr']]

            #nome_arquivo = f"df_usuario_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_usuario.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"df_usuario_totalizador_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_usuario_totalizador.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"df_usuario_totalizador_bet_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_usuario_totalizador_bet.to_csv(nome_arquivo, index=False, encoding="utf-8")

            df_dim_usuario = df_usuario_ajust.merge(df_usuario_totalizador_ajust, 
                                        on=['id'],
                                        how='outer')
            df_dim_usuario = df_dim_usuario.merge(df_usuario_totalizador_bet_ajust, 
                                        on=['id'],
                                        how='outer')

            #nome_arquivo = f"df_usuario_merged_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_dim_usuario.to_csv(nome_arquivo, index=False, encoding="utf-8")
          
            df_dim_usuario = df_dim_usuario.replace({np.nan: None})

            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.insere_dados_bulk('inplay.stg_usuario', df_dim_usuario, self.logger)
            ConnectionDB.conecta(DB, 'ZEROUM')
            ConnectionDB.mergeia_dados('inplay.stg_usuario', 'inplay.dim_usuario', df_dim_usuario, ['id'], self.logger)

            executar_agregacao_dim_usuario(
            df_list=[
                    df_stg,
                    df_deposito,
                    df_saque,
                    df_primeira_aposta,
                    df_ultima_aposta,
                    df_bonus,
                    df_bonus_ativado,
                    df_stg_sport
                ],
                cliente='ZEROUM',
                db=DB,
                logger=self.logger
            )

            executar_agregacao_cohort_retencao('ZEROUM', DB, self.logger)
            executar_kpi_diario_datatalk('ZEROUM', DB, self.logger)

            self.db_logger.log_operation(
                operation='ETL_ZEROUM',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente='ZEROUM'
            )

        except Exception as e:
            self.logger.error(f"Erro na execução do ETL ZEROUM: {e}")
            self.db_logger.log_operation(
                operation='ETL_ZEROUM',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente='ZEROUM'
            )
            b = (
                f"Descrição do erro: {e}\n\n"
                f"=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            )
            send_email(subject="[FALHA ENGENHARIA] ZeroUm - Erro na execução do ETL", body=b)
            raise
        
    def principal_energiabet(self):
        try:
            start_time = datetime.now()
            #data_final = "2026-03-25T00:00:00"
            #while data_final < "2026-03-31T00:00:00":
            #print("atualizando as datas")
            #data_inicial = data_final
            #data_final = (datetime.strptime(data_final, '%Y-%m-%dT%H:%M:%S') + timedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%S')
            #print("data_inicial")
            #print(data_inicial)
            #print("data_final")
            #print(data_final)
            self.logger.info("Iniciando ETL")
            self.logger.info("Fazendo a autenticação no Metabase")
            auth_id = self.conection('ENERGIABET')
            #auth_id = "2a30d8ef-dcfd-4753-bb87-b06dbefdd1d8"
            ConnectionDB.conecta(DB, 'ENERGIABET')
            self.logger.info("Recuperando a data base para consulta")
            data_importacao = ConnectionDB.recupera_dados('inplay.fact_user_daily', 'max(updated_at) as updated_at', '')
            print('data_importacao')
            print(data_importacao)
            data_base = data_importacao[0][0]
            #data_inicial = data_base - timedelta(days=5)
            print('data base')
            print(data_base)
            #if(data_base >= date.today()):
            #    data_final = data_base
            #else:
            #    data_final = data_base + timedelta(days=1)
            data_inicial = data_base - timedelta(hours=4)
            data_inicial = data_inicial.strftime('%Y-%m-%dT%H:%M:%S')
            #data_final = data_final.strftime('%Y-%m-%d')

            print('datas')
            print(data_inicial)

            self.logger.info("Deletando dados das stages")
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily_sport', "", self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.deleta_dados('inplay.stg_game', "", self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.deleta_dados('inplay.stg_fact_deposits_withdraws_summarized', "", self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.deleta_dados('inplay.stg_fact_casino_games_hourly', "", self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.deleta_dados('inplay.stg_usuario', "", self.logger)

            #data_inicial = "2026-02-01T00:00:00"
            #data_final = "2026-03-01T00:00:00"
            self.logger.info("Iniciando as consultas ao metabase e inserção dos dados")
            df_stg = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_Stage.value, data_inicial, 0)
            df_deposito = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_Deposito.value, data_inicial, 0)
            df_saque = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_Saque.value, data_inicial, 0)
            # ⚠️ SINALIZADO, NÃO ALTERADO: mesmo padrão de card que causou
            # MEMORY_LIMIT_EXCEEDED no ZEROUM (ver extrai_aposta_nativo /
            # _sql_aposta) provavelmente afeta a EnergiaBet também, mas o
            # PartnerId correto na tabela Bet para EnergiaBet ainda não foi
            # confirmado — por isso esta chamada continua no card antigo até
            # confirmar e replicar a mesma correção aqui.
            df_primeira_aposta = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_PrimeiraAposta.value, data_inicial, 0)
            df_ultima_aposta = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_UltimaAposta.value, data_inicial, 0)
            df_bonus = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_Bonus.value, data_inicial, 0)
            df_bonus_ativado = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_BonusAtivado.value, data_inicial, 0)
            
            #print("df_primeira_aposta")
            #print(df_primeira_aposta)
            """df_stg = df_stg.merge(df_deposito[['usuario', 'data_referencia', 'deposit_amount', 'deposit_quantity', 'deposit_pending_amount', 'deposit_pending_quantity', 'ftd_date']], 
                                        on=['usuario', 'data_referencia'],
                                        how='outer')
            df_stg = df_stg.merge(df_saque[['usuario', 'data_referencia', 'withdraw_amount', 'withdraw_quantity', 'withdraw_pending_amount', 'withdraw_pending_quantity', 'withdraw_denied_amount', 'withdraw_denied_quantity']], 
                                        on=['usuario', 'data_referencia'],
                                        how='outer')
            df_stg = df_stg.merge(df_bonus[['usuario', 'data_referencia', 'bonus_amount', 'bonus_quantity']], 
                                        on=['usuario', 'data_referencia'],
                                        how='outer')
            df_stg = df_stg.merge(df_primeira_aposta[['usuario', 'casino_bet_first_date', 'casino_bet_first_amount']], 
                                        on=['usuario'],
                                        how='left')
            df_stg = df_stg.merge(df_ultima_aposta[['usuario', 'casino_bet_last_date', 'casino_bet_last_amount']], 
                                        on=['usuario'],
                                        how='left')
            df_stg = df_stg.merge(df_bonus_ativado[['usuario', 'data_referencia', 'bonus_activated']], 
                                        on=['usuario', 'data_referencia'],
                                        how='left')"""
            
            #nome_arquivo = f"stg_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_stg.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"deposito_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_deposito.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"saque_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_saque.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"primeira_aposta_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_primeira_aposta.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"ultima_aposta_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_ultima_aposta.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"bonus_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_bonus.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #print("finaliza salvar os arquivos")

            df_stg = df_stg.replace({np.nan: None})
            df_deposito = df_deposito.replace({np.nan: None})
            df_saque = df_saque.replace({np.nan: None})
            df_bonus = df_bonus.replace({np.nan: None})
            df_primeira_aposta = df_primeira_aposta.replace({np.nan: None})
            df_ultima_aposta = df_ultima_aposta.replace({np.nan: None})
            df_bonus_ativado = df_bonus_ativado.replace({np.nan: None})

            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_stg, self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_stg, ['usuario', 'data_referencia'], self.logger)
            
            df_deposito = df_deposito[['usuario', 'data_referencia', 'deposit_amount', 'deposit_quantity', 'deposit_pending_amount', 'deposit_pending_quantity', 'ftd_date']]
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_deposito, self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_deposito, ['usuario', 'data_referencia'], self.logger)

            df_saque = df_saque[['usuario', 'data_referencia', 'withdraw_amount', 'withdraw_quantity', 'withdraw_pending_amount', 'withdraw_pending_quantity', 'withdraw_denied_amount', 'withdraw_denied_quantity']]
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_saque, self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_saque, ['usuario', 'data_referencia'], self.logger)

            df_bonus = df_bonus[['usuario', 'data_referencia', 'bonus_amount', 'bonus_quantity']]
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_bonus, self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_bonus, ['usuario', 'data_referencia'], self.logger)

            df_primeira_aposta = df_primeira_aposta[['usuario', 'casino_bet_first_date', 'casino_bet_first_amount']]
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_primeira_aposta, self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_primeira_aposta, ['usuario'], self.logger)

            df_ultima_aposta = df_ultima_aposta[['usuario', 'casino_bet_last_date', 'casino_bet_last_amount']]
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_ultima_aposta, self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_ultima_aposta, ['usuario'], self.logger)

            df_bonus_ativado = df_bonus_ativado[['usuario', 'data_referencia', 'bonus_activated']]
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_bonus_ativado, self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily', 'inplay.fact_user_daily', df_bonus_ativado, ['usuario', 'data_referencia'], self.logger)
            
            df_stg_sport = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_StageSport.value, data_inicial, 0)

            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily_sport', df_stg_sport, self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily_sport', 'inplay.fact_user_daily_sport', df_stg_sport, ['usuario', 'data_referencia'], self.logger)

            df_fact_dep_saq_dias = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_DepositoSaqueDias.value, data_inicial, 0)
            df_fact_dep_saq_dias_ajust = df_fact_dep_saq_dias[['date', 'hora', 'tipo', 'qtd', 'amount', 'updated_at', 'import_date']]

            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_deposits_withdraws_summarized', df_fact_dep_saq_dias_ajust, self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.mergeia_dados('inplay.stg_fact_deposits_withdraws_summarized', 'inplay.fact_deposits_withdraws_summarized', df_fact_dep_saq_dias_ajust, ['date', 'hora', 'tipo'], self.logger)

            df_dim_game = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_Jogos.value, 0, 0)

            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.insere_dados_bulk('inplay.stg_game', df_dim_game, self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.mergeia_dados('inplay.stg_game', 'inplay.dim_game', df_dim_game, ['game_id', 'estado'], self.logger)

            # ⚠️ SINALIZADO, NÃO ALTERADO: mesmo padrão de card sem filtro de tempo
            # (ver extrai_apostas_jogos_hora_nativo / _sql_apostas_jogos_hora)
            # provavelmente afeta a EnergiaBet também, mas essa query usa PartnerId
            # (igual à aposta) e o valor correto para EnergiaBet ainda não foi
            # confirmado — por isso esta chamada continua no card antigo até
            # confirmar e replicar a mesma correção aqui.
            df_fact_cassino_game_hourly = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_ApostasJogosHora.value, data_inicial, 0)
            df_fact_cassino_game_hourly_ajust = df_fact_cassino_game_hourly[['reference', 'game_id', 'with_bonus', 'bet_amount', 'win_amount', 'bet_qty', 'win_qty', 'ggr_amount', 'updated_at', 'data_importacao']]

            #nome_arquivo = f"df_game_hourly_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_fact_cassino_game_hourly_ajust.to_csv(nome_arquivo, index=False, encoding="utf-8")

            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_casino_games_hourly', df_fact_cassino_game_hourly_ajust, self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.mergeia_dados('inplay.stg_fact_casino_games_hourly', 'inplay.fact_casino_games_hourly', df_fact_cassino_game_hourly_ajust, ['reference', 'game_id'], self.logger)

            df_usuario = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_Usuarios.value, data_inicial, 0)
            df_usuario_ajust = df_usuario[['id','core_account_status','core_user_language','core_wallet_currency','birth_date','first_name','email_verified','email',
                                        'mobile_number_verified','mobile_number','refer_id','sms_allowed','email_allowed','city','state','updated_at','registration_date','import_date',
                                        'first_deposit_date','first_deposit_amount','first_withdraw_date','first_withdraw_amount','last_deposit_date','last_deposit_amount',
                                        'last_withdraw_date','last_withdraw_amount', 'utm',  'status_usuario']]
            
            
            #print("🚨 INICIO DESCRIPTOGRAFIA")
            #print("ANTES:")
            #print(df_usuario_ajust[['birth_date', 'first_name', 'mobile_number']].head(5))

            util = Util()

            colunas_criptografadas = ['birth_date', 'first_name', 'mobile_number']

            for col in colunas_criptografadas:
                df_usuario_ajust[col] = df_usuario_ajust[col].apply(
                    lambda x: util.descriptografar("ENERGIABET", x)
                )
            #print("DEPOIS:")
            #print(df_usuario_ajust[['birth_date', 'first_name', 'mobile_number']].head(5))

            df_usuario_ajust['birth_date'] = pd.to_datetime(
            df_usuario_ajust['birth_date'],
                format='%d-%m-%Y',
                errors='coerce'
            )

            # opcional (recomendado pra banco)
            #df_usuario_ajust['birth_date'] = df_usuario_ajust['birth_date'].dt.strftime('%Y-%m-%d')


            #print("🚨 FIM DESCRIPTOGRAFIA")
            
            
            
            
            
            
            df_usuario_totalizador = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_UsuariosTotalizador.value, data_inicial, 0)
            df_usuario_totalizador_ajust = df_usuario_totalizador[['id','total_quantity_deposit','total_amount_deposit','total_quantity_withdraw','total_amount_withdraw']]
            # Nativa com filtro por Client.LastUpdateTime embutido — ver comentário
            # equivalente em principal_zeroum. Sem PartnerId envolvido nessa query
            # (ZEROUM/EnergiaBet já são bancos ClickHouse separados via id_database),
            # então é seguro aplicar a mesma correção aqui sem confirmação extra.
            df_usuario_totalizador_bet = self.extrai_usuario_totalizador_bet_nativo(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, data_inicial)
            df_usuario_totalizador_bet_ajust = df_usuario_totalizador_bet[['id','total_quantity_bet','total_amount_bet','total_quantity_win','total_amount_win', 'total_quantity_bonus',
                                                                        'total_amount_bonus', 'total_ggr']]

            #nome_arquivo = f"df_usuario_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_usuario.to_csv(nome_arquivo, index=False, encoding="utf-8")
            #nome_arquivo = f"df_usuario_totalizador_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_usuario_totalizador.to_csv(nome_arquivo, index=False, encoding="utf-8")

            df_dim_usuario = df_usuario_ajust.merge(df_usuario_totalizador_ajust, 
                                        on=['id'],
                                        how='outer')
            df_dim_usuario = df_dim_usuario.merge(df_usuario_totalizador_bet_ajust, 
                                        on=['id'],
                                        how='outer')
            
            df_dim_usuario = df_dim_usuario.replace({np.nan: None})

            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.insere_dados_bulk('inplay.stg_usuario', df_dim_usuario, self.logger)
            ConnectionDB.conecta(DB, 'ENERGIABET')
            ConnectionDB.mergeia_dados('inplay.stg_usuario', 'inplay.dim_usuario', df_dim_usuario, ['id'], self.logger)

            self.db_logger.log_operation(
                operation='ETL_ENERGIABET',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente='ENERGIABET'
            )

        except Exception as e:
            self.logger.error(f"Erro na execução do ETL ENERGIABET: {e}")
            self.db_logger.log_operation(
                operation='ETL_ENERGIABET',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente='ENERGIABET'
            )
            b = (
                f"Descrição do erro: {e}\n\n"
                f"=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            )
            send_email(subject="[FALHA ENGENHARIA] EnergiaBet - Erro na execução do ETL", body=b)
            raise
        
    
    def principal_zro_1_bet(self, modo="incremental"):
        start_time = datetime.now()
        try:
            self.logger.info(f"Iniciando carga ZRO_1_BET - {modo}")

            df = self.extrair_zro_1_bet(
                modo="historico" if modo == "historico" else "incremental"
            )

            df = self.validar_vendas_data(df)

            self.carregar_stg_vendas_data(df)

            self.carregar_fct_vendas_data()

            self.logger.info("Carga ZRO_1_BET finalizada")

            self.db_logger.log_operation(
                operation='ETL_ZRO_1_BET',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente='ZRO_1_BET'
            )

        except Exception as e:
            self.logger.error(f"Erro na execução do ETL ZRO_1_BET: {e}")
            self.db_logger.log_operation(
                operation='ETL_ZRO_1_BET',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente='ZRO_1_BET'
            )
            b = (
                f"Descrição do erro: {e}\n\n"
                f"=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            )
            send_email(subject="[FALHA ENGENHARIA] ZRO_1_BET - Erro na execução do ETL", body=b)
            raise

    def conection(self, cliente):
        id = ''
        try:

            # =========================================================
            # 🔹 NOVO CLIENTE - POSTGRESQL
            # =========================================================
            if cliente == 'ZRO_1_BET':
                try:
                    self.logger.info("Iniciando conexão PostgreSQL - ZRO_1_BET")

                     # 🔥 SEGURANÇA: garante que o engine existe
                    if not hasattr(self, "engine_zro1bet"):
                        raise Exception("engine_zro1bet não foi inicializado no __init__")


                    #conn = self.engine_zro1bet.connect()
                    conn = psycopg2.connect(
                        host=os.getenv("DB_HOST_ZRO_1_BET_ADTK"),
                        port=os.getenv("DB_PORT_ZRO_1_BET_ADTK"),
                        dbname=os.getenv("DB_NAME_ZRO_1_BET_ADTK"),
                        user=os.getenv("DB_USER_ZRO_1_BET_ADTK"),
                        password=os.getenv("DB_PASS_ZRO_1_BET_ADTK")
                    )
                    #conn.execute(text("SELECT 1"))
                    cur = conn.cursor()
                    cur.execute("SELECT 1")
                    cur.close()
                    self.logger.info("Conexão PostgreSQL ZRO_1_BET estabelecida com sucesso")

                    return conn

                except Exception as e:
                    self.logger.error(f"Erro na conexão PostgreSQL ZRO_1_BET: {e}")
                    # e-mail não enviado aqui — principal_zro_1_bet captura e envia
                    # um único e-mail completo com histórico de log
                    raise Exception(f"Erro ao conectar no PostgreSQL ZRO_1_BET: {e}")
                                
                     


            # =========================================================
            # 🔹 CLIENTES API
            # =========================================================
            elif cliente == 'ZEROUM' or cliente == 'ENERGIABET':

                self.auth_url = API_AUTH

                self.body = {
                    "username": API_USER_ZEROUM if cliente == 'ZEROUM' else API_USER_ENERGIABET,
                    "password": API_PASS_ZEROUM if cliente == 'ZEROUM' else API_PASS_ENERGIABET
                }

                response = requests.post(self.auth_url, json=self.body, timeout=60)

                print("requisição realizada")

                id = response.json()['id']

            # =========================================================
            # 🔹 CLIENTE INVÁLIDO
            # =========================================================
            else:
                raise Exception(f"Cliente inválido: {cliente}")

        except requests.exceptions.RequestException as e:
            # captura APENAS erros HTTP do requests.post() de ZEROUM/ENERGIABET
            # erros de psycopg2 do ZRO_1_BET propagam direto para principal_zro_1_bet
            self.logger.error(f"Erro na autenticação da API: {e}")
            # Não envia e-mail aqui de propósito (mesmo motivo de extrai_csv/
            # extrai_dados_card — evita duplicar notificação com o chamador
            # de topo, que já envia o dele quando a exceção sobe).
            raise

        return id

    

    def create_engine_zro1bet(self):

        host = os.getenv("DB_HOST_ZRO_1_BET_ADTK")
        port = os.getenv("DB_PORT_ZRO_1_BET_ADTK")
        db   = os.getenv("DB_NAME_ZRO_1_BET_ADTK")
        user = os.getenv("DB_USER_ZRO_1_BET_ADTK")
        password = os.getenv("DB_PASS_ZRO_1_BET_ADTK")

        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

        return create_engine(url)


    def create_engine_dw(self):

        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        db = os.getenv("DB_NAME_ZEROUM")
        user = os.getenv("DB_USER")

        print("DW HOST:", host)
        print("DW PORT:", port)
        print("DW DB:", db)
        print("DW USER:", user)

        password = os.getenv("DB_PASS")

        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

        return create_engine(url)

    def extrai_csv(self, auth_id:str, database:int, table:int=0, card:str='', filter:str=None, timeout:int=2400, max_tentativas:int=3):
        try:
            self.rota = API_ROTA_CSV
            self.header = {
                'Content-Type': 'application/json',
                'Cookie': f'metabase.DEVICE={auth_id}; metabase.SESSION={auth_id}; metabase.TIMEOUT=alive'
            }
            self.body = {
                "query": {
                    "database": database,
                    "type": "query",
                    "query": {
                        "source-table": table if table > 0 else card
                    }
                }
            }
            
            if filter:
                self.body["query"]["query"]["filter"] = filter

            print('body')
            print(self.body)

            # timeout parametrizável (default 40 min): cobre cards pesados (ex.: apostas
            # de cassino por hora) sem interromper execuções normais; só estoura se
            # realmente travar. Chamadas com janela maior que o normal (ex.: backfill
            # de dim_usuario) podem passar um valor maior.
            #
            # retry com backoff (mesmo padrão de extrai_csv_nativo): o Metabase
            # ocasionalmente derruba a conexão no meio da requisição
            # (RemoteDisconnected/ConnectionError) sem nenhum problema real com a
            # consulta — sem retry aqui, isso derrubava o ETL inteiro (ver falha
            # do card ZeroUm_Stage). Só reage a falhas de conexão/timeout; um
            # status HTTP != 200 continua sendo apenas logado, não re-tentado,
            # pois normalmente indica erro na própria query, que não se resolve
            # tentando de novo.
            ultimo_erro = None
            response = None
            for tentativa in range(1, max_tentativas + 1):
                try:
                    response = requests.post(self.rota, headers=self.header, json=self.body, timeout=timeout)
                    break
                except requests.exceptions.RequestException as e:
                    ultimo_erro = e
                    self.logger.warning(
                        f"[extrai_csv] card={card} tentativa {tentativa}/{max_tentativas} "
                        f"falhou (timeout={timeout}s): {e}"
                    )
                    if tentativa < max_tentativas:
                        time.sleep(5 * tentativa)

            if response is None:
                self.logger.error(
                    f"[extrai_csv] card={card} falhou após {max_tentativas} tentativas: {ultimo_erro}"
                )
                raise ultimo_erro

            print("requisição realizada")
            #print(response)
            #print(response.content)

            #inclusao de log
            self.logger.info(
                f"[extrai_csv] card={card} status={response.status_code} "
                f"bytes_recebidos={len(response.content)}"
            )
            if response.status_code != 200:
                self.logger.error(
                    f"[extrai_csv] Metabase retornou status {response.status_code} "
                    f"para card {card}. Body (500 chars): {response.text[:500]}"
                )
            elif len(response.content.strip()) == 0:
                self.logger.warning(f"[extrai_csv] card={card} retornou corpo vazio")

            resultado_csv = response.content
            #df = pd.DataFrame(resultado_csv)
            return resultado_csv
        except Exception as e:
            self.logger.error(f"Erro ao extrair CSV da API: {e}")
            # Não envia e-mail aqui de propósito (achado desta conversa: gerava
            # até 3 e-mails pra mesma falha — este nível, extrai_dados_card e o
            # chamador de topo). Quem decide notificar é sempre o chamador de
            # nível mais alto (principal_zeroum/principal_energiabet,
            # backfill_dim_usuario, valida_aposta, etc.). A exceção sobe normal.
            raise
                                               
    def extrai_dados_card(self, auth_id, id_database, id_card, data_inicial, data_final,
                           campo_filtro="updated_at", tipo_campo="type/DateTime", timeout=2400):
        try:
            #self.rota = f"https://inplaysoft.metabaseapp.com/api/card/{id}/query/csv"
            #self.header = {
            #    'Content-Type': 'application/json',
            #    'Cookie': f'metabase.DEVICE={auth_id}; metabase.SESSION=9722077e-4614-410c-bc2f-1040513e3ea3; metabase.TIMEOUT=alive'
            #}

            #response = requests.post(self.rota, headers=self.header)

            #print("requisição realizada")
            #print(response)
            #print(response.content)

            #resultado_csv = response.content
            #print('resultado_csv')
            #print(resultado_csv)
            #df = pd.DataFrame(resultado_csv)
            self.logger.info(f"Extraindo dados da consulta {id_card}")
            #id_database = MetabaseDatabase.ClickhousePartnerZeroum.value
            if(data_inicial == 0 and data_final == 0):
                csv = self.extrai_csv(auth_id, id_database, 0, id_card, timeout=timeout)
            elif (data_inicial != 0 and data_final == 0):
                csv = self.extrai_csv(auth_id, id_database, 0, id_card, [">",["field",campo_filtro,{"base-type":tipo_campo}], data_inicial], timeout=timeout)
            else:
                csv = self.extrai_csv(auth_id, id_database, 0, id_card, ["between",["field",campo_filtro,{"base-type":tipo_campo}], data_inicial, data_final], timeout=timeout)
            try:
                df = pd.read_csv(io.BytesIO(csv))
                #para verificar erro 
                print("=" * 80)
                print(f"CARD {id_card}")
                print("Shape:", df.shape)
                print("Colunas:")
                print(df.columns.tolist())
                print(df.head(3))
                print("=" * 80)
            except pd.errors.EmptyDataError as e:
                    self.logger.error(
                        f"[extrai_dados_card] Card {id_card} retornou CSV vazio "
                        f"(bytes recebidos: {len(csv)}, filtro data_inicial={data_inicial}, "
                        f"data_final={data_final})"
                    )
                    raise Exception(
                        f"Card {id_card} sem dados no período informado (data_inicial={data_inicial}) "
                        f"— possível ausência de registros novos ou falha na consulta ao Metabase"
                    ) from e
            return df
        except Exception as e:
            self.logger.error(f"Erro ao extrair os dados da consulta {id_card}: {e}")
            # Não envia e-mail aqui de propósito (mesmo motivo de extrai_csv,
            # logo abaixo na pilha de chamadas — evita duplicar notificação).
            raise

    @staticmethod
    def _sql_aposta(data_inicial: str, tipo: str, partner_id: int) -> str:
        """
        Monta a query nativa de primeira/última aposta de cassino.

        HISTÓRICO (ver conversa): a primeira versão deste método filtrava
        LastUpdateTime > data_inicial dentro do MESMO WHERE que alimenta o
        GROUP BY/agregação — o que resolvia o MEMORY_LIMIT_EXCEEDED, mas
        mudava a semântica: em vez de "primeira/última aposta de todo o
        histórico, para quem teve atividade recente", virava "primeira/
        última aposta SÓ ENTRE as linhas recentes". Confirmado via
        valida_aposta: 97% de divergência em "primeira" (o MIN real quase
        sempre está fora da janela recente) e 3,5% em "última" (o MAX real
        normalmente cai dentro da janela, mas não sempre — ex.: uma aposta
        antiga que teve LastUpdateTime tocado por um ajuste/estorno tardio,
        sem gerar aposta nova).

        CORRIGIDO: filtra QUEM entra na agregação (subquery barata sobre
        ClientId, sem GROUP BY pesado) mas agrega o HISTÓRICO COMPLETO
        desses usuários — preserva a semântica do card original evitando
        escanear/agregar quem não teve atividade recente (que é o que
        estourava a memória).

        tipo: "primeira" (MIN/argMin) ou "ultima" (MAX/argMax).
        """
        if tipo == "primeira":
            agregador_data = "MIN"
            agregador_valor = "argMin"
            alias_data = "casino_bet_first_date"
            alias_valor = "casino_bet_first_amount"
        elif tipo == "ultima":
            agregador_data = "MAX"
            agregador_valor = "argMax"
            alias_data = "casino_bet_last_date"
            alias_valor = "casino_bet_last_amount"
        else:
            raise ValueError(f"tipo inválido para _sql_aposta: {tipo!r} (use 'primeira' ou 'ultima')")

        filtro_base = f"State NOT IN (1, 4) AND ProductId != 6 AND PartnerId = {partner_id}"

        return f"""
SELECT
    ClientId AS usuario,
    {agregador_data}(toTimeZone(BetTime, 'America/Sao_Paulo'))::DATE AS {alias_data},
    {agregador_valor}(BetAmount, BetTime) AS {alias_valor},
    MAX(LastUpdateTime) AS updated_at,
    now() AS data_importacao
FROM Bet
WHERE {filtro_base}
  AND ClientId IN (
      SELECT DISTINCT ClientId
      FROM Bet
      WHERE {filtro_base}
        AND LastUpdateTime > '{data_inicial}'
  )
GROUP BY ClientId
"""


    def extrai_aposta_nativo(self, auth_id, id_database, partner_id, data_inicial, tipo):
        """
        Substitui extrai_dados_card para os cards de primeira/última aposta:
        roda a query nativa de _sql_aposta (filtro por ClientId, agregação
        sobre o histórico completo — ver docstring de _sql_aposta) via
        extrai_csv_nativo, em vez de depender do card salvo no Metabase
        (que filtra por fora e estourava memória).

        timeout maior que o default de extrai_csv_nativo (180s): diferente
        do saldo diário, essa query agrega o HISTÓRICO COMPLETO do
        subconjunto de usuários filtrados, não só a janela — mais lenta por
        natureza (confirmado: 180s não foi suficiente, nem com 3 retries).

        Não envia e-mail aqui de propósito, mesmo padrão de extrai_csv_nativo
        (achado desta conversa: gerava e-mail duplicado, um daqui e outro do
        chamador — principal_zeroum/principal_energiabet ou valida_aposta já
        enviam o deles). A exceção sobe normalmente.
        """
        self.logger.info(f"Extraindo {tipo} aposta (query nativa, partner_id={partner_id})")
        sql = self._sql_aposta(data_inicial, tipo, partner_id)
        csv = self.extrai_csv_nativo(auth_id, id_database, sql, timeout=900)
        df = pd.read_csv(io.BytesIO(csv))
        return df

    @staticmethod
    def _sql_usuario_totalizador_bet(data_inicial: str) -> str:
        """
        Monta a query nativa de totalizador de aposta por usuário (substitui
        o card ZeroUm_UsuariosTotalizadorBet / EnergiaBet_UsuariosTotalizadorBet).

        A query original do card (confirmada manualmente) não tinha NENHUM
        filtro de tempo: o JOIN entre cliente_ajustado e Bet (9M+ linhas)
        agregava o histórico de TODOS os clientes que já existiram, e o
        updated_at > data_inicial era aplicado por fora pelo Metabase —
        mesmo padrão estrutural da aposta, mas com o filtro de tempo vindo
        de Client.LastUpdateTime, não de algo do Bet.

        Correção: filtra QUAIS ClientId entram no JOIN com Bet (subquery
        sobre cliente_ajustado, que vem de Client — bem menor que Bet),
        preservando a CTE de dedup original (DISTINCT ON por LastSessionId)
        intacta.

        Confirmado manualmente: ~5min com o filtro embutido (vs. >90min sem
        terminar com o filtro por fora, mesmo volume de 9M+ linhas em Bet).
        """
        return f"""
WITH cliente_ajustado AS (
    SELECT DISTINCT ON (c.Id) c.*
    FROM Client c
    ORDER BY c.Id, c.LastSessionId DESC
)
SELECT
    c.Id AS id,
    sum(CASE WHEN b.BetAmount > 0 THEN 1 ELSE 0 END) AS total_quantity_bet,
    round(sum(b.BetAmount), 2) AS total_amount_bet,
    sum(CASE WHEN b.WinAmount > 0 THEN 1 ELSE 0 END) AS total_quantity_win,
    round(sum(b.WinAmount), 2) AS total_amount_win,
    sum(CASE WHEN b.BetBonusAmount > 0 THEN 1 ELSE 0 END) AS total_quantity_bonus,
    round(sum(b.BetBonusAmount), 2) AS total_amount_bonus,
    round(sum(b.Ggr), 2) AS total_ggr,
    max(c.LastUpdateTime) AS updated_at,
    min(c.CreationTime)::DATE AS data_referencia
FROM Bet b
INNER JOIN cliente_ajustado c ON b.ClientId = c.Id
WHERE b.State NOT IN (1, 4) AND b.ProductId != 6
  AND c.Id IN (
      SELECT Id FROM cliente_ajustado WHERE LastUpdateTime > '{data_inicial}'
  )
GROUP BY c.Id
"""

    @staticmethod
    def _sql_usuario_totalizador_bet_por_ids(ids: list) -> str:
        """
        Variante de _sql_usuario_totalizador_bet para regularização pontual
        de registros específicos: filtra cliente_ajustado por uma lista de
        Id (tipicamente pequena), em vez de uma janela de LastUpdateTime.
        Evita o full scan de Bet do mesmo jeito — o filtro entra ANTES do
        JOIN, restringindo cliente_ajustado (vindo de Client, pequeno) antes
        de tocar em Bet (grande).
        """
        ids_sql = ", ".join(str(int(i)) for i in ids)
        return f"""
WITH cliente_ajustado AS (
    SELECT DISTINCT ON (c.Id) c.*
    FROM Client c
    WHERE c.Id IN ({ids_sql})
    ORDER BY c.Id, c.LastSessionId DESC
)
SELECT
    c.Id AS id,
    sum(CASE WHEN b.BetAmount > 0 THEN 1 ELSE 0 END) AS total_quantity_bet,
    round(sum(b.BetAmount), 2) AS total_amount_bet,
    sum(CASE WHEN b.WinAmount > 0 THEN 1 ELSE 0 END) AS total_quantity_win,
    round(sum(b.WinAmount), 2) AS total_amount_win,
    sum(CASE WHEN b.BetBonusAmount > 0 THEN 1 ELSE 0 END) AS total_quantity_bonus,
    round(sum(b.BetBonusAmount), 2) AS total_amount_bonus,
    round(sum(b.Ggr), 2) AS total_ggr,
    max(c.LastUpdateTime) AS updated_at,
    min(c.CreationTime)::DATE AS data_referencia
FROM Bet b
INNER JOIN cliente_ajustado c ON b.ClientId = c.Id
WHERE b.State NOT IN (1, 4) AND b.ProductId != 6
GROUP BY c.Id
"""

    def extrai_usuario_totalizador_bet_nativo(self, auth_id, id_database, data_inicial):
        """
        Substitui extrai_dados_card para ZeroUm_UsuariosTotalizadorBet /
        EnergiaBet_UsuariosTotalizadorBet: roda _sql_usuario_totalizador_bet
        (filtro embutido) via extrai_csv_nativo, em vez do card que
        escaneava Bet inteira (9M+ linhas) antes de filtrar.

        timeout=900 (15min): confirmado ~5min manualmente, margem de ~3x.

        Não envia e-mail aqui de propósito (mesmo padrão de
        extrai_aposta_nativo/extrai_csv_nativo).
        """
        self.logger.info("Extraindo totalizador bet de usuário (query nativa, filtro por Client.LastUpdateTime)")
        sql = self._sql_usuario_totalizador_bet(data_inicial)
        csv = self.extrai_csv_nativo(auth_id, id_database, sql, timeout=900)
        df = pd.read_csv(io.BytesIO(csv))
        return df

    def extrai_usuario_totalizador_bet_nativo_por_ids(self, auth_id, id_database, ids: list):
        """
        Variante para regularização pontual: mesmo princípio de
        extrai_usuario_totalizador_bet_nativo, mas filtrando por uma lista
        de ids em vez de uma janela de tempo.
        """
        self.logger.info(f"Extraindo totalizador bet de usuário para {len(ids)} ids específicos (query nativa)")
        sql = self._sql_usuario_totalizador_bet_por_ids(ids)
        csv = self.extrai_csv_nativo(auth_id, id_database, sql, timeout=900)
        df = pd.read_csv(io.BytesIO(csv))
        return df

    @staticmethod
    def _sql_apostas_jogos_hora(data_inicial: str, partner_id: int) -> str:
        """
        Monta a query nativa de apostas por jogo/hora (substitui o card
        ZeroUm_ApostasJogosHora / EnergiaBet_ApostasJogosHora).

        Diferente da aposta (primeira/última) e do totalizador bet: aqui
        NÃO há risco de mudar a semântica ao filtrar antes do GROUP BY.
        O agrupamento (reference, ProductId, data) já deriva do próprio
        LastUpdateTime que seria usado pra filtrar — cada linha pertence a
        exatamente um grupo definido por esse mesmo campo. Filtrar antes ou
        depois do GROUP BY dá o mesmo resultado; a diferença é só quantas
        linhas de Bet o ClickHouse precisa escanear pra chegar lá (query
        original não tinha NENHUM filtro de tempo — reagregava toda a
        tabela Bet a cada chamada).
        """
        return f"""
SELECT
    toTimeZone(b.LastUpdateTime, 'America/Sao_Paulo')::DATE AS data_referencia,
    formatDateTime(toStartOfHour(toTimeZone(toDateTime(LastUpdateTime, 'UTC'), 'America/Sao_Paulo')),'%Y-%m-%d %H:%i:%S') AS reference,
    b.ProductId AS game_id,
    sum(CASE WHEN b.BonusId IS NOT NULL THEN 1 ELSE 0 END) AS with_bonus,
    round(sum(b.BetAmount),2) AS bet_amount,
    round(sum(b.WinAmount),2) AS win_amount,
    sum(CASE WHEN b.BetAmount > 0 THEN 1 ELSE 0 END) AS bet_qty,
    sum(CASE WHEN b.WinAmount > 0 THEN 1 ELSE 0 END) AS win_qty,
    round(sum(ifNull(b.BetAmount, 0) - ifNull(b.WinAmount, 0)), 2) AS ggr_amount,
    max(toTimeZone(b.LastUpdateTime, 'America/Sao_Paulo')) AS updated_at,
    now() AS data_importacao
FROM Bet b
INNER JOIN Product p ON p.Id = b.ProductId
WHERE b.State NOT IN (1, 4) AND b.ProductId != 6 AND b.PartnerId = {partner_id}
  AND b.LastUpdateTime > '{data_inicial}'
GROUP BY b.ProductId, reference, toTimeZone(b.LastUpdateTime, 'America/Sao_Paulo')::DATE
ORDER BY toTimeZone(b.LastUpdateTime, 'America/Sao_Paulo')::DATE DESC
"""

    def extrai_apostas_jogos_hora_nativo(self, auth_id, id_database, partner_id, data_inicial):
        """
        Substitui extrai_dados_card para ZeroUm_ApostasJogosHora /
        EnergiaBet_ApostasJogosHora: roda _sql_apostas_jogos_hora (filtro
        embutido) via extrai_csv_nativo, em vez do card sem filtro de
        tempo que reagregava a Bet inteira a cada chamada.

        Não envia e-mail aqui de propósito (mesmo padrão dos demais nativos
        desta conversa).
        """
        self.logger.info(f"Extraindo apostas por jogo/hora (query nativa, partner_id={partner_id})")
        sql = self._sql_apostas_jogos_hora(data_inicial, partner_id)
        csv = self.extrai_csv_nativo(auth_id, id_database, sql, timeout=900)
        df = pd.read_csv(io.BytesIO(csv))
        return df

    def extrai_dados_card_por_ids(self, auth_id, id_database, id_card, ids: list,
                                   campo_filtro="id", tipo_campo="type/Integer", timeout=2400):
        """
        Extrai um card filtrando por uma lista específica de valores (ex.:
        ids), em vez de uma janela de tempo — usado para regularizar
        registros pontuais sem reprocessar uma janela inteira de datas.
        Usa o filtro MBQL "=" com múltiplos valores (equivalente a "IN"):
        ["=", field, v1, v2, ...].
        """
        self.logger.info(f"Extraindo card {id_card} filtrando por {len(ids)} valores de {campo_filtro}")
        filtro = ["=", ["field", campo_filtro, {"base-type": tipo_campo}]] + list(ids)
        csv = self.extrai_csv(auth_id, id_database, 0, id_card, filtro, timeout=timeout)
        try:
            df = pd.read_csv(io.BytesIO(csv))
        except pd.errors.EmptyDataError as e:
            raise Exception(
                f"Card {id_card} sem dados para os {len(ids)} valores de {campo_filtro} informados"
            ) from e
        return df

    def extrair_zro_1_bet(self, modo="incremental"):
        print("1 - antes conexão")
        conn = self.conection("ZRO_1_BET")
        print("2 - depois conexão")
        # histórico vs incremental
        print("EXECUÇÃO ID:", id(self))
        print("MODO:", modo)
        # Lista explícita de colunas: evita que uma alteração na estrutura da
        # tabela de origem (ex.: novas colunas) quebre o ETL em produção.
        # Se uma coluna nova for adicionada na origem e for necessária no DW,
        # ela precisa ser incluída aqui manualmente.
        colunas_origem = """
                id_transacao,
                data_criacao,
                user_id,
                nome,
                sobrenome,
                email,
                telefone,
                data_nascimento,
                endereco,
                cidade,
                estado,
                pais,
                zipcode,
                utm_campaign,
                utm_campaign_checkout,
                utm_content,
                utm_medium,
                utm_source,
                utm_term,
                utm_id,
                ad_id,
                valor,
                pagina_origem,
                page_referrer,
                status,
                tag,
                tipo
        """

        if modo == "historico":
            query = f"""
                SELECT {colunas_origem}
                FROM zro1_bet_adtk.vendas_data
            """
        else:
            query = f"""
                SELECT {colunas_origem}
                FROM zro1_bet_adtk.vendas_data
                WHERE data_criacao >= CURRENT_DATE - INTERVAL '7 days'
            """

        try:
            df = pd.read_sql(query, conn)
        finally:
            conn.close()
        print("3 - depois extract")
        print("\n=== DATAFRAME ===")
        print(df.head())
        print("\n=== SHAPE ===")
        print(df.shape)

        return df


    def validar_vendas_data(self, df):

        self.logger.info("Iniciando validação dos dados")

        if "data_criacao" not in df.columns:
            raise Exception("Coluna data_criacao não encontrada no DataFrame.")

        # garante datetime
        df["data_criacao"] = pd.to_datetime(
            df["data_criacao"],
            errors="coerce"
        )

        df_rejeitados = df[df["data_criacao"].isna()].copy()

        if not df_rejeitados.empty:

            self.logger.warning(
                f"{len(df_rejeitados)} registros rejeitados."
            )

            df_rejeitados["updated_at"] = pd.Timestamp.now()
            df_rejeitados["import_date"] = pd.Timestamp.now()

            df_rejeitados = df_rejeitados.rename(
                columns={"tag": "tag_venda"}
            )

            df_rejeitados["motivo_rejeicao"] = "DATA_CRIACAO_NULA"
            df_rejeitados["data_rejeicao"] = pd.Timestamp.now()

            self.salvar_rejeitados(df_rejeitados)

        # Mantém apenas válidos
        df = df[df["data_criacao"].notna()].copy()

        self.logger.info(
            f"Validação concluída. Registros válidos: {len(df)}"
        )

        return df
           

    def salvar_rejeitados(self, df_rejeitados):

        self.logger.info("Salvando registros rejeitados")

        if df_rejeitados.empty:
            self.logger.info("Nenhum registro rejeitado encontrado.")
            return

        conn = ConnectionDB.conecta(DB, "ZEROUM")

        try:

            if conn.closed:
                raise Exception("Conexão fechada.")

            cur = conn.cursor()

            # ==========================================================
            # 1. LIMPA STG TEMPORÁRIA
            # ==========================================================

            self.logger.info("Limpando STG de rejeitados")

            cur.execute("""
                TRUNCATE TABLE inplay.stg_vendas_data_rejeitados_tmp
            """)

            conn.commit()
            cur.close()

            # ==========================================================
            # 2. CARREGA STG TEMPORÁRIA
            # ==========================================================

            # força todas as colunas datetime virarem object
            for coluna in df_rejeitados.select_dtypes(include=["datetime64[ns]"]).columns:

                df_rejeitados[coluna] = df_rejeitados[coluna].astype(object)

            df_rejeitados = df_rejeitados.where(
                pd.notnull(df_rejeitados),
                None
            )


            print("\n================ DEBUG REJEITADOS ================")

            for coluna in df_rejeitados.columns:

                if "data" in coluna.lower() or "updated" in coluna.lower():

                    print(f"\nColuna: {coluna}")
                    print("dtype:", df_rejeitados[coluna].dtype)
                    print("Valor:", repr(df_rejeitados.iloc[0][coluna]))
                    print("Tipo :", type(df_rejeitados.iloc[0][coluna]))

            print("==================================================\n")


            ConnectionDB.insere_dados_bulk(
                "inplay.stg_vendas_data_rejeitados_tmp",
                df_rejeitados,
                self.logger
            )

            self.logger.info(
                f"{len(df_rejeitados)} registros inseridos na STG de rejeitados."
            )

            # insere_dados_bulk fecha a conexão automaticamente.
            # Reabre para continuar o processamento.

            conn = ConnectionDB.conecta(DB, "ZEROUM")

            if conn.closed:
                raise Exception("Falha ao reabrir conexão.")

            cur = conn.cursor()

            # ==========================================================
            # 3. UPDATE DOS REJEITADOS JÁ EXISTENTES
            # ==========================================================

            self.logger.info("Atualizando rejeitados existentes")

            cur.execute("""

                UPDATE inplay.log_vendas_data_rejeitados l
                SET
                    data_criacao      = s.data_criacao,
                    user_id           = s.user_id,
                    nome              = s.nome,
                    sobrenome         = s.sobrenome,
                    email             = s.email,
                    telefone          = s.telefone,
                    data_nascimento   = s.data_nascimento,
                    endereco          = s.endereco,
                    cidade            = s.cidade,
                    estado            = s.estado,
                    pais              = s.pais,
                    zipcode           = s.zipcode,
                    utm_campaign      = s.utm_campaign,
                    utm_campaign_checkout = s.utm_campaign_checkout,
                    utm_content       = s.utm_content,
                    utm_medium        = s.utm_medium,
                    utm_source        = s.utm_source,
                    utm_term          = s.utm_term,
                    ad_id             = s.ad_id,
                    valor             = s.valor,
                    pagina_origem     = s.pagina_origem,
                    page_referrer     = s.page_referrer,
                    status            = s.status,
                    tag_venda         = s.tag_venda,
                    tipo              = s.tipo,
                    utm_id            = s.utm_id,
                    updated_at        = s.updated_at,
                    import_date       = s.import_date,
                    motivo_rejeicao   = s.motivo_rejeicao,
                    ultima_ocorrencia = s.data_rejeicao,
                    qtde_rejeicoes    = l.qtde_rejeicoes + 1

                FROM inplay.stg_vendas_data_rejeitados_tmp s

                WHERE l.id_transacao = s.id_transacao;

            """)

            self.logger.info("UPDATE de rejeitados concluído.")

            # ==========================================================
            # 4. INSERT DOS NOVOS REJEITADOS
            # ==========================================================

            self.logger.info("Inserindo novos rejeitados")

            cur.execute("""

                INSERT INTO inplay.log_vendas_data_rejeitados
                (
                    id_transacao,
                    data_criacao,
                    user_id,
                    nome,
                    sobrenome,
                    email,
                    telefone,
                    data_nascimento,
                    endereco,
                    cidade,
                    estado,
                    pais,
                    zipcode,
                    utm_campaign,
                    utm_campaign_checkout,
                    utm_content,
                    utm_medium,
                    utm_source,
                    utm_term,
                    ad_id,
                    valor,
                    pagina_origem,
                    page_referrer,
                    status,
                    tag_venda,
                    tipo,
                    utm_id,
                    updated_at,
                    import_date,
                    motivo_rejeicao,
                    data_rejeicao,
                    ultima_ocorrencia,
                    qtde_rejeicoes
                )

                SELECT
                    s.id_transacao,
                    s.data_criacao,
                    s.user_id,
                    s.nome,
                    s.sobrenome,
                    s.email,
                    s.telefone,
                    s.data_nascimento,
                    s.endereco,
                    s.cidade,
                    s.estado,
                    s.pais,
                    s.zipcode,
                    s.utm_campaign,
                    s.utm_campaign_checkout,
                    s.utm_content,
                    s.utm_medium,
                    s.utm_source,
                    s.utm_term,
                    s.ad_id,
                    s.valor,
                    s.pagina_origem,
                    s.page_referrer,
                    s.status,
                    s.tag_venda,
                    s.tipo,
                    s.utm_id,
                    s.updated_at,
                    s.import_date,
                    s.motivo_rejeicao,
                    s.data_rejeicao,
                    s.data_rejeicao,
                    1

                FROM inplay.stg_vendas_data_rejeitados_tmp s

                WHERE NOT EXISTS
                (
                    SELECT 1
                    FROM inplay.log_vendas_data_rejeitados l
                    WHERE l.id_transacao = s.id_transacao
                );

            """)

            conn.commit()

            cur.close()

            self.logger.info("Log de rejeitados atualizado com sucesso.")

        except Exception as e:

            try:
                conn.rollback()
            except:
                pass

            self.logger.error(f"Erro ao salvar rejeitados: {e}")
            raise

        finally:

            if conn:
                conn.close()       


    def carregar_stg_vendas_data(self, df):

        print("A - entrou STG")

        self.logger.info("Carregando STAGE")

        conn = ConnectionDB.conecta(DB, 'ZEROUM')

        print("B - conectou DW")

        if conn is None:
            raise Exception("Falha ao conectar no DW (ZEROUM)")

        try:

            self.logger.info("Limpando STAGE antes da carga")

            print("C - truncate")

            #conn.cursor().execute("TRUNCATE TABLE inplay.stg_vendas_data")
            #conn.commit()
            #if hasattr(ConnectionDB, "cur"):
            #    ConnectionDB.cur.execute(
           #         "TRUNCATE TABLE inplay.stg_vendas_data"
            #    )

            cur = conn.cursor()
            cur.execute("TRUNCATE TABLE inplay.stg_vendas_data")
            conn.commit()
            cur.close() 

            print("C.1 - truncate realizado")

            # -------------------
            # PREPARA DATAFRAME
            # -------------------

            df["updated_at"] = pd.Timestamp.now()
            df["import_date"] = pd.Timestamp.now()

            df = df.rename(columns={"tag": "tag_venda"})

            print("D - antes to_sql")
            print("Linhas para inserir:", len(df))

            print(df.columns.tolist())
            print(type(conn))

             # motivo: Redshift antigo NÃO aguenta 669k em um único execute_values

            #chunk_size = 5000  # equilíbrio entre performance e estabilidade

            #print(f"Inserindo em batches de {chunk_size} linhas...")

            #for i in range(0, len(df), chunk_size):

            #    chunk = df.iloc[i:i + chunk_size]

            #    ConnectionDB.insere_dados_bulk(
            #        "inplay.stg_vendas_data",
            #        chunk,
            #        self.logger
            #    )

           #     print(f"Batch {i} → {i + len(chunk)} inserido")

            #print("E - insert finalizado")

            #self.logger.info("STAGE carregada com sucesso")
            if conn.closed:
                raise Exception("Conexão foi fechada antes do insert")
            
            ConnectionDB.insere_dados_bulk(
                "inplay.stg_vendas_data",
                df,
                self.logger
            )

            print("E - depois to_sql")

            self.logger.info("STAGE carregada com sucesso")

        except Exception as e:

            self.logger.error(
                f"Erro ao carregar STAGE: {e}"
            )

            raise

        finally:

            if conn:
                conn.close()

            print("F - conexão fechada")

            self.logger.info(
                "Conexão da STAGE fechada"
            )

    def carregar_fct_vendas_data(self):

        self.logger.info("Carregando FACT (UPDATE + INSERT)")

        conn = ConnectionDB.conecta(DB, 'ZEROUM')

        try:

            if conn.closed:
                raise Exception("Conexão fechada antes da execução da FACT")

            cur = conn.cursor()

            try:
                # ==========================================
                # UPDATE DOS REGISTROS EXISTENTES
                # ==========================================

                query_update = """
                    UPDATE inplay.fact_vendas_data f
                    SET
                        user_id = s.user_id,
                        nome = s.nome,
                        sobrenome = s.sobrenome,
                        email = s.email,
                        telefone = s.telefone,
                        data_nascimento = s.data_nascimento,
                        endereco = s.endereco,
                        cidade = s.cidade,
                        estado = s.estado,
                        pais = s.pais,
                        zipcode = s.zipcode,
                        utm_campaign = s.utm_campaign,
                        utm_campaign_checkout = s.utm_campaign_checkout,
                        utm_content = s.utm_content,
                        utm_medium = s.utm_medium,
                        utm_source = s.utm_source,
                        utm_term = s.utm_term,
                        ad_id = s.ad_id,
                        valor = s.valor,
                        pagina_origem = s.pagina_origem,
                        page_referrer = s.page_referrer,
                        status = s.status,
                        tag_venda = s.tag_venda,
                        tipo = s.tipo,
                        utm_id = s.utm_id,
                        updated_at = s.updated_at,
                        import_date = s.import_date
                    FROM inplay.stg_vendas_data s
                    WHERE
                        f.id_transacao = s.id_transacao
                    AND f.data_criacao = s.data_criacao;
                """

                cur.execute(query_update)

                self.logger.info("UPDATE realizado")

                # ==========================================
                # INSERE APENAS NOVOS REGISTROS
                # ==========================================

                query_insert = """
                    INSERT INTO inplay.fact_vendas_data
                    SELECT s.*
                    FROM inplay.stg_vendas_data s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM inplay.fact_vendas_data f
                        WHERE f.id_transacao = s.id_transacao
                        AND f.data_criacao = s.data_criacao
                    );
                """

                cur.execute(query_insert)

                conn.commit()

            finally:
                cur.close()

            self.logger.info("FACT carregada com sucesso")

        except Exception as e:

            try:
                conn.rollback()
            except:
                pass

            self.logger.error(f"Erro ao carregar FACT: {e}")
            raise

        finally:
            if conn:
                conn.close()


    def sobe_dados(self):
        df = pd.read_csv('jogos_jogador.csv', sep=';', header=0)

        print(df)

        #df['Id'] = df['Id'].astype(int)
        df = df.astype(object)

        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.insere_dados_bulk('inplay.jogos_jogador', df, self.logger)
           
    def valida_dados(self, cliente: str):
        try:
            start_time = datetime.now()
            print('função para validar os dados da base')
            self.logger.info("Iniciando o processo de validação dos dados")
            df_validacao = pd.DataFrame()
            database = MetabaseDatabase.ClickhousePartnerZeroum.value if cliente == 'ZEROUM' else MetabaseDatabase.ClickhousePartnerEnergiabet.value
            auth_id = self.conection(cliente)
            card = MetabaseCard.ZeroUm_Validacao_ApostasDia.value if cliente == 'ZEROUM' else MetabaseCard.EnergiaBet_Validacao_ApostasDia.value
            df_apostas_dia_origem = self.extrai_dados_card(auth_id, database, card, 0, 0)
            card = MetabaseCard.ZeroUm_Validacao_DepositoSaque.value if cliente == 'ZEROUM' else MetabaseCard.EnergiaBet_Validacao_DepositoSaque.value
            df_deposito_saque_origem = self.extrai_dados_card(auth_id, database, card, 0, 0)
            card = MetabaseCard.ZeroUm_Validacao_RegistroUsuario.value if cliente == 'ZEROUM' else MetabaseCard.EnergiaBet_Validacao_RegistroUsuario.value
            df_usuario_origem = self.extrai_dados_card(auth_id, database, card, 0, 0)

            nome_arquivo = f"usuario_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            df_usuario_origem.to_csv(nome_arquivo, index=False, encoding="utf-8")

            df_validacao = df_apostas_dia_origem.merge(df_deposito_saque_origem[['data_referencia', 'deposit_amount_origem', 'deposit_qtd_origem', 'withdraw_amount_origem', 'withdraw_qtd_origem']], 
                                        on=['data_referencia'],
                                        how='left')
            df_validacao = df_validacao.merge(df_usuario_origem[['data_referencia', 'users_registered_origem']], 
                                        on=['data_referencia'],
                                        how='left')
            
            #nome_arquivo = f"validacao_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_validacao.to_csv(nome_arquivo, index=False, encoding="utf-8")

            script = """
                        with
                        deposits_withdraw_summarized as (
                            select 
                                f.date,
                                sum(case when f.tipo = 'deposit' then f.amount else 0 end)  as deposit_amount_destino_summarized,
                                sum(case when f.tipo = 'deposit' then f.qtd else 0 end)  as deposit_qtd_destino_summarized,
                                sum(case when f.tipo = 'withdraw' then f.amount else 0 end)  as withdraw_amount_destino_summarized,
                                sum(case when f.tipo = 'withdraw' then f.qtd else 0 end)  as withdraw_qtd_destino_summarized
                            from inplay.fact_deposits_withdraws_summarized f
                            group by f.date
                        ),
                        user_daily as (
                            select 
                                f.data_referencia ,
                                sum(f.deposit_amount) as deposit_amount_destino_user_daily,
                                sum(f.deposit_quantity) as deposit_qtd_destino_user_daily,
                                sum(f.withdraw_amount) as withdraw_amount_destino_user_daily,
                                sum(f.withdraw_quantity) as withdraw_qtd_destino_user_daily,
                                sum(f.cassino_bet_amount) as bet_amount_destino_user_daily,
                                sum(f.cassino_bet_quantity) as bet_qtd_destino_user_daily
                            from inplay.fact_user_daily f
                            group by f.data_referencia
                        ),
                        casino_games as (
                            select 
                                f.reference::date,
                                sum(f.bet_amount) as bet_amount_destino_casino_games,
                                sum(f.bet_qty) as bet_qtd_destino_casino_games
                            from inplay.fact_casino_games_hourly f
                            group by reference::date 
                        ),
                        users_registration as (
                            select 
                                d.registration_date::date,
                                count(1) as users_registered_destino_dim_usuario
                            from inplay.dim_usuario d
                            group by d.registration_date::date
                        )
                        select
                            coalesce(s.date, ud.data_referencia, cg.reference) as data_referencia,
                            -- deposit
                            s.deposit_amount_destino_summarized,
                            ud.deposit_amount_destino_user_daily,
                            s.deposit_qtd_destino_summarized,
                            ud.deposit_qtd_destino_user_daily,
                            -- withdraw
                            s.withdraw_amount_destino_summarized,
                            ud.withdraw_amount_destino_user_daily,
                            s.withdraw_qtd_destino_summarized,
                            ud.withdraw_qtd_destino_user_daily,
                            -- bets
                            cg.bet_amount_destino_casino_games,
                            ud.bet_amount_destino_user_daily,
                            cg.bet_qtd_destino_casino_games,
                            ud.bet_qtd_destino_user_daily,
                            -- users_registration
                            ur.users_registered_destino_dim_usuario,
                        -- import_date
                            timezone('America/Sao_Paulo', current_timestamp) as import_date
                        from deposits_withdraw_summarized s
                            full outer join user_daily ud on s.date = ud.data_referencia
                            full outer join casino_games cg on coalesce(s.date, ud.data_referencia) = cg.reference
                            full outer join users_registration ur on coalesce(s.date, ud.data_referencia, cg.reference) = ur.registration_date
                        order by 1 desc;
                """

            tabela_validacao = 'inplay.verificacao_zero_um' if cliente == 'ZEROUM' else 'inplay.verificacao_energia_bet'
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.deleta_dados(tabela_validacao, "", self.logger)
            ConnectionDB.conecta(DB, cliente)
            df_dados_destino = ConnectionDB.executa_script(script, self.logger)

            df_validacao['data_referencia'] = pd.to_datetime(df_validacao['data_referencia'])
            df_dados_destino['data_referencia'] = pd.to_datetime(df_dados_destino['data_referencia'])

            df_validacao = df_validacao.merge(df_dados_destino[['data_referencia', 'deposit_amount_destino_summarized', 'deposit_amount_destino_user_daily', 'deposit_qtd_destino_summarized', 'deposit_qtd_destino_user_daily',
                                                                'withdraw_amount_destino_summarized', 'withdraw_amount_destino_user_daily', 'withdraw_qtd_destino_summarized', 'withdraw_qtd_destino_user_daily',
                                                                'bet_amount_destino_casino_games', 'bet_amount_destino_user_daily', 'bet_qtd_destino_casino_games', 'bet_qtd_destino_user_daily', 
                                                                'users_registered_destino_dim_usuario', 'import_date']], 
                                        on=['data_referencia'],
                                        how='outer')
            
            df_validacao = df_validacao.rename(columns={'data_referencia': 'data'})
            df_validacao = df_validacao.replace({np.nan: None})

            df_validacao['deposit_validacao'] = np.where(((df_validacao['deposit_amount_origem'] != df_validacao['deposit_amount_destino_summarized']) |
                                                        (df_validacao['deposit_amount_origem'] != df_validacao['deposit_amount_destino_user_daily']) |
                                                        (df_validacao['deposit_qtd_origem'] != df_validacao['deposit_qtd_destino_summarized']) |
                                                        (df_validacao['deposit_qtd_origem'] != df_validacao['deposit_qtd_destino_user_daily'])), 
                                                        'errado', 
                                                        'certo')
            
            df_validacao['withdraw_validacao'] = np.where(((df_validacao['withdraw_amount_origem'] != df_validacao['withdraw_amount_destino_summarized']) |
                                                        (df_validacao['withdraw_amount_origem'] != df_validacao['withdraw_amount_destino_user_daily']) |
                                                        (df_validacao['withdraw_qtd_origem'] != df_validacao['withdraw_qtd_destino_summarized']) |
                                                        (df_validacao['withdraw_qtd_origem'] != df_validacao['withdraw_qtd_destino_user_daily'])), 
                                                        'errado', 
                                                        'certo')
            
            df_validacao['bet_validacao'] = np.where(((df_validacao['bet_amount_origem'] != df_validacao['bet_amount_destino_casino_games']) |
                                                    (df_validacao['bet_amount_origem'] != df_validacao['bet_amount_destino_user_daily']) |
                                                    (df_validacao['bet_qtd_origem'] != df_validacao['bet_qtd_destino_casino_games']) |
                                                    (df_validacao['bet_qtd_origem'] != df_validacao['bet_qtd_destino_user_daily'])), 
                                                    'errado', 
                                                    'certo')
            
            df_validacao['users_registered_validacao'] = np.where((df_validacao['users_registered_origem'] != df_validacao['users_registered_destino_dim_usuario']), 
                                                        'errado', 
                                                        'certo')

            nome_arquivo = f"validacao2_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            df_validacao.to_csv(nome_arquivo, index=False, encoding="utf-8")
                
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.insere_dados_bulk(tabela_validacao, df_validacao, self.logger)

            self.db_logger.log_operation(
                operation=f'VALIDACAO_{cliente}',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente=cliente
            )

        except Exception as e:
            self.logger.error(f"Erro na validação de dados para {cliente}: {e}")
            self.db_logger.log_operation(
                operation=f'VALIDACAO_{cliente}',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente=cliente
            )
            b = (
                f"Erro na validação de dados para {cliente}: {e}\n\n"
                f"=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            )
            send_email(subject=f"[FALHA ENGENHARIA] {cliente} - Erro na Validação", body=b)
            raise

    def valida_aposta(self, cliente: str, partner_id: int):
        """
        Compara, para a MESMA janela de tempo usada em produção
        (data_inicial calculada a partir de max(updated_at) em
        inplay.fact_user_daily, igual a principal_zeroum/principal_energiabet),
        o resultado do card antigo do Metabase (extrai_dados_card, filtro
        aplicado POR FORA) contra a query nativa nova (extrai_aposta_nativo,
        filtro embutido no WHERE — ver _sql_aposta) para primeira e última
        aposta.

        Objetivo: confirmar que a troca de fonte não muda o resultado
        (mesmos usuários, mesmas datas, mesmos valores) antes de confiar
        nela em produção. Gera um CSV de divergências por tipo, se houver.
        """
        start_time = datetime.now()
        try:
            self.logger.info(f"Iniciando validação card vs nativo (primeira/última aposta) - {cliente}")

            database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                        if cliente == 'ZEROUM'
                        else MetabaseDatabase.ClickhousePartnerEnergiabet.value)

            auth_id = self.conection(cliente)

            ConnectionDB.conecta(DB, cliente)
            data_importacao = ConnectionDB.recupera_dados('inplay.fact_user_daily', 'max(updated_at) as updated_at', '')
            data_base = data_importacao[0][0]
            data_inicial = (data_base - timedelta(hours=4)).strftime('%Y-%m-%dT%H:%M:%S')
            self.logger.info(f"Janela de comparação: updated_at > {data_inicial}")

            cards = {
                "primeira": (MetabaseCard.ZeroUm_PrimeiraAposta.value if cliente == 'ZEROUM'
                             else MetabaseCard.EnergiaBet_PrimeiraAposta.value),
                "ultima": (MetabaseCard.ZeroUm_UltimaAposta.value if cliente == 'ZEROUM'
                           else MetabaseCard.EnergiaBet_UltimaAposta.value),
            }

            resumo = []

            for tipo, id_card in cards.items():
                self.logger.info(f"Comparando {tipo} aposta (card vs nativo)")

                col_data = "casino_bet_first_date" if tipo == "primeira" else "casino_bet_last_date"
                col_valor = "casino_bet_first_amount" if tipo == "primeira" else "casino_bet_last_amount"
                colunas_esperadas = ['usuario', col_data, col_valor]

                df_card = self.extrai_dados_card(auth_id, database, id_card, data_inicial, 0)
                self.logger.info(f"[{tipo}] Colunas retornadas pelo card {id_card}: {df_card.columns.tolist()}")

                faltando = [c for c in colunas_esperadas if c not in df_card.columns]
                if faltando:
                    self.logger.error(
                        f"[{tipo}] Card {id_card} não trouxe as colunas esperadas {faltando}. "
                        f"Colunas recebidas: {df_card.columns.tolist()} "
                        f"(linhas: {len(df_card)}) — pulando comparação deste tipo."
                    )
                    print(f"\n===== {tipo.upper()} APOSTA — {cliente} =====")
                    print(f"ERRO: card {id_card} não trouxe as colunas esperadas.")
                    print(f"Esperado: {colunas_esperadas}")
                    print(f"Recebido: {df_card.columns.tolist()}")
                    print(f"Linhas recebidas: {len(df_card)}")
                    resumo.append({
                        "tipo": tipo,
                        "erro": f"card sem colunas esperadas (faltando: {faltando})",
                        "colunas_recebidas": df_card.columns.tolist(),
                        "linhas_card": len(df_card),
                    })
                    continue

                # updated_at incluído só para diagnóstico (não entra no cálculo
                # de divergência) — permite checar se a divergência é timing
                # skew (updated_at do nativo mais recente que o do card, ou
                # seja, chegou aposta nova entre as duas chamadas) em vez de
                # um problema real de lógica.
                colunas_diag = colunas_esperadas + (['updated_at'] if 'updated_at' in df_card.columns else [])

                df_card = df_card[colunas_diag].copy()
                df_card['usuario'] = df_card['usuario'].astype(str)

                df_nativo = self.extrai_aposta_nativo(auth_id, database, partner_id, data_inicial, tipo)
                df_nativo = df_nativo[colunas_diag].copy()
                df_nativo['usuario'] = df_nativo['usuario'].astype(str)

                dup_card = df_card[df_card.duplicated('usuario', keep=False)]
                dup_nativo = df_nativo[df_nativo.duplicated('usuario', keep=False)]

                comp = df_card.merge(
                    df_nativo, on='usuario', how='outer',
                    suffixes=('_card', '_nativo'), indicator=True
                )

                so_card = comp[comp['_merge'] == 'left_only']
                so_nativo = comp[comp['_merge'] == 'right_only']
                ambos = comp[comp['_merge'] == 'both'].copy()

                ambos['data_diverge'] = (
                    ambos[f'{col_data}_card'].astype(str) != ambos[f'{col_data}_nativo'].astype(str)
                )
                ambos['valor_diverge'] = (
                    ambos[f'{col_valor}_card'].round(2) != ambos[f'{col_valor}_nativo'].round(2)
                )
                divergentes = ambos[ambos['data_diverge'] | ambos['valor_diverge']]

                divergentes_timing_skew = 0
                divergentes_suspeitos = len(divergentes)
                if 'updated_at' in df_card.columns and len(divergentes) > 0:
                    dt_card = pd.to_datetime(divergentes['updated_at_card'], errors='coerce')
                    dt_nativo = pd.to_datetime(divergentes['updated_at_nativo'], errors='coerce')
                    explicavel = dt_nativo > dt_card
                    divergentes_timing_skew = int(explicavel.sum())
                    divergentes_suspeitos = len(divergentes) - divergentes_timing_skew

                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

                if not divergentes.empty:
                    nome = f"divergencias_{tipo}_aposta_{cliente}_{timestamp}.csv"
                    divergentes.to_csv(nome, index=False, encoding="utf-8")
                    self.logger.warning(
                        f"{tipo} aposta: {len(divergentes)} divergentes "
                        f"({divergentes_timing_skew} explicáveis por timing skew, "
                        f"{divergentes_suspeitos} suspeitos) -> {nome}"
                    )

                if not so_card.empty:
                    nome = f"so_no_card_{tipo}_aposta_{cliente}_{timestamp}.csv"
                    so_card.to_csv(nome, index=False, encoding="utf-8")

                if not so_nativo.empty:
                    nome = f"so_no_nativo_{tipo}_aposta_{cliente}_{timestamp}.csv"
                    so_nativo.to_csv(nome, index=False, encoding="utf-8")

                linha_resumo = {
                    "tipo": tipo,
                    "linhas_card": len(df_card),
                    "linhas_nativo": len(df_nativo),
                    "duplicados_card": len(dup_card),
                    "duplicados_nativo": len(dup_nativo),
                    "so_no_card": len(so_card),
                    "so_no_nativo": len(so_nativo),
                    "divergentes": len(divergentes),
                    "divergentes_timing_skew": divergentes_timing_skew,
                    "divergentes_suspeitos": divergentes_suspeitos,
                }
                resumo.append(linha_resumo)

                print(f"\n===== {tipo.upper()} APOSTA — {cliente} (janela: updated_at > {data_inicial}) =====")
                for k, v in linha_resumo.items():
                    if k != "tipo":
                        print(f"{k}: {v}")

            self.logger.info(f"Validação card vs nativo concluída: {resumo}")

            self.db_logger.log_operation(
                operation=f'VALIDA_APOSTA_{cliente}',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente=cliente
            )

        except Exception as e:
            self.logger.error(f"Erro na validação card vs nativo (aposta) para {cliente}: {e}")
            self.db_logger.log_operation(
                operation=f'VALIDA_APOSTA_{cliente}',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente=cliente
            )
            b = (
                f"Erro na validação card vs nativo (aposta) para {cliente}: {e}\n\n"
                f"=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            )
            send_email(subject=f"[FALHA ENGENHARIA] {cliente} - Erro na Validação (aposta card vs nativo)", body=b)
            raise

    def backfill_dim_usuario(self, cliente: str, data_inicial_str: str):
        """
        Uso ÚNICO: recarrega inplay.dim_usuario cobrindo uma janela de tempo
        manual (data_inicial_str em diante), independente do updated_at de
        inplay.fact_user_daily.

        Motivo de existir: em principal_zeroum/principal_energiabet,
        data_inicial é calculado UMA VEZ a partir de max(updated_at) em
        fact_user_daily e reaproveitado em TODAS as extrações, inclusive a
        de usuários. Como fact_user_daily continuou avançando normalmente
        durante o período em que a extração de aposta estava quebrando
        (MEMORY_LIMIT_EXCEEDED — ver conversa) e abortando o restante da
        execução antes de chegar em dim_usuario, a cada nova tentativa
        data_inicial era empurrado para a data atual — não para a última
        atualização bem-sucedida de dim_usuario. Ou seja: qualquer usuário
        alterado no intervalo entre o último sucesso de dim_usuario e a
        correção pode nunca ser capturado pela incremental normal.

        Este método roda só a parte de usuários (extração + merge em
        dim_usuario), com data_inicial_str informado manualmente — deve ser
        a data do último updated_at bem-sucedido de dim_usuario (ex.:
        '2026-07-19T00:00:00'), não a de fact_user_daily. Depois de rodar
        uma vez, fact_user_daily e dim_usuario voltam a ficar sincronizados
        e a incremental normal (principal_zeroum/principal_energiabet) volta
        a ser suficiente.

        Não roda agregações (executar_agregacao_dim_usuario, cohort,
        kpi_diario) — escopo é só dim_usuario.
        """
        start_time = datetime.now()
        try:
            self.logger.info(f"Iniciando backfill de dim_usuario ({cliente}) a partir de {data_inicial_str}")

            database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                        if cliente == 'ZEROUM'
                        else MetabaseDatabase.ClickhousePartnerEnergiabet.value)

            card_usuarios = (MetabaseCard.ZeroUm_Usuarios.value if cliente == 'ZEROUM'
                              else MetabaseCard.EnergiaBet_Usuarios.value)
            card_totalizador = (MetabaseCard.ZeroUm_UsuariosTotalizador.value if cliente == 'ZEROUM'
                                 else MetabaseCard.EnergiaBet_UsuariosTotalizador.value)

            auth_id = self.conection(cliente)

            df_usuario = self.extrai_dados_card(auth_id, database, card_usuarios, data_inicial_str, 0)
            df_usuario_ajust = df_usuario[['id','core_account_status','core_user_language','core_wallet_currency','birth_date','first_name','email_verified','email',
                                        'mobile_number_verified','mobile_number','refer_id','sms_allowed','email_allowed','city','state','updated_at','registration_date','import_date',
                                        'first_deposit_date','first_deposit_amount','first_withdraw_date','first_withdraw_amount','last_deposit_date','last_deposit_amount',
                                        'last_withdraw_date','last_withdraw_amount', 'utm', 'status_usuario']]

            util = Util()
            colunas_criptografadas = ['birth_date', 'first_name', 'mobile_number']
            for col in colunas_criptografadas:
                df_usuario_ajust[col] = df_usuario_ajust[col].apply(
                    lambda x: util.descriptografar(cliente, x)
                )

            df_usuario_ajust['birth_date'] = pd.to_datetime(
                df_usuario_ajust['birth_date'],
                format='%d-%m-%Y',
                errors='coerce'
            )

            df_usuario_totalizador = self.extrai_dados_card(auth_id, database, card_totalizador, data_inicial_str, 0)
            df_usuario_totalizador_ajust = df_usuario_totalizador[['id','total_quantity_deposit','total_amount_deposit','total_quantity_withdraw','total_amount_withdraw']]

            # Nativa com filtro por Client.LastUpdateTime embutido — o card original
            # (ZeroUm_UsuariosTotalizadorBet) não tinha nenhum filtro de tempo,
            # agregando o histórico de TODOS os clientes (9M+ linhas em Bet) antes de
            # filtrar. Confirmado: ~5min com filtro embutido vs. >90min sem terminar.
            df_usuario_totalizador_bet = self.extrai_usuario_totalizador_bet_nativo(auth_id, database, data_inicial_str)
            df_usuario_totalizador_bet_ajust = df_usuario_totalizador_bet[['id','total_quantity_bet','total_amount_bet','total_quantity_win','total_amount_win', 'total_quantity_bonus',
                                                                        'total_amount_bonus', 'total_ggr']]

            df_dim_usuario = df_usuario_ajust.merge(df_usuario_totalizador_ajust, on=['id'], how='outer')
            df_dim_usuario = df_dim_usuario.merge(df_usuario_totalizador_bet_ajust, on=['id'], how='outer')
            df_dim_usuario = df_dim_usuario.replace({np.nan: None})

            self.logger.info(f"Backfill: {len(df_dim_usuario)} usuários recuperados a partir de {data_inicial_str}")

            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.deleta_dados('inplay.stg_usuario', "", self.logger)
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.insere_dados_bulk('inplay.stg_usuario', df_dim_usuario, self.logger)
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.mergeia_dados('inplay.stg_usuario', 'inplay.dim_usuario', df_dim_usuario, ['id'], self.logger)

            self.logger.info("Backfill de dim_usuario concluído com sucesso")

            self.db_logger.log_operation(
                operation=f'BACKFILL_DIM_USUARIO_{cliente}',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente=cliente
            )

        except Exception as e:
            self.logger.error(f"Erro no backfill de dim_usuario ({cliente}): {e}")
            self.db_logger.log_operation(
                operation=f'BACKFILL_DIM_USUARIO_{cliente}',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente=cliente
            )
            b = (
                f"Erro no backfill de dim_usuario ({cliente}) a partir de {data_inicial_str}: {e}\n\n"
                f"=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            )
            send_email(subject=f"[FALHA ENGENHARIA] {cliente} - Erro no backfill de dim_usuario", body=b)
            raise

    def backfill_dim_usuario_por_ids(self, cliente: str, ids: list):
        """
        Uso pontual: regulariza registros ESPECÍFICOS de dim_usuario (por
        id), em vez de uma janela de tempo. Feito para casos como: N ids
        que ficaram com carga incompleta (campos nulos de updated_at/
        import_date e demais colunas) por falha anterior no pipeline —
        sem precisar reprocessar toda a janela de datas em que isso
        ocorreu, o que traria de volta muito mais registros do que o
        necessário.

        Mesma lógica de backfill_dim_usuario (extração dos 3 cards de
        usuário, descriptografia, merge, carga em dim_usuario), mas
        filtrando por id em vez de updated_at > data:
        - Usuarios / UsuariosTotalizador: filtro MBQL "=" (lista de ids)
          via extrai_dados_card_por_ids, direto no card do Metabase.
        - UsuariosTotalizadorBet: query nativa com c.Id IN (ids) embutido
          ANTES do JOIN com Bet — evita o full scan de 9M+ linhas mesmo
          sem usar uma janela de tempo (ver _sql_usuario_totalizador_bet_por_ids).
        """
        start_time = datetime.now()
        try:
            self.logger.info(f"Iniciando regularização pontual de dim_usuario ({cliente}) para {len(ids)} ids")

            database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                        if cliente == 'ZEROUM'
                        else MetabaseDatabase.ClickhousePartnerEnergiabet.value)

            card_usuarios = (MetabaseCard.ZeroUm_Usuarios.value if cliente == 'ZEROUM'
                              else MetabaseCard.EnergiaBet_Usuarios.value)
            card_totalizador = (MetabaseCard.ZeroUm_UsuariosTotalizador.value if cliente == 'ZEROUM'
                                 else MetabaseCard.EnergiaBet_UsuariosTotalizador.value)

            auth_id = self.conection(cliente)

            df_usuario = self.extrai_dados_card_por_ids(auth_id, database, card_usuarios, ids)
            df_usuario_ajust = df_usuario[['id','core_account_status','core_user_language','core_wallet_currency','birth_date','first_name','email_verified','email',
                                        'mobile_number_verified','mobile_number','refer_id','sms_allowed','email_allowed','city','state','updated_at','registration_date','import_date',
                                        'first_deposit_date','first_deposit_amount','first_withdraw_date','first_withdraw_amount','last_deposit_date','last_deposit_amount',
                                        'last_withdraw_date','last_withdraw_amount', 'utm', 'status_usuario']]

            util = Util()
            colunas_criptografadas = ['birth_date', 'first_name', 'mobile_number']
            for col in colunas_criptografadas:
                df_usuario_ajust[col] = df_usuario_ajust[col].apply(
                    lambda x: util.descriptografar(cliente, x)
                )

            df_usuario_ajust['birth_date'] = pd.to_datetime(
                df_usuario_ajust['birth_date'],
                format='%d-%m-%Y',
                errors='coerce'
            )

            df_usuario_totalizador = self.extrai_dados_card_por_ids(auth_id, database, card_totalizador, ids)
            df_usuario_totalizador_ajust = df_usuario_totalizador[['id','total_quantity_deposit','total_amount_deposit','total_quantity_withdraw','total_amount_withdraw']]

            df_usuario_totalizador_bet = self.extrai_usuario_totalizador_bet_nativo_por_ids(auth_id, database, ids)
            df_usuario_totalizador_bet_ajust = df_usuario_totalizador_bet[['id','total_quantity_bet','total_amount_bet','total_quantity_win','total_amount_win', 'total_quantity_bonus',
                                                                        'total_amount_bonus', 'total_ggr']]

            df_dim_usuario = df_usuario_ajust.merge(df_usuario_totalizador_ajust, on=['id'], how='outer')
            df_dim_usuario = df_dim_usuario.merge(df_usuario_totalizador_bet_ajust, on=['id'], how='outer')
            df_dim_usuario = df_dim_usuario.replace({np.nan: None})

            self.logger.info(f"Regularização pontual: {len(df_dim_usuario)} de {len(ids)} ids recuperados")

            ids_recuperados = set(df_dim_usuario['id'].astype(str))
            ids_faltando = set(str(i) for i in ids) - ids_recuperados
            if ids_faltando:
                self.logger.warning(
                    f"{len(ids_faltando)} ids não encontrados em nenhum card "
                    f"(podem não existir mais, ou os cards de usuário/totalizadores "
                    f"não os retornaram): {sorted(ids_faltando)}"
                )

            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.deleta_dados('inplay.stg_usuario', "", self.logger)
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.insere_dados_bulk('inplay.stg_usuario', df_dim_usuario, self.logger)
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.mergeia_dados('inplay.stg_usuario', 'inplay.dim_usuario', df_dim_usuario, ['id'], self.logger)

            self.logger.info("Regularização pontual de dim_usuario concluída com sucesso")

            self.db_logger.log_operation(
                operation=f'BACKFILL_DIM_USUARIO_POR_IDS_{cliente}',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente=cliente
            )

        except Exception as e:
            self.logger.error(f"Erro na regularização pontual de dim_usuario ({cliente}): {e}")
            self.db_logger.log_operation(
                operation=f'BACKFILL_DIM_USUARIO_POR_IDS_{cliente}',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente=cliente
            )
            b = (
                f"Erro na regularização pontual de dim_usuario ({cliente}) para {len(ids)} ids: {e}\n\n"
                f"=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            )
            send_email(subject=f"[FALHA ENGENHARIA] {cliente} - Erro na regularização pontual de dim_usuario", body=b)
            raise

    def processa_saldo_diario(self, cliente, modo="incremental", data_final=None):
        """
        Carga isolada de Saldo Diário (ClientDailyBalance).
 
        Escopo reduzido desta entrega (entrega segmentada, ver
        `plano_entrega_saldo_diario.md`): este método cobria antes
        também Saldo Realtime (Account) e Sessões Diárias
        (ClientSession) — essas duas seções foram REMOVIDAS daqui e
        ficam registradas em `backlog_saldo_realtime_sessoes.md` para
        quando o desenvolvimento delas for retomado. Não reintroduzir
        essas seções neste método sem revisitar o backlog primeiro
        (elas dependem de itens ainda pendentes, como o gargalo de
        `insere_dados_bulk` para Saldo Realtime).
 
        Não é chamada por principal_zeroum()/principal_energiabet() —
        roda como um cliente próprio (ZEROUM_SALDO / ENERGIABET_SALDO),
        em cron separado, sem tocar nas tabelas do pipeline horário
        existente (fact_user_daily etc.).
 
        modo="incremental" (padrão): usa max(updated_at) de
        inplay.fact_saldo_diario e filtra a origem por updated_at
        (cursor de sincronização CDC) — correto para operação contínua.
 
        modo="<DATA_ISO>" (ex.: "2025-09-02T00:00:00"): usado para
        backfill histórico. Filtra a origem por data_referencia (data de
        negócio), NÃO por updated_at — necessário porque cargas
        históricas em lote sincronizam tudo com o mesmo updated_at,
        tornando esse campo inútil para localizar períodos antigos.
 
        data_final (opcional): fecha a janela em modo de backfill. Se
        None em modo backfill, extrai até agora.
        """
        start_time = datetime.now()
        try:
            self.logger.info(
                f"Iniciando carga de Saldo Diário - {cliente} (modo={modo}, data_final={data_final})"
            )
            self.logger.info("Fazendo a autenticação no Metabase")
            auth_id = self.conection(cliente)
 
            database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                        if cliente == 'ZEROUM'
                        else MetabaseDatabase.ClickhousePartnerEnergiabet.value)
 
            card_saldo_diario = (MetabaseCard.ZeroUm_SaldoDiario.value
                                  if cliente == 'ZEROUM'
                                  else MetabaseCard.EnergiaBet_SaldoDiario.value)
 
            # ================================================================
            # CHECAGEM DE DISPONIBILIDADE NA ORIGEM (só no modo incremental)
            # ------------------------------------------------------------------
            # Achado desta conversa: a origem (ClientDailyBalance) publica o
            # snapshot completo de um dia só de madrugada do dia SEGUINTE
            # (ex.: o snapshot de 14/07 foi sincronizado só entre 00:17 e
            # 04:18 de 15/07). Rodar o incremental antes disso simplesmente
            # não encontra nada novo — inofensivo, mas gasta chamada de API
            # e Redshift à toa. Esta checagem confirma que o dia esperado
            # (ontem) já está disponível antes de prosseguir; se ainda não
            # estiver, registra e ENCERRA a execução sem erro (é rotina, não
            # falha) — só levanta exceção de verdade se o atraso passar de 1
            # dia, o que já foge do padrão observado e merece alerta.
            #
            # IMPORTANTE — achado desta conversa: o servidor roda em UTC, não
            # em horário de Brasília. `datetime.now()` sozinho devolveria a
            # data UTC, que fica 1 dia à FRENTE da data de Brasília durante a
            # janela das 00:00-02:59 UTC (21:00-23:59 do dia anterior em
            # Brasília) — usar isso "cegamente" poderia fazer o script
            # calcular um "ontem" errado dependendo da hora exata em que
            # rodar. Por isso ancoramos explicitamente em UTC e aplicamos o
            # MESMO ajuste de -3h já usado no SQL (`CreateDate - INTERVAL 3
            # HOUR`), em vez de confiar no fuso horário configurado no SO do
            # servidor.
            # ================================================================
            if modo == "incremental":
                agora_brasilia = datetime.utcnow() - timedelta(hours=3)
                dia_esperado = (agora_brasilia - timedelta(days=1)).date()
                ultimo_dia_disponivel = self._ultimo_dia_disponivel_na_origem(auth_id, database)
                self.logger.info(
                    f"Último dia disponível na origem: {ultimo_dia_disponivel} "
                    f"(esperado pelo menos: {dia_esperado})"
                )
                if ultimo_dia_disponivel < dia_esperado:
                    atraso_dias = (dia_esperado - ultimo_dia_disponivel).days
                    if atraso_dias >= 2:
                        raise Exception(
                            f"Origem (ClientDailyBalance) parece atrasada: último dia "
                            f"disponível é {ultimo_dia_disponivel}, esperado pelo menos "
                            f"{dia_esperado} ({atraso_dias} dias de atraso). Isso passa "
                            "do atraso rotineiro de sincronização (normalmente resolvido "
                            "até a manhã seguinte) — pode indicar um problema real na "
                            "origem/CDC, vale investigar."
                        )
                    self.logger.warning(
                        f"Dado do dia {dia_esperado} ainda não publicado na origem "
                        f"(último disponível: {ultimo_dia_disponivel}). Isso é esperado "
                        "se a sincronização da origem ainda não rodou hoje — encerrando "
                        "esta execução sem processar nada; a próxima execução (cron do "
                        "dia seguinte, ou um run manual) deve pegar normalmente assim "
                        "que a origem publicar."
                    )
                    self.db_logger.log_operation(
                        operation=f'ETL_SALDO_DIARIO_{cliente}',
                        status='SKIPPED',
                        start_time=start_time,
                        end_time=datetime.now(),
                        error_reason=(f"Origem ainda não publicou {dia_esperado} "
                                      f"(último disponível: {ultimo_dia_disponivel})"),
                        cliente=cliente
                    )
                    return
 
            # campo de filtro conforme o modo (achado desta conversa:
            # updated_at não serve para localizar backfill histórico)
            campo_filtro_periodo = "updated_at" if modo == "incremental" else "data_referencia"
 
            # ================================================================
            # SALDO DIÁRIO — chunking automático por período. Campo de
            # filtro depende do modo (ver docstring acima).
            # ================================================================
            if modo == "incremental":
                self.logger.info("Recuperando data base para Saldo Diário")
                ConnectionDB.conecta(DB, cliente)
                data_importacao = ConnectionDB.recupera_dados(
                    'inplay.fact_saldo_diario', 'max(updated_at) as updated_at', ''
                )
                data_base = data_importacao[0][0]
 
                if data_base is None:
                    raise Exception(
                        "inplay.fact_saldo_diario está vazia. A primeira carga NÃO pode "
                        "ser full (volume bruto de origem de até 949M linhas). Chame "
                        "ConsumeAPI(cliente='%s_SALDO', modo='<DATA_CORTE_ISO>'), "
                        "ex.: modo='2025-09-02T00:00:00' (após a sincronização em lote "
                        "confirmada em 01/set/2025), informando uma data de corte "
                        "explícita antes de rodar em modo incremental." % cliente
                    )
                data_inicial_dt = data_base - timedelta(hours=4)
            else:
                data_inicial_dt = datetime.fromisoformat(modo)
 
            data_final_dt = datetime.fromisoformat(data_final) if data_final else datetime.now()
 
            self.logger.info(
                f"Extraindo Saldo Diário ({card_saldo_diario}) [{campo_filtro_periodo}] "
                f"de {data_inicial_dt} a {data_final_dt}"
            )
            df_saldo_diario = self.extrai_dados_card_por_periodo(
                auth_id, database, card_saldo_diario,
                data_inicial_dt, data_final_dt, campo_filtro=campo_filtro_periodo
            )
            df_saldo_diario = df_saldo_diario.replace({np.nan: None})
 
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.deleta_dados('inplay.stg_saldo_diario', "", self.logger)
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.insere_dados_bulk('inplay.stg_saldo_diario', df_saldo_diario, self.logger)
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.mergeia_dados(
                'inplay.stg_saldo_diario', 'inplay.fact_saldo_diario',
                df_saldo_diario, ['usuario', 'data_referencia'], self.logger
            )
 
            self.logger.info(f"Carga de Saldo Diário concluída com sucesso - {cliente}")
 
            self.db_logger.log_operation(
                operation=f'ETL_SALDO_DIARIO_{cliente}',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente=cliente
            )
 
        except requests.exceptions.RequestException as e:
            descricao = self._descricao_erro(e)
            self.logger.error(f"Erro de rede na carga de Saldo Diário {cliente}: {descricao}")
            self.db_logger.log_operation(
                operation=f'ETL_SALDO_DIARIO_{cliente}',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=descricao,
                cliente=cliente
            )
            b = f"Descrição do erro: {descricao}\n\n=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            send_email(subject=f"[FALHA ENGENHARIA] {cliente} - Erro na carga de Saldo Diário", body=b)
            raise Exception(f"Erro na carga de Saldo Diário {cliente}: {descricao}")
        except Exception as e:
            descricao = self._descricao_erro(e)
            self.logger.error(f"Erro na carga de Saldo Diário {cliente}: {descricao}")
            self.db_logger.log_operation(
                operation=f'ETL_SALDO_DIARIO_{cliente}',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=descricao,
                cliente=cliente
            )
            b = f"Descrição do erro: {descricao}\n\n=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            send_email(subject=f"[FALHA ENGENHARIA] {cliente} - Erro na carga de Saldo Diário", body=b)
            raise
 
    @staticmethod
    def _descricao_erro(e: Exception) -> str:
        """
        Monta uma descrição de erro que NUNCA fica vazia/ambígua no
        e-mail de alerta — achado nesta conversa: um e-mail chegou só
        com "Descrição do erro:" sem nada depois. Algumas exceções
        (em especial as encadeadas via `raise ultimo_erro` em
        extrai_csv_nativo, ou exceções HTTP sem corpo de resposta)
        podem ter `str(e)` vazio ou pouco informativo — nesse caso
        cair só em `str(e)` no f-string produz uma linha em branco.
        Aqui sempre prefixamos com o tipo da exceção e usamos
        `repr(e)` como fallback se `str(e)` vier vazio, garantindo que
        sempre haja algo útil pra investigar (mesmo que seja só o
        nome da classe da exceção).
        """
        texto = str(e).strip()
        if not texto:
            texto = repr(e)
        return f"{type(e).__name__}: {texto}"
 
    def _ultimo_dia_disponivel_na_origem(self, auth_id, id_database):
        """
        Consulta o ClickHouse (via SQL nativo, mesma via de
        _sql_saldo_diario) para saber qual e o dia mais recente com
        dado publicado em ClientDailyBalance -- usado para checar, no
        modo incremental, se a origem ja sincronizou o dia esperado
        antes de rodar a extracao de verdade (ver achado desta
        conversa: a origem publica o snapshot completo de um dia so
        de madrugada do dia SEGUINTE).
 
        Consulta rapida (so um max(), sem QUALIFY nem GROUP BY
        pesado) -- o custo dela e desprezivel perto da extracao normal.
        """
        sql = """
SELECT max(toDate(CreateDate - INTERVAL 3 HOUR)) AS ultimo_dia
FROM ClientDailyBalance
WHERE _peerdb_is_deleted = 0
"""
        csv = self.extrai_csv_nativo(auth_id, id_database, sql)
        df = pd.read_csv(io.BytesIO(csv))
        valor = str(df.iloc[0, 0])
        return datetime.strptime(valor[:10], '%Y-%m-%d').date()
 
    @staticmethod
    def _intervalo_dias_referencia(data_inicial_dt, data_final_dt):
        """
        Converte a janela de datetime [data_inicial_dt, data_final_dt)
        (fim tratado como EXCLUSIVO quando cai exatamente à meia-noite)
        no par de datas (dia_inicial, dia_final) que ela representa,
        ambas INCLUSIVE.
 
        Usado em TRÊS lugares que precisam concordar exatamente entre
        si — se divergirem, uma bissecção "correta" por dia pode
        acabar gerando uma query SQL que cobre um dia a mais ou a
        menos do que o pretendido:
          1) `_sql_saldo_diario`, para montar o `BETWEEN` da condição
             (que é inclusive nos dois extremos);
          2) `_ponto_de_corte`, para decidir onde bisseccionar;
          3) `extrai_dados_card_por_periodo`, para saber se a janela
             atual já é "um único dia" (piso temporal).
 
        Exemplo do problema que isso evita: a janela
        [2026-07-10T00:00:00, 2026-07-11T00:00:00) representa só o
        dia 10/07 (o 00:00:00 do dia 11 é o limite exclusivo, não faz
        parte da janela) — mas `data_final_dt.date()` sozinho daria
        11/07, fazendo um BETWEEN ingênuo incluir os dois dias.
        """
        dia_inicial = data_inicial_dt.date()
        if data_final_dt.time() == datetime.min.time() and data_final_dt.date() > dia_inicial:
            dia_final = data_final_dt.date() - timedelta(days=1)
        else:
            dia_final = data_final_dt.date()
        if dia_final < dia_inicial:
            dia_final = dia_inicial
        return dia_inicial, dia_final
 
    @staticmethod
    def _ponto_de_corte(data_inicial_dt, data_final_dt, campo_filtro):
        """
        Escolhe onde bisseccionar uma janela [data_inicial_dt, data_final_dt).
 
        Modo "updated_at": meio bruto do intervalo (comportamento original)
        — cada redução de intervalo já muda a query, então bisseccionar
        por tempo é sempre efetivo.
 
        Modo "data_referencia": bisseccionar pelo RELÓGIO cria fatias
        fracionárias de um mesmo dia sempre que a janela cruza um
        limite de dia perto da borda (ex.: 23:59:59 vira duas fatias
        de menos de 1s cada, ambas ainda representando o MESMO dia de
        calendário para a query). Isso gera uma explosão de chamadas
        desnecessárias antes de finalmente cair no piso e mudar para
        `ClientId`. Em vez disso, bissecciona por CONTAGEM DE DIAS de
        calendário (via `_intervalo_dias_referencia`): corta exatamente
        na fronteira de dia mais próxima do meio, então cada sub-janela
        sempre cobre um número inteiro de dias — a recursão chega
        direto no piso (um único dia) sem gerar fatias de horário
        inúteis pelo caminho.
        """
        if campo_filtro != "data_referencia":
            return data_inicial_dt + (data_final_dt - data_inicial_dt) / 2
 
        dia_inicial, dia_final = ConsumeAPI._intervalo_dias_referencia(
            data_inicial_dt, data_final_dt
        )
        total_dias = (dia_final - dia_inicial).days + 1
        if total_dias <= 1:
            # já é um único dia — não há fronteira de dia para cortar;
            # cai no piso temporal e quem chamou trata via ClientId.
            return data_inicial_dt + (data_final_dt - data_inicial_dt) / 2
 
        dias_primeira_metade = max(1, total_dias // 2)
        # o corte é sempre à meia-noite do primeiro dia da segunda
        # metade — respeita o mesmo contrato de "fim exclusivo à
        # meia-noite" usado por _intervalo_dias_referencia.
        return datetime.combine(dia_inicial + timedelta(days=dias_primeira_metade),
                                 datetime.min.time())
 
    def extrai_dados_card_por_periodo(self, auth_id, id_database, id_card,
                                       data_inicial_dt, data_final_dt,
                                       campo_filtro="updated_at",
                                       client_id_min=None, client_id_max=None):
        """
        Extrai Saldo Diário num período, via SQL nativo (filtro
        empurrado antes do QUALIFY — ver achado desta conversa). id_card
        aqui só identifica a consulta para fins de log (já vem com o
        prefixo "card__" de enums.py, ex.: "card__20433") — a extração
        não passa mais pelo card salvo.
 
        Escopo reduzido desta entrega: este método também extraía
        Sessões Diárias (ramificação por id_card entre
        `_sql_saldo_diario`/`_sql_sessoes_diarias`) — removido daqui.
        Se Sessões Diárias for retomada (ver backlog), reintroduzir essa
        ramificação em vez de chamar `_sql_saldo_diario` direto.
 
        Subdivide automaticamente em DUAS dimensões possíveis quando
        (a) a chamada retorna >= LIMITE_LINHAS_METABASE linhas, ou
        (b) falha por problema de conexão/timeout:
 
        1) Por período (padrão) — bisseciona `data_inicial_dt`/`data_final_dt`
           ao meio. Funciona bem enquanto a janela cobrir MAIS DE UM dia
           de calendário no modo "data_referencia" (a redução do
           intervalo muda a data BETWEEN da query), ou em qualquer
           redução de intervalo no modo "updated_at".
 
        2) Por faixa de ClientId — usada quando a janela de tempo já
           está no "piso": no modo "data_referencia", isso significa
           que `data_inicial_dt` e `data_final_dt` já caem no MESMO dia
           de calendário. BUG CORRIGIDO NESTA REVISÃO: `_sql_saldo_diario`
           filtra por `toDate(CreateDate - INTERVAL 3 HOUR)` (dia inteiro,
           sem hora) quando `campo_filtro="data_referencia"` — então
           qualquer sub-janela de horário DENTRO do mesmo dia gera
           exatamente a mesma query SQL e nunca reduz o resultado
           (a bisseção por tempo ficava presa em loop, sempre batendo
           no teto do Metabase). Filtrar por faixa de `ClientId` não
           tem esse problema porque o `QUALIFY ROW_NUMBER() OVER
           (PARTITION BY ClientId, TypeId, dia ...)` é independente por
           cliente — restringir o `WHERE` a uma faixa de `ClientId` não
           quebra a correção da deduplicação (diferente de tentar
           restringir por horário, que cortaria no meio o histórico de
           sincronização de um mesmo cliente/dia usado para escolher a
           versão mais recente).
        """
        data_inicial_str = data_inicial_dt.strftime('%Y-%m-%dT%H:%M:%S')
        data_final_str = data_final_dt.strftime('%Y-%m-%dT%H:%M:%S')
        duracao = data_final_dt - data_inicial_dt
 
        # "piso temporal": ponto em que bisseccionar por tempo não muda
        # mais a query gerada (ou, no modo updated_at, ponto em que
        # bisseccionar por tempo se torna INSEGURO, não só inútil).
        #
        # Modo "data_referencia": usa o MESMO helper que monta a condição
        # SQL (_sql_saldo_diario) para não divergir sobre quantos dias a
        # janela realmente cobre (ver docstring de _intervalo_dias_referencia).
        # Bisseccionar por dia É seguro aqui porque a partição do QUALIFY
        # inclui "dia" — nunca corta o histórico de um mesmo ClientId+TypeId+dia
        # ao meio.
        #
        # Modo "updated_at": BUG ENCONTRADO NESTA CONVERSA — bisseccionar
        # por tempo NUNCA é seguro neste modo, mesmo cobrindo só alguns
        # minutos. O filtro aqui é por `_peerdb_synced_at` (quando a linha
        # foi sincronizada), mas a partição do QUALIFY é por
        # `ClientId, TypeId, dia` (campo de negócio). Um mesmo
        # ClientId+dia pode ter TypeIds diferentes sincronizados em
        # instantes diferentes — se a janela de tempo for cortada entre
        # esses instantes, cada metade roda seu próprio
        # `GROUP BY ClientId, dia` de forma independente e devolve uma
        # linha PARCIAL (só com os TypeId que caíram naquela metade) para
        # o mesmo usuário+dia. O `pd.concat` das duas metades então gera
        # `usuario+data_referencia` duplicado — com valores de saldo
        # incompletos/errados em cada linha, não apenas duplicidade
        # cosmética (isso quebrou o MERGE com "multiple matches to
        # update the same tuple" — ver conversa). Diferente de dividir
        # por ClientId, que é sempre seguro (nunca corta o histórico de
        # um mesmo cliente ao meio, `PARTITION BY ClientId` garante
        # isso), então no modo updated_at o piso temporal é considerado
        # atingido IMEDIATAMENTE — a única dimensão de subdivisão válida
        # aqui é ClientId.
        mesmo_dia_referencia = False
        if campo_filtro == "data_referencia":
            dia_ini, dia_fim = self._intervalo_dias_referencia(data_inicial_dt, data_final_dt)
            mesmo_dia_referencia = (dia_ini == dia_fim)
            piso_temporal = mesmo_dia_referencia or duracao <= timedelta(seconds=1)
        else:
            piso_temporal = True
 
        faixa_desc = (f" [ClientId {client_id_min}-{client_id_max}]"
                      if client_id_min is not None else "")
        self.logger.info(
            f"Extraindo {id_card} [SQL nativo, {campo_filtro}] de "
            f"{data_inicial_str} a {data_final_str}{faixa_desc}"
        )
 
        sql = self._sql_saldo_diario(data_inicial_str, data_final_str, campo_filtro,
                                      client_id_min, client_id_max)
 
        try:
            csv = self.extrai_csv_nativo(auth_id, id_database, sql)
            df = pd.read_csv(io.BytesIO(csv))
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if piso_temporal:
                return self._particiona_por_client_id(
                    auth_id, id_database, id_card, data_inicial_dt, data_final_dt,
                    campo_filtro, client_id_min, client_id_max,
                    motivo=f"falha de conexão ({type(e).__name__}): {e}"
                )
            self.logger.warning(
                f"{id_card}: falha de conexão ({type(e).__name__}) na janela "
                f"{data_inicial_str}-{data_final_str}{faixa_desc}. Dividindo o "
                "período ao meio e tentando de novo."
            )
            meio = self._ponto_de_corte(data_inicial_dt, data_final_dt, campo_filtro)
            df_primeira_metade = self.extrai_dados_card_por_periodo(
                auth_id, id_database, id_card, data_inicial_dt, meio, campo_filtro,
                client_id_min, client_id_max
            )
            df_segunda_metade = self.extrai_dados_card_por_periodo(
                auth_id, id_database, id_card, meio, data_final_dt, campo_filtro,
                client_id_min, client_id_max
            )
            return pd.concat([df_primeira_metade, df_segunda_metade], ignore_index=True)
 
        if len(df) < self.LIMITE_LINHAS_METABASE:
            return df
 
        if piso_temporal:
            return self._particiona_por_client_id(
                auth_id, id_database, id_card, data_inicial_dt, data_final_dt,
                campo_filtro, client_id_min, client_id_max,
                motivo=f"{len(df)} linhas (teto do Metabase)"
            )
 
        self.logger.warning(
            f"{id_card}: {len(df)} linhas (teto do Metabase) para "
            f"{data_inicial_str}–{data_final_str}{faixa_desc}. Dividindo o "
            "período ao meio."
        )
        meio = self._ponto_de_corte(data_inicial_dt, data_final_dt, campo_filtro)
        df_primeira_metade = self.extrai_dados_card_por_periodo(
            auth_id, id_database, id_card, data_inicial_dt, meio, campo_filtro,
            client_id_min, client_id_max
        )
        df_segunda_metade = self.extrai_dados_card_por_periodo(
            auth_id, id_database, id_card, meio, data_final_dt, campo_filtro,
            client_id_min, client_id_max
        )
        return pd.concat([df_primeira_metade, df_segunda_metade], ignore_index=True)
 
    def _particiona_por_client_id(self, auth_id, id_database, id_card,
                                   data_inicial_dt, data_final_dt, campo_filtro,
                                   client_id_min, client_id_max, motivo):
        """
        Segunda dimensão de subdivisão, usada só quando a janela de
        tempo já está no piso (ver docstring de
        `extrai_dados_card_por_periodo`) e ainda assim estoura o teto
        do Metabase ou falha de conexão. Bisseciona a faixa de
        `ClientId` em vez do período.
 
        `client_id_min`/`client_id_max` = None na primeira chamada
        (nenhum filtro de ClientId ainda) — nesse caso assume a faixa
        completa (0 a TETO_CLIENT_ID_METABASE) para dar o primeiro corte.
        """
        minimo = client_id_min if client_id_min is not None else 0
        maximo = client_id_max if client_id_max is not None else self.TETO_CLIENT_ID_METABASE
 
        data_inicial_str = data_inicial_dt.strftime('%Y-%m-%dT%H:%M:%S')
        data_final_str = data_final_dt.strftime('%Y-%m-%dT%H:%M:%S')
 
        if maximo <= minimo:
            self.logger.error(
                f"{id_card}: {motivo} para {data_inicial_str}-{data_final_str}, "
                f"mas a faixa de ClientId não pode mais ser subdividida "
                f"({minimo}-{maximo}). Retornando o que a API entregar, "
                "possivelmente truncado — revisar manualmente."
            )
            sql = self._sql_saldo_diario(data_inicial_str, data_final_str,
                                          campo_filtro, minimo, maximo)
            csv = self.extrai_csv_nativo(auth_id, id_database, sql)
            return pd.read_csv(io.BytesIO(csv))
 
        self.logger.warning(
            f"{id_card}: {motivo} para {data_inicial_str}-{data_final_str} — a "
            "janela de tempo já está no piso mínimo (não reduz mais o "
            f"resultado). Subdividindo por faixa de ClientId ({minimo}-{maximo}) "
            "em vez de tempo."
        )
        meio = (minimo + maximo) // 2
        df_primeira_faixa = self.extrai_dados_card_por_periodo(
            auth_id, id_database, id_card, data_inicial_dt, data_final_dt,
            campo_filtro, minimo, meio
        )
        df_segunda_faixa = self.extrai_dados_card_por_periodo(
            auth_id, id_database, id_card, data_inicial_dt, data_final_dt,
            campo_filtro, meio + 1, maximo
        )
        return pd.concat([df_primeira_faixa, df_segunda_faixa], ignore_index=True)
 
    
 
    def extrai_csv_nativo(self, auth_id, id_database, sql_query, max_tentativas=3, timeout=180):
        """
        Envia uma query SQL nativa (ClickHouse) direto para a API do
        Metabase, sem passar por um card salvo. Usado para Saldo Diário,
        Sessões Diárias e Saldo Realtime, onde o filtro de período/faixa
        precisa estar embutido no WHERE da própria query (antes do
        QUALIFY) para não pagar o custo de deduplicar a tabela inteira
        a cada chamada — ver achado confirmado com teste manual (9s com
        filtro embutido vs. >5min sem terminar com filtro por fora).

        timeout: em segundos, por tentativa. Default 180s (suficiente para
        saldo diário, que filtra e agrega só a janela pedida). Chamadas que
        agregam histórico completo de um subconjunto de usuários (ver
        extrai_aposta_nativo) precisam de um valor maior — passar
        explicitamente nesses casos.
        """
        header = {
            'Content-Type': 'application/json',
            'Cookie': f'metabase.DEVICE={auth_id}; metabase.SESSION={auth_id}; metabase.TIMEOUT=alive'
        }
        body = {
            "query": {
                "database": id_database,
                "type": "native",
                "native": {"query": sql_query}
            }
        }
 
        ultimo_erro = None
        for tentativa in range(1, max_tentativas + 1):
            try:
                response = requests.post(API_ROTA_CSV, headers=header, json=body, timeout=timeout)
                response.raise_for_status()
                return response.content
            except requests.exceptions.RequestException as e:
                ultimo_erro = e
                self.logger.warning(
                    f"[extrai_csv_nativo] Tentativa {tentativa}/{max_tentativas} "
                    f"falhou (timeout={timeout}s): {e}"
                )
                if tentativa < max_tentativas:
                    time.sleep(5 * tentativa)
 
        descricao = self._descricao_erro(ultimo_erro)
        self.logger.error(f"[extrai_csv_nativo] Falhou após {max_tentativas} tentativas: {descricao}")
        # Não envia e-mail aqui de propósito (achado desta conversa: gerava
        # DOIS e-mails por falha, um daqui e outro do handler de nível mais
        # alto em processa_saldo_diario). A exceção sobe normalmente e quem
        # decide notificar/logar é o chamador — processa_saldo_diario já
        # envia o e-mail (com a mesma descrição, via _descricao_erro) e
        # grava em inplay.etl_execution_logs.
        raise ultimo_erro
    
 
    @staticmethod
    def _sql_saldo_diario(data_inicial, data_final, campo_filtro="data_referencia",
                           client_id_min=None, client_id_max=None):
        if campo_filtro == "data_referencia":
            # datas no formato 'YYYY-MM-DD' (sem hora) — filtra pela data de
            # negócio. ATENÇÃO: como o filtro é por dia (sem hora), duas
            # janelas de datetime diferentes que caiam no mesmo dia geram a
            # MESMA condição aqui — por isso a subdivisão por horário dentro
            # do mesmo dia não funciona (ver extrai_dados_card_por_periodo,
            # que por isso muda para subdivisão por ClientId nesse caso).
            #
            # Usa _intervalo_dias_referencia (em vez de truncar [:10] direto)
            # para tratar corretamente o caso em que `data_final` cai
            # exatamente à meia-noite (fim EXCLUSIVO daquele dia) — sem
            # isso, um BETWEEN ingênuo incluiria um dia a mais do que o
            # pretendido, já que BETWEEN é inclusive nos dois extremos.
            data_inicial_dt = datetime.strptime(data_inicial, '%Y-%m-%dT%H:%M:%S')
            data_final_dt = datetime.strptime(data_final, '%Y-%m-%dT%H:%M:%S')
            dia_inicial, dia_final = ConsumeAPI._intervalo_dias_referencia(
                data_inicial_dt, data_final_dt
            )
            condicao = (
                f"AND toDate(CreateDate - INTERVAL 3 HOUR) "
                f"BETWEEN '{dia_inicial}' AND '{dia_final}'"
            )
        else:
            # incremental: filtra pelo timestamp de sincronização do CDC
            condicao = f"AND _peerdb_synced_at BETWEEN '{data_inicial}' AND '{data_final}'"
 
        # filtro opcional de faixa de ClientId (usado só quando a
        # subdivisão por período já não reduz mais o resultado — ver
        # _particiona_por_client_id). Seguro para o QUALIFY abaixo porque
        # a dedup é PARTITION BY ClientId, ou seja, independente por
        # cliente: restringir a faixa não muda qual linha "vence" a
        # dedup para os clientes que sobram dentro da faixa.
        if client_id_min is not None and client_id_max is not None:
            condicao += f" AND ClientId BETWEEN {client_id_min} AND {client_id_max}"
 
        return f"""
WITH balance_dedup AS (
    SELECT ClientId, TypeId, Balance, CreateDate, _peerdb_synced_at
    FROM ClientDailyBalance
    WHERE _peerdb_is_deleted = 0
    {condicao}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ClientId, TypeId, toDate(CreateDate - INTERVAL 3 HOUR)
        ORDER BY _peerdb_synced_at DESC
    ) = 1
)
SELECT
    ClientId AS usuario,
    toDate(CreateDate - INTERVAL 3 HOUR) AS data_referencia,
    sum(CASE WHEN TypeId = 1  THEN Balance ELSE 0 END) AS saldo_em_uso,
    sum(CASE WHEN TypeId = 2  THEN Balance ELSE 0 END) AS saldo_disponivel,
    sum(CASE WHEN TypeId = 3  THEN Balance ELSE 0 END) AS saldo_booking,
    sum(CASE WHEN TypeId = 12 THEN Balance ELSE 0 END) AS saldo_bonus,
    sum(Balance) AS saldo_total_dia,
    max(_peerdb_synced_at) AS updated_at,
    now() AS import_date
FROM balance_dedup
GROUP BY ClientId, toDate(CreateDate - INTERVAL 3 HOUR)
"""