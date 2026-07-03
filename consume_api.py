import requests
import pandas as pd
import numpy as np
import io
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
    def __init__(self, cliente,  modo="incremental"):
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
            df_primeira_aposta = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_PrimeiraAposta.value, data_inicial, 0)
            df_ultima_aposta = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_UltimaAposta.value, data_inicial, 0)
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

            df_fact_cassino_game_hourly = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_ApostasJogosHora.value, data_inicial, 0)
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
            df_usuario_totalizador_bet = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_UsuariosTotalizadorBet.value, data_inicial, 0)
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
            df_usuario_totalizador_bet = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_UsuariosTotalizadorBet.value, data_inicial, 0)
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

                response = requests.post(self.auth_url, json=self.body)

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

            b = (
                f"Ocorreu um erro na autenticação da API: {e}\n\n"
                f"=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            )

            send_email(
                subject="[FALHA ENGENHARIA] API - Erro na autenticação",
                body=b
            )

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

    def extrai_csv(self, auth_id:str, database:int, table:int=0, card:str='', filter:str=None):
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

            response = requests.post(self.rota, headers=self.header, json=self.body)

            print("requisição realizada")
            #print(response)
            #print(response.content)

            resultado_csv = response.content
            #df = pd.DataFrame(resultado_csv)
            return resultado_csv
        except Exception as e:
            self.logger.error(f"Erro ao extrair CSV da API: {e}")
            b = (
                f"Ocorreu um erro ao extrair CSV da API: {e}\n\n"
                f"=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            )
            send_email(subject="[FALHA ENGENHARIA] API - Erro ao extrair CSV", body=b)
            raise
                                               
    def extrai_dados_card(self, auth_id, id_database, id_card, data_inicial, data_final):
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
                csv = self.extrai_csv(auth_id, id_database, 0, id_card)
            elif (data_inicial != 0 and data_final == 0):
                csv = self.extrai_csv(auth_id, id_database, 0, id_card, [">",["field","updated_at",{"base-type":"type/DateTime"}], data_inicial])
            else:
                csv = self.extrai_csv(auth_id, id_database, 0, id_card, ["between",["field","updated_at",{"base-type":"type/DateTime"}], data_inicial, data_final])
            df = pd.read_csv(io.BytesIO(csv))
            return df
        except Exception as e:
            self.logger.error(f"Erro ao extrair os dados da consulta {id_card}: {e}")
            b = (
                f"Erro no card {id_card}: {e}\n\n"
                f"=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            )
            send_email(subject=f"[FALHA ENGENHARIA] Metabase - Erro card {id_card}", body=b)
            raise
    

    def extrair_zro_1_bet(self, modo="incremental"):
        print("1 - antes conexão")
        conn = self.conection("ZRO_1_BET")
        print("2 - depois conexão")
        # histórico vs incremental
        print("EXECUÇÃO ID:", id(self))
        print("MODO:", modo)
        if modo == "historico":
            query = """
                SELECT *
                FROM zro1_bet_adtk.vendas_data
            """
        else:
            query = """
                SELECT *
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