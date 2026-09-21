import requests
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime, timedelta, date
from config import API_AUTH, API_USER_ZEROUM, API_PASS_ZEROUM, API_USER_ENERGIABET, API_PASS_ENERGIABET, API_ROTA_CSV, DB, METABASE_CARD_USUARIOS_ZEROUM, METABASE_CARD_USUARIOS_ENERGIABET
from enums import MetabaseTable, MetabaseDatabase, MetabaseCard
from database import ConnectionDB
import logging
from logging.handlers import RotatingFileHandler
from send_email import send_email
import os
from sqlalchemy import create_engine
from sqlalchemy import text
import psycopg2
from agregacao_dim_usuario import executar_agregacao_dim_usuario
from db_logger import DBLogger
from agregacao_cohort_retencao import executar_agregacao_cohort_retencao, executar_kpi_diario_datatalk
from agregacao_bonus import executar_agregacao_bonus_concessoes
from agregacao_fraude_bonus import executar_agregacao_fraude_bonus
from dispara_alerta_bonus import executar_disparo_alertas_bonus

# CSVs de conferência (validações origem x destino) -- só para uso LOCAL.
# Em produção a variável não existe (ou é "false") e nenhum arquivo é gravado.
# Para gerar localmente: EXPORTA_CSV_CONFERENCIA=true no .env da sua máquina.
# (config.py já chama load_dotenv() ao ser importado acima.)
EXPORTA_CSV_CONFERENCIA = os.getenv("EXPORTA_CSV_CONFERENCIA", "false").lower() == "true"


def salva_csv_conferencia(df, nome_arquivo):
    """Grava CSV de conferência só se EXPORTA_CSV_CONFERENCIA=true (uso local)."""
    if EXPORTA_CSV_CONFERENCIA:
        df.to_csv(nome_arquivo, index=False, encoding="utf-8")


class ConsumeAPI:

    LIMITE_LINHAS_METABASE = 1048575  # teto de exportação CSV do Metabase (2^20 - 1)

    TETO_CLIENT_ID_METABASE = 50_000_000
 
   
    def __init__(self, cliente,  modo="incremental", data_final=None, data_inicial_backfill=None, ids_backfill=None,
                 dias_backfill=None, partner_id=None):
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
        elif cliente == 'ZEROUM_VALIDACAO_PERIODO':
            # Reaproveita --data-inicial-backfill/--data-final como início/fim
            # do período a validar (mesmo padrão usado nos backfills abaixo),
            # em vez de reprocessar o histórico inteiro como ZEROUM_VALIDACAO.
            # 'data_referencia' nos cards de origem é DATE (sem hora), então
            # aqui o formato esperado é YYYY-MM-DD, não datetime completo.
            # Ex.: --data-inicial-backfill=2026-09-05 --data-final=2026-09-15
            if not data_inicial_backfill or not data_final:
                self.logger.error(
                    "ZEROUM_VALIDACAO_PERIODO requer --data-inicial-backfill e --data-final "
                    "(formato YYYY-MM-DD, ex.: 2026-09-05)"
                )
            else:
                self.valida_dados_periodo('ZEROUM', data_inicial_backfill, data_final)
        elif cliente == 'ENERGIABET':
            self.principal_energiabet()
        elif cliente == 'ENERGIABET_VALIDACAO':
            self.valida_dados('ENERGIABET')
        elif cliente == 'ENERGIABET_VALIDACAO_PERIODO':
            if not data_inicial_backfill or not data_final:
                self.logger.error(
                    "ENERGIABET_VALIDACAO_PERIODO requer --data-inicial-backfill e --data-final "
                    "(formato YYYY-MM-DD, ex.: 2026-09-05)"
                )
            else:
                self.valida_dados_periodo('ENERGIABET', data_inicial_backfill, data_final)
        elif cliente == 'ZEROUM_REPROCESSA_DIAS_PONTUAIS':
            # Usa --dias-backfill (dedicado — NÃO reaproveita --ids-backfill,
            # que o main.py já converte pra int, incompatível com datas).
            # Ex.: --dias-backfill=2026-09-05,2026-09-06,2026-09-14
            if not dias_backfill:
                self.logger.error(
                    "ZEROUM_REPROCESSA_DIAS_PONTUAIS requer --dias-backfill com a lista de dias "
                    "(formato YYYY-MM-DD separados por vírgula, ex.: 2026-09-05,2026-09-06,2026-09-14)"
                )
            else:
                self.reprocessa_dias_pontuais('ZEROUM', dias_backfill)
        elif cliente == 'ENERGIABET_REPROCESSA_DIAS_PONTUAIS':
            if not dias_backfill:
                self.logger.error(
                    "ENERGIABET_REPROCESSA_DIAS_PONTUAIS requer --dias-backfill com a lista de dias "
                    "(formato YYYY-MM-DD separados por vírgula, ex.: 2026-09-05,2026-09-06,2026-09-14)"
                )
            else:
                self.reprocessa_dias_pontuais('ENERGIABET', dias_backfill)
        elif cliente == 'ZEROUM_REPROCESSA_FACT_USER_DAILY':
            # Reaproveita --dias-backfill. Reprocessa Stage (aposta) e Saque
            # de fact_user_daily para os dias informados.
            if not dias_backfill:
                self.logger.error(
                    "ZEROUM_REPROCESSA_FACT_USER_DAILY requer --dias-backfill com a lista de dias "
                    "(formato YYYY-MM-DD separados por vírgula, ex.: 2026-09-05,2026-09-06)"
                )
            else:
                self.reprocessa_fact_user_daily_dias('ZEROUM', dias_backfill)
        elif cliente == 'ENERGIABET_REPROCESSA_FACT_USER_DAILY':
            if not dias_backfill:
                self.logger.error(
                    "ENERGIABET_REPROCESSA_FACT_USER_DAILY requer --dias-backfill com a lista de dias "
                    "(formato YYYY-MM-DD separados por vírgula, ex.: 2026-09-05,2026-09-06)"
                )
            else:
                self.reprocessa_fact_user_daily_dias('ENERGIABET', dias_backfill)
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
        elif cliente == 'ZEROUM_BACKFILL_HISTORICO_PROTECAO':
            # Carga histórica ÚNICA da melhoria de proteção de dados pessoais
            # (Frente A: lastname/taxnumber/documento/kyc: Frente B: colunas
            # first_name_protegido/mobile_number_protegido/birth_date_protegido).
            # Uso único, roda contra o card de teste em validação (via
            # METABASE_CARD_USUARIOS_ZEROUM no .env) e depois contra produção.
            # Reaproveita data_inicial_backfill/data_final como início/fim do
            # intervalo histórico a cobrir (ex.: --data-inicial-backfill=2015-01-01T00:00:00
            # --data-final=2026-08-01T00:00:00).
            if not data_inicial_backfill or not data_final:
                self.logger.error(
                    "ZEROUM_BACKFILL_HISTORICO_PROTECAO requer --data-inicial-backfill "
                    "e --data-final (formato YYYY-MM-DDTHH:MM:SS, ex.: 2015-01-01T00:00:00)"
                )
            else:
                self.backfill_historico_protecao_dados_pessoais('ZEROUM', data_inicial_backfill, data_final)
        elif cliente == 'ENERGIABET_BACKFILL_HISTORICO_PROTECAO':
            if not data_inicial_backfill or not data_final:
                self.logger.error(
                    "ENERGIABET_BACKFILL_HISTORICO_PROTECAO requer --data-inicial-backfill "
                    "e --data-final (formato YYYY-MM-DDTHH:MM:SS, ex.: 2015-01-01T00:00:00)"
                )
            else:
                self.backfill_historico_protecao_dados_pessoais('ENERGIABET', data_inicial_backfill, data_final)
        elif cliente == 'ZEROUM_BACKFILL_BONUS_AWARDING_NULO':
            # Backfill suplementar de inplay.fact_user_bonus: cobre os
            # ClientBonus com AwardingTime NULL (achado real: ~9,6M+
            # registros, majoritariamente Status=6 / BonusType 12,14,15 --
            # freebet/freespin/riskfree, que não passam por ativação
            # explícita). Nunca capturados pelo backfill principal
            # (processa_bonus_backfill, filtrado por AwardingTime).
            # Reaproveita data_inicial_backfill/data_final como início/fim
            # do intervalo (por CreationTime).
            if not data_inicial_backfill or not data_final:
                self.logger.error(
                    "ZEROUM_BACKFILL_BONUS_AWARDING_NULO requer --data-inicial-backfill "
                    "e --data-final (formato YYYY-MM-DDTHH:MM:SS, ex.: 2025-04-11T00:00:00)"
                )
            else:
                self.processa_bonus_backfill_awarding_nulo('ZEROUM', data_inicial_backfill, data_final, partner_id=partner_id)
        elif cliente == 'ENERGIABET_BACKFILL_BONUS_AWARDING_NULO':
            if not data_inicial_backfill or not data_final:
                self.logger.error(
                    "ENERGIABET_BACKFILL_BONUS_AWARDING_NULO requer --data-inicial-backfill "
                    "e --data-final (formato YYYY-MM-DDTHH:MM:SS, ex.: 2025-04-11T00:00:00)"
                )
            else:
                self.processa_bonus_backfill_awarding_nulo('ENERGIABET', data_inicial_backfill, data_final, partner_id=partner_id)
        elif cliente == 'ZEROUM_BONUS':
            # Carga recorrente (cron diário) de Bônus, ISOLADA do pipeline
            # horário principal (principal_zeroum). Uso:
            #   incremental (padrão, agendar diariamente):
            #     python main.py --cliente ZEROUM_BONUS
            #   backfill histórico (data de corte explícita):
            #     python main.py --cliente ZEROUM_BONUS --modo 2025-04-01T00:00:00 --data-final 2025-05-01T00:00:00
            self.processa_bonus('ZEROUM', modo=modo, data_final=data_final, partner_id=partner_id)
        elif cliente == 'ENERGIABET_BONUS':
            # partner_id OBRIGATÓRIO até confirmação -- ver processa_bonus().
            # Chamar via python -c, não via CLI:
            #   ConsumeAPI(cliente='ENERGIABET_BONUS', partner_id=<valor confirmado>)
            self.processa_bonus('ENERGIABET', modo=modo, data_final=data_final, partner_id=partner_id)
        elif cliente == 'ZEROUM_PIX':
            # Carga recorrente (cron diário) de Agregação Pix, ISOLADA do
            # pipeline horário principal (principal_zeroum) e do cron do
            # Saldo Diário/Bônus — agendar como entrada própria. Uso:
            #   incremental (padrão, agendar diariamente):
            #     python main.py --cliente ZEROUM_PIX
            #   backfill único, obrigatório antes do primeiro incremental:
            #     ConsumeAPI(cliente='ZEROUM_PIX', modo='full')  # via python -c, não via CLI
            self.processa_agregacao_pix('ZEROUM', modo=modo, data_final=data_final, partner_id=partner_id)
        elif cliente == 'ENERGIABET_PIX':
            # partner_id OBRIGATÓRIO até confirmação -- ver processa_agregacao_pix().
            # Chamar via python -c, não via CLI:
            #   ConsumeAPI(cliente='ENERGIABET_PIX', modo='full', partner_id=<valor confirmado>)
            self.processa_agregacao_pix('ENERGIABET', modo=modo, data_final=data_final, partner_id=partner_id)
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

    def _recupera_data_base_incremental(self, cliente: str, tabelas: list):
        """
        Calcula a data base para o incremental olhando o max(updated_at) de
        TODAS as tabelas de destino do pipeline (não só a primeira), e
        retorna a MAIS DESATUALIZADA (o mínimo entre os máximos).

        Motivação (ponto pendente desta conversa): principal_zeroum/
        principal_energiabet gravam em sequência várias tabelas de destino
        (fact_user_daily, fact_user_daily_sport,
        fact_deposits_withdraws_summarized, fact_casino_games_hourly,
        dim_usuario). Se uma rodada anterior quebrar no meio do caminho
        (ex.: erro na API do Metabase, timeout, MEMORY_LIMIT_EXCEEDED), as
        tabelas gravadas ANTES da falha ficam com updated_at mais recente
        que as gravadas DEPOIS — as datas de updated_at das tabelas
        divergem entre si.

        Usar só uma tabela (a primeira, fact_user_daily) como já era feito
        antes fazia a rodada seguinte calcular data_inicial a partir do
        ponto mais avançado, pulando exatamente a janela que faltou gravar
        nas tabelas que não chegaram a rodar — um buraco silencioso nos
        dados.

        Usando o MÍNIMO entre os MAX(updated_at) de todas as tabelas,
        data_inicial sempre parte do ponto da tabela mais atrasada:
        reprocessa um pouco a mais nas tabelas que já estavam em dia (sem
        problema, o merge é idempotente/upsert), mas nunca deixa buraco na
        que ficou pra trás.
        """
        datas_por_tabela = {}
        for tabela in tabelas:
            ConnectionDB.conecta(DB, cliente)
            resultado = ConnectionDB.recupera_dados(tabela, 'max(updated_at) as updated_at', '')
            data_tabela = resultado[0][0] if resultado else None
            if data_tabela is None:
                self.logger.warning(
                    f"[{cliente}] {tabela}: max(updated_at) veio nulo (tabela "
                    "vazia?) — ignorada no cálculo da data base incremental."
                )
                continue
            datas_por_tabela[tabela] = data_tabela

        if not datas_por_tabela:
            raise ValueError(
                f"[{cliente}] Não foi possível recuperar updated_at de "
                f"nenhuma das tabelas de destino {tabelas} para calcular a "
                "data base incremental."
            )

        tabela_mais_desatualizada = min(datas_por_tabela, key=datas_por_tabela.get)
        data_base = datas_por_tabela[tabela_mais_desatualizada]

        if len(set(datas_por_tabela.values())) > 1:
            detalhes = ", ".join(
                f"{tabela}={data}" for tabela, data in
                sorted(datas_por_tabela.items(), key=lambda item: item[1])
            )
            self.logger.warning(
                f"[{cliente}] Datas de updated_at divergentes entre as "
                f"tabelas de destino (provável rodada anterior interrompida "
                f"no meio do caminho): {detalhes}. Usando a mais "
                f"desatualizada ({tabela_mais_desatualizada} = {data_base}) "
                "como data base para não deixar buraco no incremental."
            )
        else:
            self.logger.info(
                f"[{cliente}] Datas de updated_at em sincronia entre as "
                f"tabelas de destino: {data_base}."
            )

        return data_base

    def principal_zeroum(self):
        try:
            start_time = datetime.now()
            # Lock best-effort contra execução concorrente (ver docstring de
            # DBLogger.esta_rodando/log_start em db_logger.py — achado desta
            # conversa: os erros "Found multiple matches to update the same
            # tuple" na recuperação do incidente batiam com 2+ instâncias de
            # ETL_ZEROUM rodando ao mesmo tempo, disputando a mesma tabela
            # de staging).
            if self.db_logger.esta_rodando('ETL_ZEROUM', 'ZEROUM'):
                self.logger.warning(
                    "[LOCK] Já existe uma execução de ETL_ZEROUM em andamento "
                    "(iniciada há menos de 30 min) — abortando esta execução "
                    "para evitar concorrência."
                )
                return
            self.db_logger.log_start('ETL_ZEROUM', 'ZEROUM', start_time)
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
            self.logger.info("Recuperando a data base para consulta")
            data_base = self._recupera_data_base_incremental('ZEROUM', [
                'inplay.fact_user_daily',
                'inplay.fact_user_daily_sport',
                'inplay.fact_deposits_withdraws_summarized',
                'inplay.fact_casino_games_hourly',
                'inplay.dim_usuario',
            ])
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

            #df_usuario = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_Usuarios.value, data_inicial, 0)
            df_usuario = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, METABASE_CARD_USUARIOS_ZEROUM, data_inicial, 0)
            # LEITURA B (decisão de produto): first_name/birth_date/mobile_number
            # NÃO são mais selecionados nem gravados -- essas colunas ficam
            # congeladas em dim_usuario a partir daqui, sem receber nenhuma
            # atualização futura da incremental. Só as colunas "_protegido"
            # (cifradas, já vêm prontas do card -- ver SELECT) continuam sendo
            # alimentadas. Nenhum decrypt acontece mais neste método.
            df_usuario_ajust = df_usuario[['id','core_account_status','core_user_language','core_wallet_currency','email_verified','email',
                                        'mobile_number_verified','refer_id','sms_allowed','email_allowed','city','state','updated_at','registration_date','import_date',
                                        'first_deposit_date','first_deposit_amount','first_withdraw_date','first_withdraw_amount','last_deposit_date','last_deposit_amount',
                                        'last_withdraw_date','last_withdraw_amount', 'utm', 'status_usuario',
                                        # 8 campos novos (Frente A)
                                        'LastName', 'TaxNumber', 'DocumentType', 'DocumentNumber',
                                        'DocumentIssuedBy', 'IsDocumentVerified', 'KYCStatus', 'KYCDocsStatus',
                                        # Frente B: já vêm cifrados do card, sem tratamento em Python
                                        'first_name_protegido', 'mobile_number_protegido', 'birth_date_protegido']]


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
            if self.db_logger.esta_rodando('ETL_ENERGIABET', 'ENERGIABET'):
                self.logger.warning(
                    "[LOCK] Já existe uma execução de ETL_ENERGIABET em andamento "
                    "(iniciada há menos de 30 min) — abortando esta execução "
                    "para evitar concorrência."
                )
                return
            self.db_logger.log_start('ETL_ENERGIABET', 'ENERGIABET', start_time)
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
            self.logger.info("Recuperando a data base para consulta")
            data_base = self._recupera_data_base_incremental('ENERGIABET', [
                'inplay.fact_user_daily',
                'inplay.fact_user_daily_sport',
                'inplay.fact_deposits_withdraws_summarized',
                'inplay.fact_casino_games_hourly',
                'inplay.dim_usuario',
            ])
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

            df_usuario = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, METABASE_CARD_USUARIOS_ENERGIABET, data_inicial, 0)
            # LEITURA B (decisão de produto): first_name/birth_date/mobile_number
            # NÃO são mais selecionados nem gravados -- essas colunas ficam
            # congeladas em dim_usuario a partir daqui. Só as colunas
            # "_protegido" (cifradas, já vêm prontas do card) continuam sendo
            # alimentadas. Nenhum decrypt acontece mais neste método.
            df_usuario_ajust = df_usuario[['id','core_account_status','core_user_language','core_wallet_currency','email_verified','email',
                                        'mobile_number_verified','refer_id','sms_allowed','email_allowed','city','state','updated_at','registration_date','import_date',
                                        'first_deposit_date','first_deposit_amount','first_withdraw_date','first_withdraw_amount','last_deposit_date','last_deposit_amount',
                                        'last_withdraw_date','last_withdraw_amount', 'utm',  'status_usuario',
                                        # 8 campos novos (Frente A)
                                        'LastName', 'TaxNumber', 'DocumentType', 'DocumentNumber',
                                        'DocumentIssuedBy', 'IsDocumentVerified', 'KYCStatus', 'KYCDocsStatus',
                                        # Frente B: já vêm cifrados do card, sem tratamento em Python
                                        'first_name_protegido', 'mobile_number_protegido', 'birth_date_protegido']]


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
                # ACHADO DESTA CONVERSA: antes disso era só logado e o fluxo
                # seguia, passando o corpo de erro (JSON) pro pd.read_csv como
                # se fosse CSV válido — gerava colunas com nome literal tipo
                # '{"database_id":67' e quebrava mais na frente com um erro de
                # sintaxe SQL confuso, bem longe da causa real. Agora falha
                # aqui, na hora, com o motivo real.
                raise Exception(
                    f"[extrai_csv] Metabase retornou status {response.status_code} para card {card} "
                    f"— extração abortada (corpo da resposta não é CSV válido). "
                    f"Detalhe: {response.text[:500]}"
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
                           campo_filtro="updated_at", tipo_campo="type/DateTime", timeout=2400,
                           _profundidade=0):
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

            # ACHADO DESTA CONVERSA: o Metabase corta a exportação de CSV em
            # LIMITE_LINHAS_METABASE linhas — e até agora só a carga de Saldo
            # Diário (extrai_dados_card_por_periodo) se protegia disso. Se um
            # card do pipeline principal chegasse perto/no teto durante uma
            # janela grande (ex.: catch-up depois de uma parada longa), o
            # Metabase truncaria silenciosamente, sem gerar erro nenhum.
            # Replica aqui o mesmo princípio: se bateu perto do teto, divide a
            # janela de datas ao meio e tenta de novo recursivamente.
            if data_inicial != 0 and len(df) >= self.LIMITE_LINHAS_METABASE:
                if _profundidade >= 20:
                    self.logger.warning(
                        f"[extrai_dados_card] card={id_card}: profundidade máxima de divisão "
                        f"atingida (20) e ainda retornou {len(df)} linhas — devolvendo como está, "
                        f"revisar manualmente."
                    )
                    return df
                try:
                    limite_inferior = pd.to_datetime(data_inicial)
                    limite_superior = datetime.now() if data_final == 0 else pd.to_datetime(data_final)
                except Exception:
                    self.logger.warning(
                        f"[extrai_dados_card] card={id_card}: {len(df)} linhas perto do teto do "
                        f"Metabase, mas não foi possível interpretar data_inicial/data_final "
                        f"({data_inicial!r}/{data_final!r}) para dividir a janela — devolvendo como está."
                    )
                    return df
                duracao = limite_superior - limite_inferior
                if duracao <= timedelta(minutes=1):
                    self.logger.warning(
                        f"[extrai_dados_card] card={id_card}: janela mínima atingida "
                        f"({limite_inferior} a {limite_superior}) e ainda assim retornou "
                        f"{len(df)} linhas — possível truncamento residual, revisar manualmente."
                    )
                    return df
                meio = limite_inferior + duracao / 2
                meio_str = meio.strftime('%Y-%m-%dT%H:%M:%S')
                self.logger.warning(
                    f"[extrai_dados_card] card={id_card}: {len(df)} linhas (perto/no teto de "
                    f"{self.LIMITE_LINHAS_METABASE} do Metabase) para {data_inicial}–"
                    f"{data_final or 'agora'}. Dividindo a janela ao meio e tentando de novo."
                )
                data_final_primeira_metade = meio_str
                data_final_segunda_metade = data_final if data_final != 0 else 0
                df_primeira_metade = self.extrai_dados_card(
                    auth_id, id_database, id_card, data_inicial, data_final_primeira_metade,
                    campo_filtro, tipo_campo, timeout, _profundidade + 1
                )
                df_segunda_metade = self.extrai_dados_card(
                    auth_id, id_database, id_card, meio_str, data_final_segunda_metade,
                    campo_filtro, tipo_campo, timeout, _profundidade + 1
                )
                return pd.concat([df_primeira_metade, df_segunda_metade], ignore_index=True)

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
  AND _peerdb_is_deleted = 0
  AND ClientId IN (
      SELECT DISTINCT ClientId
      FROM Bet
      WHERE {filtro_base}
        AND _peerdb_is_deleted = 0
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
),
bet_dedup AS (
    SELECT
        Id,
        argMax(ClientId, _peerdb_version) AS bet_client_id,
        argMax(BetAmount, _peerdb_version) AS BetAmount,
        argMax(WinAmount, _peerdb_version) AS WinAmount,
        argMax(BetBonusAmount, _peerdb_version) AS BetBonusAmount,
        argMax(Ggr, _peerdb_version) AS Ggr,
        argMax(State, _peerdb_version) AS State,
        argMax(ProductId, _peerdb_version) AS ProductId
    FROM Bet
    WHERE _peerdb_is_deleted = 0
      AND ClientId IN (
          SELECT Id FROM cliente_ajustado WHERE LastUpdateTime > '{data_inicial}'
      )
    GROUP BY Id
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
FROM bet_dedup b
INNER JOIN cliente_ajustado c ON b.bet_client_id = c.Id
WHERE b.State NOT IN (1, 4) AND b.ProductId != 6
GROUP BY c.Id
"""

    @staticmethod
    def _sql_usuario_totalizador_bet_por_ids(ids: list) -> str:
        """
        Variante de _sql_usuario_totalizador_bet para regularização pontual
        de registros específicos.

        Filtra a tabela Bet pelos ClientId desejados antes da deduplicação
        por Id/_peerdb_version, evitando processar a Bet inteira.
        """

        ids_sql = ", ".join(str(int(i)) for i in ids)

        return f"""
    WITH cliente_ajustado AS (

        SELECT DISTINCT ON (c.Id) c.*
        FROM Client c
        WHERE c.Id IN ({ids_sql})
        ORDER BY c.Id, c.LastSessionId DESC

    ),

    bet_filtrado AS (

        SELECT
            Id,
            ClientId,
            BetAmount,
            WinAmount,
            BetBonusAmount,
            Ggr,
            State,
            ProductId,
            _peerdb_version,
            _peerdb_is_deleted
        FROM Bet
        WHERE
            _peerdb_is_deleted = 0
            AND ClientId IN ({ids_sql})

    ),

    bet_dedup AS (

        SELECT
            Id,
            argMax(ClientId, _peerdb_version) AS bet_client_id,
            argMax(BetAmount, _peerdb_version) AS BetAmount,
            argMax(WinAmount, _peerdb_version) AS WinAmount,
            argMax(BetBonusAmount, _peerdb_version) AS BetBonusAmount,
            argMax(Ggr, _peerdb_version) AS Ggr,
            argMax(State, _peerdb_version) AS State,
            argMax(ProductId, _peerdb_version) AS ProductId
        FROM bet_filtrado
        GROUP BY Id

    )

    SELECT

        c.Id AS id,

        sum(
            CASE
                WHEN b.BetAmount > 0 THEN 1
                ELSE 0
            END
        ) AS total_quantity_bet,

        round(sum(b.BetAmount), 2) AS total_amount_bet,

        sum(
            CASE
                WHEN b.WinAmount > 0 THEN 1
                ELSE 0
            END
        ) AS total_quantity_win,

        round(sum(b.WinAmount), 2) AS total_amount_win,

        sum(
            CASE
                WHEN b.BetBonusAmount > 0 THEN 1
                ELSE 0
            END
        ) AS total_quantity_bonus,

        round(sum(b.BetBonusAmount), 2) AS total_amount_bonus,

        round(sum(b.Ggr), 2) AS total_ggr,

        max(c.LastUpdateTime) AS updated_at,

        toDate(min(c.CreationTime)) AS data_referencia

    FROM bet_dedup b

    INNER JOIN cliente_ajustado c
        ON b.bet_client_id = c.Id

    WHERE
        b.State NOT IN (1, 4)
        AND b.ProductId != 6

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

        self.logger.info(
            f"[DEBUG BET] Quantidade de IDs no lote: {len(ids)}"
        )
        self.logger.info(
            f"[DEBUG BET] Primeiros IDs: {ids[:10]}"
        )
        self.logger.info(f"Extraindo totalizador bet de usuário para {len(ids)} ids específicos (query nativa)")
        sql = self._sql_usuario_totalizador_bet_por_ids(ids)
        csv = self.extrai_csv_nativo(auth_id, id_database, sql, timeout=900)
        df = pd.read_csv(io.BytesIO(csv))
        return df

    @staticmethod
    def _sql_apostas_jogos_hora(data_inicial: str, partner_id: int) -> str:
        # PRECAUÇÃO (ver conversa): mesmo padrão estrutural que já confirmou
        # o bug "ILLEGAL_AGGREGATION" em _sql_usuario_totalizador_bet —
        # agregado com o MESMO nome da coluna original (ProductId AS
        # ProductId), usado depois num JOIN por fora (INNER JOIN Product p
        # ON p.Id = b.ProductId). Renomeado para bet_product_id como
        # precaução, mesmo sem confirmação direta de falha aqui (é uma
        # subquery FROM (...), não uma CTE WITH — o bug pode ou não se
        # manifestar do mesmo jeito, mas o padrão de risco é idêntico).
        return f"""
    SELECT
        toTimeZone(b.LastUpdateTimeMax, 'America/Sao_Paulo')::DATE AS data_referencia,
        formatDateTime(toStartOfHour(toTimeZone(toDateTime(LastUpdateTimeMax, 'UTC'), 'America/Sao_Paulo')),'%Y-%m-%d %H:%i:%S') AS reference,
        b.bet_product_id AS game_id,
        sum(CASE WHEN b.BonusId IS NOT NULL THEN 1 ELSE 0 END) AS with_bonus,
        round(sum(b.BetAmount),2) AS bet_amount,
        round(sum(b.WinAmount),2) AS win_amount,
        sum(CASE WHEN b.BetAmount > 0 THEN 1 ELSE 0 END) AS bet_qty,
        sum(CASE WHEN b.WinAmount > 0 THEN 1 ELSE 0 END) AS win_qty,
        round(sum(ifNull(b.BetAmount, 0) - ifNull(b.WinAmount, 0)), 2) AS ggr_amount,
        max(toTimeZone(b.LastUpdateTimeMax, 'America/Sao_Paulo')) AS updated_at,
        now() AS data_importacao
    FROM (
        SELECT
            Id,
            argMax(BetAmount, _peerdb_version) AS BetAmount,
            argMax(WinAmount, _peerdb_version) AS WinAmount,
            argMax(BonusId, _peerdb_version) AS BonusId,
            argMax(State, _peerdb_version) AS State,
            argMax(ProductId, _peerdb_version) AS bet_product_id,
            argMax(PartnerId, _peerdb_version) AS PartnerId,
            argMax(LastUpdateTime, _peerdb_version) AS LastUpdateTimeMax
        FROM Bet
        WHERE _peerdb_is_deleted = 0
        AND LastUpdateTime > '{data_inicial}'
        GROUP BY Id
    ) b
    INNER JOIN Product p ON p.Id = b.bet_product_id
    WHERE b.State NOT IN (1, 4) AND b.bet_product_id != 6 AND b.PartnerId = {partner_id}
    GROUP BY b.bet_product_id, reference, toTimeZone(b.LastUpdateTimeMax, 'America/Sao_Paulo')::DATE
    ORDER BY toTimeZone(b.LastUpdateTimeMax, 'America/Sao_Paulo')::DATE DESC
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

           #     print(f"Batch {i} -> {i + len(chunk)} inserido")

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
            salva_csv_conferencia(df_usuario_origem, nome_arquivo)

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
            salva_csv_conferencia(df_validacao, nome_arquivo)
                
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

    def valida_dados_periodo(self, cliente: str, data_inicio: str, data_fim: str,
                              campo_filtro_origem: str = "data_referencia",
                              tipo_campo_origem: str = "type/Date"):
        """
        Mesma comparação origem x destino de valida_dados(), mas restrita a
        um período [data_inicio, data_fim), em vez de recalcular o
        histórico inteiro a cada execução.

        Criada para apurações pontuais (ex.: conferência de um intervalo
        específico após um incidente) sem precisar reprocessar/sobrescrever
        a tabela de validação inteira, nem reextrair o histórico completo
        da origem, só pra checar alguns dias.

        data_inicio / data_fim: string de data, ex. '2026-09-05' e
        '2026-09-15' — sem hora, porque 'data_referencia' nos cards de
        origem é um campo DATE (confirmado: é o resultado de
        toTimeZone(..., 'America/Sao_Paulo')::DATE em cima de um campo
        *LastUpdateTime, truncado pra dia — não tem componente de hora).

        campo_filtro_origem: campo usado para filtrar os 3 cards de
        validação na origem — default 'data_referencia' (a data que o
        card representa), e não 'updated_at' (quando o registro foi
        tocado por último), porque aqui queremos os DIAS do período, não
        os registros modificados no período.

        tipo_campo_origem: base-type passado ao filtro do Metabase —
        default 'type/Date' (não 'type/DateTime', que é o padrão de
        extrai_dados_card) exatamente porque 'data_referencia' é DATE.
        Se algum dia campo_filtro_origem for trocado para um campo
        datetime (ex.: 'updated_at'), ajuste também este parâmetro para
        'type/DateTime'.
        """
        start_time = datetime.now()
        try:
            self.logger.info(
                f"Iniciando validação por período ({data_inicio} a {data_fim}) para {cliente}"
            )
            database = MetabaseDatabase.ClickhousePartnerZeroum.value if cliente == 'ZEROUM' else MetabaseDatabase.ClickhousePartnerEnergiabet.value
            auth_id = self.conection(cliente)

            card = MetabaseCard.ZeroUm_Validacao_ApostasDia.value if cliente == 'ZEROUM' else MetabaseCard.EnergiaBet_Validacao_ApostasDia.value
            df_apostas_dia_origem = self.extrai_dados_card(
                auth_id, database, card, data_inicio, data_fim,
                campo_filtro=campo_filtro_origem, tipo_campo=tipo_campo_origem
            )

            card = MetabaseCard.ZeroUm_Validacao_DepositoSaque.value if cliente == 'ZEROUM' else MetabaseCard.EnergiaBet_Validacao_DepositoSaque.value
            df_deposito_saque_origem = self.extrai_dados_card(
                auth_id, database, card, data_inicio, data_fim,
                campo_filtro=campo_filtro_origem, tipo_campo=tipo_campo_origem
            )

            card = MetabaseCard.ZeroUm_Validacao_RegistroUsuario.value if cliente == 'ZEROUM' else MetabaseCard.EnergiaBet_Validacao_RegistroUsuario.value
            df_usuario_origem = self.extrai_dados_card(
                auth_id, database, card, data_inicio, data_fim,
                campo_filtro=campo_filtro_origem, tipo_campo=tipo_campo_origem
            )

            # 'outer' (não 'left') para não perder dias que tenham
            # depósito/saque ou cadastro, mas nenhuma aposta no período.
            df_validacao = df_apostas_dia_origem.merge(
                df_deposito_saque_origem[['data_referencia', 'deposit_amount_origem', 'deposit_qtd_origem',
                                           'withdraw_amount_origem', 'withdraw_qtd_origem']],
                on=['data_referencia'], how='outer'
            )
            df_validacao = df_validacao.merge(
                df_usuario_origem[['data_referencia', 'users_registered_origem']],
                on=['data_referencia'], how='outer'
            )

            script = f"""
                        with
                        deposits_withdraw_summarized as (
                            select 
                                f.date,
                                sum(case when f.tipo = 'deposit' then f.amount else 0 end)  as deposit_amount_destino_summarized,
                                sum(case when f.tipo = 'deposit' then f.qtd else 0 end)  as deposit_qtd_destino_summarized,
                                sum(case when f.tipo = 'withdraw' then f.amount else 0 end)  as withdraw_amount_destino_summarized,
                                sum(case when f.tipo = 'withdraw' then f.qtd else 0 end)  as withdraw_qtd_destino_summarized
                            from inplay.fact_deposits_withdraws_summarized f
                            where f.date::date BETWEEN '{data_inicio}'::date AND '{data_fim}'::date
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
                            where f.data_referencia::date BETWEEN '{data_inicio}'::date AND '{data_fim}'::date
                            group by f.data_referencia
                        ),
                        casino_games as (
                            select 
                                f.reference::date,
                                sum(f.bet_amount) as bet_amount_destino_casino_games,
                                sum(f.bet_qty) as bet_qtd_destino_casino_games
                            from inplay.fact_casino_games_hourly f
                            where f.reference::date BETWEEN '{data_inicio}'::date AND '{data_fim}'::date
                            group by reference::date 
                        ),
                        users_registration as (
                            select 
                                d.registration_date::date,
                                count(1) as users_registered_destino_dim_usuario
                            from inplay.dim_usuario d
                            where d.registration_date::date BETWEEN '{data_inicio}'::date AND '{data_fim}'::date
                            group by d.registration_date::date
                        )
                        select
                            coalesce(s.date, ud.data_referencia, cg.reference, ur.registration_date) as data_referencia,
                            s.deposit_amount_destino_summarized,
                            ud.deposit_amount_destino_user_daily,
                            s.deposit_qtd_destino_summarized,
                            ud.deposit_qtd_destino_user_daily,
                            s.withdraw_amount_destino_summarized,
                            ud.withdraw_amount_destino_user_daily,
                            s.withdraw_qtd_destino_summarized,
                            ud.withdraw_qtd_destino_user_daily,
                            cg.bet_amount_destino_casino_games,
                            ud.bet_amount_destino_user_daily,
                            cg.bet_qtd_destino_casino_games,
                            ud.bet_qtd_destino_user_daily,
                            ur.users_registered_destino_dim_usuario,
                            timezone('America/Sao_Paulo', current_timestamp) as import_date
                        from deposits_withdraw_summarized s
                            full outer join user_daily ud on s.date = ud.data_referencia
                            full outer join casino_games cg on coalesce(s.date, ud.data_referencia) = cg.reference
                            full outer join users_registration ur on coalesce(s.date, ud.data_referencia, cg.reference) = ur.registration_date
                        order by 1 desc;
                    """

            tabela_validacao = 'inplay.verificacao_zero_um' if cliente == 'ZEROUM' else 'inplay.verificacao_energia_bet'
            ConnectionDB.conecta(DB, cliente)
            df_dados_destino = ConnectionDB.executa_script(script, self.logger)

            df_validacao['data_referencia'] = pd.to_datetime(df_validacao['data_referencia'])
            df_dados_destino['data_referencia'] = pd.to_datetime(df_dados_destino['data_referencia'])

            df_validacao = df_validacao.merge(
                df_dados_destino[['data_referencia', 'deposit_amount_destino_summarized', 'deposit_amount_destino_user_daily',
                                   'deposit_qtd_destino_summarized', 'deposit_qtd_destino_user_daily',
                                   'withdraw_amount_destino_summarized', 'withdraw_amount_destino_user_daily',
                                   'withdraw_qtd_destino_summarized', 'withdraw_qtd_destino_user_daily',
                                   'bet_amount_destino_casino_games', 'bet_amount_destino_user_daily',
                                   'bet_qtd_destino_casino_games', 'bet_qtd_destino_user_daily',
                                   'users_registered_destino_dim_usuario', 'import_date']],
                on=['data_referencia'], how='outer'
            )

            df_validacao = df_validacao.rename(columns={'data_referencia': 'data'})
            df_validacao = df_validacao.replace({np.nan: None})

            df_validacao['deposit_validacao'] = np.where(
                ((df_validacao['deposit_amount_origem'] != df_validacao['deposit_amount_destino_summarized']) |
                 (df_validacao['deposit_amount_origem'] != df_validacao['deposit_amount_destino_user_daily']) |
                 (df_validacao['deposit_qtd_origem'] != df_validacao['deposit_qtd_destino_summarized']) |
                 (df_validacao['deposit_qtd_origem'] != df_validacao['deposit_qtd_destino_user_daily'])),
                'errado', 'certo'
            )
            df_validacao['withdraw_validacao'] = np.where(
                ((df_validacao['withdraw_amount_origem'] != df_validacao['withdraw_amount_destino_summarized']) |
                 (df_validacao['withdraw_amount_origem'] != df_validacao['withdraw_amount_destino_user_daily']) |
                 (df_validacao['withdraw_qtd_origem'] != df_validacao['withdraw_qtd_destino_summarized']) |
                 (df_validacao['withdraw_qtd_origem'] != df_validacao['withdraw_qtd_destino_user_daily'])),
                'errado', 'certo'
            )
            df_validacao['bet_validacao'] = np.where(
                ((df_validacao['bet_amount_origem'] != df_validacao['bet_amount_destino_casino_games']) |
                 (df_validacao['bet_amount_origem'] != df_validacao['bet_amount_destino_user_daily']) |
                 (df_validacao['bet_qtd_origem'] != df_validacao['bet_qtd_destino_casino_games']) |
                 (df_validacao['bet_qtd_origem'] != df_validacao['bet_qtd_destino_user_daily'])),
                'errado', 'certo'
            )
            df_validacao['users_registered_validacao'] = np.where(
                (df_validacao['users_registered_origem'] != df_validacao['users_registered_destino_dim_usuario']),
                'errado', 'certo'
            )

            # Apaga e recarrega SÓ o recorte de datas pedido — preserva o
            # restante da tabela (valida_dados() apaga tudo, esta não).
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.deleta_dados(
                tabela_validacao,
                f"WHERE data BETWEEN '{data_inicio}' AND '{data_fim}'",
                self.logger
            )
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.insere_dados_bulk(tabela_validacao, df_validacao, self.logger)

            self.db_logger.log_operation(
                operation=f'VALIDACAO_{cliente}_PERIODO',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente=cliente
            )
            return df_validacao

        except Exception as e:
            self.logger.error(
                f"Erro na validação por período para {cliente} ({data_inicio} a {data_fim}): {e}"
            )
            self.db_logger.log_operation(
                operation=f'VALIDACAO_{cliente}_PERIODO',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente=cliente
            )
            raise

    def reprocessa_dias_pontuais(self, cliente: str, dias: list):
        """
        Reprocessa pontualmente inplay.fact_casino_games_hourly e
        inplay.fact_deposits_withdraws_summarized para uma lista de dias
        específicos — sem rodar principal_zeroum()/principal_energiabet()
        inteiro de novo.

        Criada pro incidente da parada de 8+ dias (ver conversa):
        valida_dados_periodo() apontou que só alguns dias pontuais
        ficaram com dado incompleto nessas duas tabelas especificamente
        (ex.: 2026-09-05, 2026-09-06, 2026-09-14) — as demais
        tabelas/dias do período já estavam corretos, então não faz
        sentido reprocessar tudo de novo.

        dias: lista de strings 'YYYY-MM-DD', ex.:
              ['2026-09-05', '2026-09-06', '2026-09-14']

        Estratégia importante: filtra a origem DIRETO pelo campo de data
        do próprio registro ('reference' / 'date'), e não por
        'updated_at' como o pipeline principal faz. Isso é proposital —
        o dado que falta é antigo, o updated_at dele já passou e não
        reaparece num filtro incremental por updated_at. Só filtrando
        pela data do evento em si é que a origem devolve essas linhas de
        novo. O merge no destino é um upsert (por chave), então rodar
        esta função mais de uma vez para o mesmo dia é seguro — não
        duplica nada.
        """
        start_time = datetime.now()
        try:
            database = MetabaseDatabase.ClickhousePartnerZeroum.value if cliente == 'ZEROUM' else MetabaseDatabase.ClickhousePartnerEnergiabet.value
            card_apostas_jogos_hora = MetabaseCard.ZeroUm_ApostasJogosHora.value if cliente == 'ZEROUM' else MetabaseCard.EnergiaBet_ApostasJogosHora.value
            card_dep_saq_dias = MetabaseCard.ZeroUm_DepositoSaqueDias.value if cliente == 'ZEROUM' else MetabaseCard.EnergiaBet_DepositoSaqueDias.value
            auth_id = self.conection(cliente)

            for dia in dias:
                dia_dt = datetime.strptime(dia, '%Y-%m-%d')
                dia_seguinte = (dia_dt + timedelta(days=1)).strftime('%Y-%m-%d')

                self.logger.info(f"[reprocessa_dias_pontuais] {cliente} — reprocessando {dia}")

                # --- fact_casino_games_hourly ---
                # janela [dia 00:00, dia+1 00:00) filtrando por 'reference'.
                # ATENÇÃO: no card ZeroUm_ApostasJogosHora (14785), 'reference'
                # é gerado com formatDateTime(...) no ClickHouse — ou seja, é
                # TEXTO ('YYYY-MM-DD HH:MM:SS'), não um DateTime nativo (isso é
                # diferente de 'updated_at', que É um DateTime de verdade — por
                # isso o pipeline principal, que filtra por updated_at, nunca
                # esbarrou nisso). Por ser texto, o filtro tem que usar
                # tipo_campo="type/Text" e o valor tem que estar EXATAMENTE no
                # mesmo formato da coluna (espaço, não "T", entre data e hora —
                # comparação de string é sensível a isso).
                data_inicio_ref = f"{dia} 00:00:00"
                data_fim_ref = f"{dia_seguinte} 00:00:00"
                df_cassino = self.extrai_dados_card(
                    auth_id, database, card_apostas_jogos_hora,
                    data_inicio_ref, data_fim_ref,
                    campo_filtro="reference", tipo_campo="type/Text"
                )
                colunas_esperadas_cassino = ['reference', 'game_id', 'with_bonus', 'bet_amount', 'win_amount',
                                              'bet_qty', 'win_qty', 'ggr_amount', 'updated_at', 'data_importacao']
                faltando = [c for c in colunas_esperadas_cassino if c not in df_cassino.columns]
                if faltando:
                    raise Exception(
                        f"[reprocessa_dias_pontuais] card ApostasJogosHora ({dia}) não trouxe as colunas "
                        f"esperadas {faltando} — provável erro do Metabase/ClickHouse na extração "
                        f"(ver log de [extrai_csv]/[extrai_dados_card] logo acima). "
                        f"Colunas recebidas: {list(df_cassino.columns)[:10]}..."
                    )
                df_cassino_ajust = df_cassino[colunas_esperadas_cassino]
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.deleta_dados('inplay.stg_fact_casino_games_hourly', "", self.logger)
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.insere_dados_bulk('inplay.stg_fact_casino_games_hourly', df_cassino_ajust, self.logger)
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.mergeia_dados(
                    'inplay.stg_fact_casino_games_hourly', 'inplay.fact_casino_games_hourly',
                    df_cassino_ajust, ['reference', 'game_id'], self.logger
                )

                # --- fact_deposits_withdraws_summarized ---
                # 'date' é campo DATE nativo (::DATE no ClickHouse), diferente
                # de 'reference' acima — type/Date continua correto aqui.
                df_dep_saq = self.extrai_dados_card(
                    auth_id, database, card_dep_saq_dias,
                    dia, dia,
                    campo_filtro="date", tipo_campo="type/Date"
                )
                colunas_esperadas_dep_saq = ['date', 'hora', 'tipo', 'qtd', 'amount', 'updated_at', 'import_date']
                faltando = [c for c in colunas_esperadas_dep_saq if c not in df_dep_saq.columns]
                if faltando:
                    raise Exception(
                        f"[reprocessa_dias_pontuais] card DepositoSaqueDias ({dia}) não trouxe as colunas "
                        f"esperadas {faltando} — provável erro do Metabase/ClickHouse na extração "
                        f"(ver log de [extrai_csv]/[extrai_dados_card] logo acima). "
                        f"Colunas recebidas: {list(df_dep_saq.columns)[:10]}..."
                    )
                df_dep_saq_ajust = df_dep_saq[colunas_esperadas_dep_saq]
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.deleta_dados('inplay.stg_fact_deposits_withdraws_summarized', "", self.logger)
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.insere_dados_bulk('inplay.stg_fact_deposits_withdraws_summarized', df_dep_saq_ajust, self.logger)
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.mergeia_dados(
                    'inplay.stg_fact_deposits_withdraws_summarized', 'inplay.fact_deposits_withdraws_summarized',
                    df_dep_saq_ajust, ['date', 'hora', 'tipo'], self.logger
                )

                self.logger.info(f"[reprocessa_dias_pontuais] {cliente} — {dia} concluído")

            self.db_logger.log_operation(
                operation=f'REPROCESSO_PONTUAL_{cliente}',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente=cliente
            )

        except Exception as e:
            self.logger.error(f"Erro no reprocessamento pontual para {cliente} (dias={dias}): {e}")
            self.db_logger.log_operation(
                operation=f'REPROCESSO_PONTUAL_{cliente}',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente=cliente
            )
            raise

    def reprocessa_fact_user_daily_dias(self, cliente: str, dias: list, fontes: list = None):
        """
        Reprocessa pontualmente inplay.fact_user_daily para uma lista de
        dias específicos, refazendo os MERGEs de 'Stage' (aposta de
        cassino) e/ou 'Saque' a partir dos cards do Metabase.

        Criada pro mesmo incidente da parada de 8+ dias (ver conversa):
        05/09 e 06/09 ficaram com bet_amount/bet_qty e withdraw_amount/
        withdraw_quantity inflados em fact_user_daily especificamente —
        confirmado que NÃO é duplicata de CDC ainda presente na origem
        (checado direto na Bet/PaymentRequest, a diferença é irrisória
        perto do excesso observado) — é defasagem: o dado foi carregado
        durante o catch-up caótico de 14/09 e nunca mais atualizado, e o
        valor de origem hoje já está correto. Por isso NÃO precisa de
        dedup nenhum aqui — só re-extrair e re-mergear com o card normal.

        dias: lista de strings 'YYYY-MM-DD'.
        fontes: subconjunto de ['stage', 'saque'] — default as duas.
                Ex.: fontes=['stage'] reprocessa só a aposta.

        Diferente de reprocessa_dias_pontuais (que filtra por
        'reference'/'date', campos de grão fino), aqui o campo de
        filtro é 'data_referencia' — já é DATE nativo nos dois cards
        (::DATE no ClickHouse), então usa tipo_campo="type/Date" sem
        os cuidados de formato string que tivemos com 'reference' em
        ApostasJogosHora.
        """
        if fontes is None:
            fontes = ['stage', 'saque']

        start_time = datetime.now()
        try:
            database = MetabaseDatabase.ClickhousePartnerZeroum.value if cliente == 'ZEROUM' else MetabaseDatabase.ClickhousePartnerEnergiabet.value
            card_stage = MetabaseCard.ZeroUm_Stage.value if cliente == 'ZEROUM' else MetabaseCard.EnergiaBet_Stage.value
            card_saque = MetabaseCard.ZeroUm_Saque.value if cliente == 'ZEROUM' else MetabaseCard.EnergiaBet_Saque.value
            auth_id = self.conection(cliente)

            for dia in dias:
                self.logger.info(f"[reprocessa_fact_user_daily_dias] {cliente} — reprocessando {dia} ({fontes})")

                if 'stage' in fontes:
                    df_stg = self.extrai_dados_card(
                        auth_id, database, card_stage, dia, dia,
                        campo_filtro="data_referencia", tipo_campo="type/Date"
                    )
                    faltando = [c for c in ('usuario', 'data_referencia') if c not in df_stg.columns]
                    if faltando:
                        raise Exception(
                            f"[reprocessa_fact_user_daily_dias] card Stage ({dia}) não trouxe as colunas "
                            f"mínimas {faltando} — provável erro do Metabase/ClickHouse na extração. "
                            f"Colunas recebidas: {list(df_stg.columns)[:10]}..."
                        )
                    df_stg = df_stg.replace({np.nan: None})
                    ConnectionDB.conecta(DB, cliente)
                    ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
                    ConnectionDB.conecta(DB, cliente)
                    ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_stg, self.logger)
                    ConnectionDB.conecta(DB, cliente)
                    ConnectionDB.mergeia_dados(
                        'inplay.stg_fact_user_daily', 'inplay.fact_user_daily',
                        df_stg, ['usuario', 'data_referencia'], self.logger
                    )

                if 'saque' in fontes:
                    df_saque = self.extrai_dados_card(
                        auth_id, database, card_saque, dia, dia,
                        campo_filtro="data_referencia", tipo_campo="type/Date"
                    )
                    colunas_esperadas_saque = ['usuario', 'data_referencia', 'withdraw_amount', 'withdraw_quantity',
                                                'withdraw_pending_amount', 'withdraw_pending_quantity',
                                                'withdraw_denied_amount', 'withdraw_denied_quantity']
                    faltando = [c for c in colunas_esperadas_saque if c not in df_saque.columns]
                    if faltando:
                        raise Exception(
                            f"[reprocessa_fact_user_daily_dias] card Saque ({dia}) não trouxe as colunas "
                            f"esperadas {faltando} — provável erro do Metabase/ClickHouse na extração. "
                            f"Colunas recebidas: {list(df_saque.columns)[:10]}..."
                        )
                    df_saque = df_saque[colunas_esperadas_saque].replace({np.nan: None})
                    ConnectionDB.conecta(DB, cliente)
                    ConnectionDB.deleta_dados('inplay.stg_fact_user_daily', "", self.logger)
                    ConnectionDB.conecta(DB, cliente)
                    ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily', df_saque, self.logger)
                    ConnectionDB.conecta(DB, cliente)
                    ConnectionDB.mergeia_dados(
                        'inplay.stg_fact_user_daily', 'inplay.fact_user_daily',
                        df_saque, ['usuario', 'data_referencia'], self.logger
                    )

                self.logger.info(f"[reprocessa_fact_user_daily_dias] {cliente} — {dia} concluído")

            self.db_logger.log_operation(
                operation=f'REPROCESSO_FACT_USER_DAILY_{cliente}',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente=cliente
            )

        except Exception as e:
            self.logger.error(f"Erro no reprocessamento de fact_user_daily para {cliente} (dias={dias}): {e}")
            self.db_logger.log_operation(
                operation=f'REPROCESSO_FACT_USER_DAILY_{cliente}',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente=cliente
            )
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
                    salva_csv_conferencia(divergentes, nome)
                    self.logger.warning(
                        f"{tipo} aposta: {len(divergentes)} divergentes "
                        f"({divergentes_timing_skew} explicáveis por timing skew, "
                        f"{divergentes_suspeitos} suspeitos)"
                        + (f" -> {nome}" if EXPORTA_CSV_CONFERENCIA else "")
                    )

                if not so_card.empty:
                    nome = f"so_no_card_{tipo}_aposta_{cliente}_{timestamp}.csv"
                    salva_csv_conferencia(so_card, nome)

                if not so_nativo.empty:
                    nome = f"so_no_nativo_{tipo}_aposta_{cliente}_{timestamp}.csv"
                    salva_csv_conferencia(so_nativo, nome)

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

            card_usuarios = (METABASE_CARD_USUARIOS_ZEROUM if cliente == 'ZEROUM'
                              else METABASE_CARD_USUARIOS_ENERGIABET)
            card_totalizador = (MetabaseCard.ZeroUm_UsuariosTotalizador.value if cliente == 'ZEROUM'
                                 else MetabaseCard.EnergiaBet_UsuariosTotalizador.value)

            auth_id = self.conection(cliente)

            df_usuario = self.extrai_dados_card(auth_id, database, card_usuarios, data_inicial_str, 0)
            # LEITURA B (decisão de produto): first_name/birth_date/mobile_number
            # NÃO são mais selecionados nem gravados. Só as colunas
            # "_protegido" (cifradas, já vêm prontas do card) são carregadas.
            df_usuario_ajust = df_usuario[['id','core_account_status','core_user_language','core_wallet_currency','email_verified','email',
                                        'mobile_number_verified','refer_id','sms_allowed','email_allowed','city','state','updated_at','registration_date','import_date',
                                        'first_deposit_date','first_deposit_amount','first_withdraw_date','first_withdraw_amount','last_deposit_date','last_deposit_amount',
                                        'last_withdraw_date','last_withdraw_amount', 'utm', 'status_usuario', 'LastName', 'TaxNumber', 'DocumentType', 'DocumentNumber',
                                         'DocumentIssuedBy', 'IsDocumentVerified', 'KYCStatus', 'KYCDocsStatus',
                                         'first_name_protegido', 'mobile_number_protegido', 'birth_date_protegido']]


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

            card_usuarios = (METABASE_CARD_USUARIOS_ZEROUM if cliente == 'ZEROUM'
                              else METABASE_CARD_USUARIOS_ENERGIABET)
            card_totalizador = (MetabaseCard.ZeroUm_UsuariosTotalizador.value if cliente == 'ZEROUM'
                                 else MetabaseCard.EnergiaBet_UsuariosTotalizador.value)

            auth_id = self.conection(cliente)

            df_usuario = self.extrai_dados_card_por_ids(auth_id, database, card_usuarios, ids)
            # LEITURA B (decisão de produto): first_name/birth_date/mobile_number
            # NÃO são mais selecionados nem gravados. Só as colunas
            # "_protegido" (cifradas, já vêm prontas do card) são carregadas.
            df_usuario_ajust = df_usuario[['id','core_account_status','core_user_language','core_wallet_currency','email_verified','email',
                                        'mobile_number_verified','refer_id','sms_allowed','email_allowed','city','state','updated_at','registration_date','import_date',
                                        'first_deposit_date','first_deposit_amount','first_withdraw_date','first_withdraw_amount','last_deposit_date','last_deposit_amount',
                                        'last_withdraw_date','last_withdraw_amount', 'utm', 'status_usuario', 'LastName', 'TaxNumber', 'DocumentType', 'DocumentNumber',
                                         'DocumentIssuedBy', 'IsDocumentVerified', 'KYCStatus', 'KYCDocsStatus',
                                         'first_name_protegido', 'mobile_number_protegido', 'birth_date_protegido']]


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

    def _extrai_usuario_em_janelas(self, auth_id, id_database, id_card, cliente,
                                    data_inicial_dt, data_final_dt, colunas_origem,
                                    contador, janela_minima=timedelta(hours=1)):
        """
        Extrai o card de usuários (Frente A + Frente B) em janelas de tempo
        por registration_date, gravando cada janela IMEDIATAMENTE em
        inplay.stg_usuario_backfill (nunca acumula o histórico inteiro em
        memória — importante numa base de 9M+ usuários).

        Por que não reaproveitar extrai_dados_card_por_periodo: esse método
        já existe na classe, mas é hoje específico do fluxo de Saldo Diário
        (monta SQL nativo via _sql_saldo_diario contra ClientDailyBalance).
        Este método replica a MESMA estratégia geral (subdivide a janela ao
        meio sempre que bater no teto do Metabase ou falhar por
        timeout/conexão — self.LIMITE_LINHAS_METABASE, já existente na
        classe), mas usando extrai_dados_card contra o card de usuários
        (card de teste ou produção, conforme id_card recebido).

        Erros HTTP 4xx (SQL/card malformado) sobem direto, sem subdividir —
        subdividir a janela não conserta uma consulta com erro de sintaxe,
        mesma lógica já usada em extrai_dados_card_por_periodo.
        """
        ini_str = data_inicial_dt.strftime('%Y-%m-%dT%H:%M:%S')
        fim_str = data_final_dt.strftime('%Y-%m-%dT%H:%M:%S')
        duracao = data_final_dt - data_inicial_dt

        try:
            df = self.extrai_dados_card(
                auth_id, id_database, id_card, ini_str, fim_str,
                campo_filtro="registration_date", timeout=2400,
            )
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status is None or status < 500:
                raise
            if duracao <= janela_minima:
                self.logger.error(f"[BACKFILL] Falha 5xx na janela mínima {ini_str}-{fim_str}: {self._descricao_erro(e)}")
                raise
            self.logger.warning(f"[BACKFILL] Falha 5xx na janela {ini_str}-{fim_str}, dividindo ao meio: {self._descricao_erro(e)}")
            meio = data_inicial_dt + duracao / 2
            self._extrai_usuario_em_janelas(auth_id, id_database, id_card, cliente, data_inicial_dt, meio, colunas_origem, contador, janela_minima)
            self._extrai_usuario_em_janelas(auth_id, id_database, id_card, cliente, meio, data_final_dt, colunas_origem, contador, janela_minima)
            return
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if duracao <= janela_minima:
                self.logger.error(f"[BACKFILL] Falha de conexão na janela mínima {ini_str}-{fim_str}: {self._descricao_erro(e)}")
                raise
            self.logger.warning(f"[BACKFILL] Falha de conexão na janela {ini_str}-{fim_str}, dividindo ao meio: {self._descricao_erro(e)}")
            meio = data_inicial_dt + duracao / 2
            self._extrai_usuario_em_janelas(auth_id, id_database, id_card, cliente, data_inicial_dt, meio, colunas_origem, contador, janela_minima)
            self._extrai_usuario_em_janelas(auth_id, id_database, id_card, cliente, meio, data_final_dt, colunas_origem, contador, janela_minima)
            return

        if len(df) >= self.LIMITE_LINHAS_METABASE:
            if duracao <= janela_minima:
                self.logger.warning(
                    f"[BACKFILL] Janela mínima atingida ({ini_str}-{fim_str}) e ainda assim "
                    f"{len(df)} linhas -- possível truncamento residual. Revisar manualmente."
                )
            else:
                self.logger.warning(
                    f"[BACKFILL] Janela {ini_str}-{fim_str} atingiu o teto do Metabase "
                    f"({len(df)} linhas). Dividindo ao meio."
                )
                meio = data_inicial_dt + duracao / 2
                self._extrai_usuario_em_janelas(auth_id, id_database, id_card, cliente, data_inicial_dt, meio, colunas_origem, contador, janela_minima)
                self._extrai_usuario_em_janelas(auth_id, id_database, id_card, cliente, meio, data_final_dt, colunas_origem, contador, janela_minima)
                return

        if len(df) == 0:
            self.logger.info(f"[BACKFILL] Janela {ini_str}-{fim_str}: 0 linhas, pulando.")
            return

        df_ajust = df[colunas_origem].rename(columns={
            'first_name': 'first_name_protegido',
            'birth_date': 'birth_date_protegido',
            'mobile_number': 'mobile_number_protegido',
        })
        df_ajust = df_ajust.replace({np.nan: None})

        # Retry com reconexão: numa execução de horas (9M+ usuários, muitas
        # janelas), uma queda pontual de conexão com o Redshift (rede, VPN,
        # timeout de sessão) é esperada em algum momento -- sem isso, uma
        # única falha transitória em qualquer janela derruba a execução
        # inteira e perde todo o progresso já feito (não há checkpoint; a
        # stg só é truncada uma vez, no início, mas se o processo morrer
        # tudo precisa ser refeito do zero).
        max_tentativas = 3
        for tentativa in range(1, max_tentativas + 1):
            try:
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.insere_dados_bulk('inplay.stg_usuario_backfill', df_ajust, self.logger)
                break
            except Exception as e:
                if tentativa == max_tentativas:
                    self.logger.error(
                        f"[BACKFILL] Falha ao inserir janela {ini_str}-{fim_str} após "
                        f"{max_tentativas} tentativas: {e}"
                    )
                    raise
                espera = 5 * tentativa
                self.logger.warning(
                    f"[BACKFILL] Falha ao inserir janela {ini_str}-{fim_str} "
                    f"(tentativa {tentativa}/{max_tentativas}): {e}. "
                    f"Reconectando e tentando de novo em {espera}s."
                )
                time.sleep(espera)

        contador['total'] += len(df_ajust)
        contador['janelas'] += 1
        self.logger.info(
            f"[BACKFILL] Janela {ini_str}-{fim_str}: {len(df_ajust)} linhas inseridas "
            f"(acumulado: {contador['total']} linhas em {contador['janelas']} janelas)"
        )

    def backfill_historico_protecao_dados_pessoais(self, cliente: str, data_inicio_str: str, data_fim_str: str):
        """
        Carga histórica ÚNICA da melhoria de proteção de dados pessoais em
        dim_usuario (ver plano) -- une numa só passada:

          Frente A (campos novos, sem par em claro): lastname, taxnumber,
          documenttype, documentnumber, documentissuedby, isdocumentverified,
          kycstatus, kycdocsstatus.

          Frente B (campos já existentes, NÃO TOCA no original): grava o
          valor cifrado em first_name_protegido/mobile_number_protegido/
          birth_date_protegido -- first_name/birth_date/mobile_number
          continuam intocados, porque a API do cliente consome esses 3
          campos hoje (ver plano, Corte de Produção é etapa separada).

        Usa METABASE_CARD_USUARIOS_ZEROUM/ENERGIABET (config.py, lido do
        .env) como card -- em validação, apontar essas variáveis para o
        card de teste (card__21517/card__21518); em produção, deixar sem
        definir no .env (cai no fallback = card de produção real).

        Extrai em janelas de tempo (self._extrai_usuario_em_janelas) para
        não estourar o teto de exportação do Metabase (9M+ usuários na base
        - uma extração única seria truncada silenciosamente). Cada janela é
        gravada direto em inplay.stg_usuario_backfill; ao final, um único
        UPDATE aplica as 11 colunas de uma vez em dim_usuario.

        Não aplica decrypt em nenhum momento.

        data_inicio_str/data_fim_str: formato 'YYYY-MM-DDTHH:MM:SS' (mesmo
        padrão de --data-inicial-backfill já usado por
        ZEROUM_BACKFILL_USUARIO), ex.: '2015-01-01T00:00:00'.
        """
        start_time = datetime.now()
        try:
            self.logger.info(
                f"[BACKFILL] Iniciando carga histórica unificada (proteção de dados "
                f"pessoais) para {cliente}, {data_inicio_str} a {data_fim_str}"
            )

            database = (MetabaseDatabase.ClickhousePartnerZeroum.value if cliente == 'ZEROUM'
                        else MetabaseDatabase.ClickhousePartnerEnergiabet.value)
            id_card = (METABASE_CARD_USUARIOS_ZEROUM if cliente == 'ZEROUM'
                       else METABASE_CARD_USUARIOS_ENERGIABET)

            self.logger.info(f"[BACKFILL] Card em uso: {id_card}")

            auth_id = self.conection(cliente)

            colunas_origem = ['id',
                               'LastName', 'TaxNumber', 'DocumentType', 'DocumentNumber',
                               'DocumentIssuedBy', 'IsDocumentVerified', 'KYCStatus', 'KYCDocsStatus',
                               'first_name', 'birth_date', 'mobile_number']

            # Trunca a stg SÓ no início da execução inteira -- cada janela
            # depois disso é um INSERT (append), nunca um TRUNCATE por janela.
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.deleta_dados('inplay.stg_usuario_backfill', '', self.logger)

            data_inicio_dt = datetime.strptime(data_inicio_str, '%Y-%m-%dT%H:%M:%S')
            data_fim_dt = datetime.strptime(data_fim_str, '%Y-%m-%dT%H:%M:%S')

            contador = {'total': 0, 'janelas': 0}
            self._extrai_usuario_em_janelas(
                auth_id, database, id_card, cliente,
                data_inicio_dt, data_fim_dt, colunas_origem, contador
            )

            self.logger.info(
                f"[BACKFILL] Extração concluída: {contador['total']} linhas em "
                f"{contador['janelas']} janelas gravadas em stg_usuario_backfill"
            )

            # Diagnóstico: a subdivisão recursiva de janelas usa um filtro
            # "between" (inclusivo nas duas pontas) -- um usuário cujo
            # registration_date caia EXATAMENTE no instante de corte entre
            # duas janelas pode ser extraído duas vezes (uma em cada janela),
            # gerando id duplicado em stg_usuario_backfill. Os valores das
            # duas linhas são idênticos (mesma origem), então não corrompe o
            # resultado -- mas duplicar o id quebraria o UPDATE (Redshift não
            # aceita mais de uma linha da tabela de origem casando com a
            # mesma linha de destino num UPDATE...FROM). Log aqui só para
            # visibilidade -- a proteção de verdade está no sql_update abaixo.
            ConnectionDB.conecta(DB, cliente)
            resultado_dedup = ConnectionDB.recupera_dados(
                'inplay.stg_usuario_backfill',
                'count(*) as total, count(distinct id) as distintos', ''
            )
            if resultado_dedup:
                total_linhas, ids_distintos = resultado_dedup[0]
                if total_linhas != ids_distintos:
                    self.logger.warning(
                        f"[BACKFILL] stg_usuario_backfill tem {total_linhas} linhas mas só "
                        f"{ids_distintos} ids distintos -- {total_linhas - ids_distintos} "
                        "duplicatas detectadas (esperado, ver comentário no código). "
                        "O UPDATE abaixo já deduplica por id antes de aplicar."
                    )

            # UPDATE protegido contra duplicidade: junta com uma versão
            # deduplicada da stg (ROW_NUMBER() OVER PARTITION BY id = 1) em
            # vez de ir direto contra a tabela -- garante no máximo uma linha
            # de origem por id, mesmo que a extração em janelas tenha gravado
            # o mesmo usuário mais de uma vez.
            sql_update = """
                UPDATE inplay.dim_usuario d
                SET
                    lastname                = s."LastName",
                    taxnumber                = s."TaxNumber",
                    documenttype             = s."DocumentType",
                    documentnumber           = s."DocumentNumber",
                    documentissuedby         = s."DocumentIssuedBy",
                    isdocumentverified       = s."IsDocumentVerified",
                    kycstatus                = s."KYCStatus",
                    kycdocsstatus            = s."KYCDocsStatus",
                    first_name_protegido     = s.first_name_protegido,
                    mobile_number_protegido  = s.mobile_number_protegido,
                    birth_date_protegido     = s.birth_date_protegido
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn_dedup
                    FROM inplay.stg_usuario_backfill
                ) s
                WHERE d.id = s.id
                  AND s.rn_dedup = 1
            """
            self.logger.info("[BACKFILL] Aplicando UPDATE único em dim_usuario (11 colunas, deduplicado por id)...")
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.executa_dml(sql_update, self.logger)
            self.logger.info(
                "[BACKFILL] UPDATE concluído. LEMBRETE: rodar VACUUM e ANALYZE em "
                "inplay.dim_usuario em seguida (fora de transação, janela de baixo "
                "tráfego)."
            )

            self.db_logger.log_operation(
                operation=f'BACKFILL_HISTORICO_PROTECAO_{cliente}',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente=cliente
            )

        except Exception as e:
            self.logger.error(f"Erro no backfill histórico de proteção de dados pessoais ({cliente}): {e}")
            self.db_logger.log_operation(
                operation=f'BACKFILL_HISTORICO_PROTECAO_{cliente}',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente=cliente
            )
            b = (
                f"Erro no backfill histórico de proteção de dados pessoais ({cliente}) "
                f"de {data_inicio_str} a {data_fim_str}: {e}\n\n"
                f"=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            )
            send_email(subject=f"[FALHA ENGENHARIA] {cliente} - Erro no backfill histórico de proteção de dados pessoais", body=b)
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
        descricao = f"{type(e).__name__}: {texto}"

        # CORREÇÃO DESTA REVISÃO: anexa o corpo da resposta HTTP (quando
        # existir) à descrição que vai pro e-mail de alerta e pro log em
        # inplay.etl_execution_logs — antes o e-mail de "[FALHA
        # ENGENHARIA]" só trazia "HTTPError: 500 Server Error:
        # Internal Server Error for url: ...", sem a mensagem real do
        # Metabase, obrigando a reproduzir o erro manualmente pra
        # descobrir a causa.
        response = getattr(e, "response", None)
        if response is not None:
            try:
                corpo = response.text.strip()[:2000]
                if corpo:
                    descricao += f" | corpo da resposta: {corpo}"
            except Exception:
                pass
        return descricao
 
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
 
    def processa_agregacao_pix(self, cliente, modo="incremental", partner_id=None, data_final=None):
        """
        Carga de inplay.agg_pix_cliente — agregação de PaymentRequest por
        ClientId/PixKey/PixType (qtd de PaymentRequest Type=1, Status IN
        (7,8,12), PixKey preenchido).

        Roda 1x/dia, como cliente próprio (ZEROUM_PIX / ENERGIABET_PIX),
        em cron separado — não integra o pipeline horário existente nem
        os crons de Saldo Diário/Bônus.

        DECISÃO full x incremental: a origem filtrada (PartnerId=180,
        Type=1, Status IN (7,8,12), PixKey IS NOT NULL) tem ~77,5M linhas
        / ~775K grupos resultantes (validado em 17/ago/2026). Rodar o
        GROUP BY completo todo dia é inviável a médio prazo (a tabela é
        transacional e só cresce). Optou-se por incremental por "ClientId
        impactado", no mesmo padrão de agregacao_cohort_retencao.py
        (recalcula do zero, não faz delta), porque não há garantia de que
        um PaymentRequest não mude de Status ou PixKey depois de atingir
        7/8/12 (estorno, reprocessamento etc.) — um delta cego
        (`count = count + delta`) divergiria silenciosamente nesse
        cenário.

        A origem (PaymentRequest) é SharedReplacingMergeTree — pode
        existir mais de uma versão da mesma linha (mesmo Id) até o merge
        físico do Clickhouse rodar em background. A query sempre
        deduplica por Id (QUALIFY ROW_NUMBER() ... ORDER BY
        _peerdb_version DESC) = 1, mesmo padrão do _sql_saldo_diario.

        Type=1 = saque, confirmado empiricamente em 18/ago/2026 via
        Client.FirstWithdrawalId/LastWithdrawalId (só correlaciona com
        Type=1) vs Client.FirstDepositId/LastDepositId (só correlaciona
        com Type=2) — mesmo PartnerId=180 já usado em valida_aposta().

        modo="incremental" (padrão, operação contínua): usa
        max(updated_at) de inplay.agg_pix_cliente como cursor, filtra a
        origem por _peerdb_synced_at (CDC) desde esse cursor (com margem
        de 4h, mesmo padrão de principal_zeroum/processa_saldo_diario),
        identifica os ClientId impactados nessa janela e recalcula do
        zero o agrupamento completo (sem filtro de tempo) SÓ para esses
        ClientId.

        modo="full": recalcula a tabela inteira, sem filtro de tempo.
        Usar SOMENTE para o backfill inicial (obrigatório antes de rodar
        em modo incremental — sem isso não há cursor de partida). NÃO
        rodar em produção diária: o volume de origem inviabiliza. Também
        serve como reconciliação de segurança se rodado eventualmente
        (ex.: mensal), pois seu soft-delete varre a tabela inteira, não
        só os client_id de uma janela.

        SOFT DELETE (não DELETE físico): esta tabela é consumida via
        endpoint por um cliente/parceiro EXTERNO, que faz um pull
        completo inicial e depois sincroniza incrementalmente todo dia
        via `updated_at`. Se um grupo Pix deixasse de existir (ex.:
        status saiu de 7/8/12) e a linha fosse simplesmente apagada, o
        parceiro nunca ficaria sabendo — manteria dado obsoleto
        indefinidamente. Por isso a carga marca `is_active = false` e
        `deleted_at = <timestamp>` (atualizando `updated_at` junto) em
        vez de fazer DELETE. O endpoint deve expor os dois campos, e o
        parceiro deve tratar `is_active = false` como remoção lógica.
        Grupos reativados (voltaram a existir) são upsertados de volta
        com `is_active = true`, `deleted_at = NULL`.

        partner_id: PartnerId da origem (PaymentRequest). Se None, usa 180
        para ZEROUM (valor confirmado em produção, mesmo já usado em
        valida_aposta()) e 181 para ENERGIABET — este último INFERIDO por
        analogia (181 é o PartnerId confirmado em produção para
        ClientBonus/Bonus do Energiabet, ver processa_bonus; PartnerId é
        um identificador de marca reaproveitado em praticamente todas as
        tabelas do schema, incluindo PaymentRequest, então é esperado que
        valha o mesmo — mas isso NÃO foi confirmado diretamente contra
        PaymentRequest ainda). Recomenda-se validar antes do backfill de
        ENERGIABET_PIX com a mesma técnica usada para o Type do ZEROUM
        (correlação via Client.FirstWithdrawalId/LastWithdrawalId, ver
        documentacao_agregacao_pix.md seção 3.5) — se divergir, informar
        o valor correto explicitamente via partner_id=<valor correto>.

        data_final (opcional): fecha a janela do modo incremental. Se
        None, usa datetime.now().
        """
        start_time = datetime.now()
        try:
            if partner_id is None:
                if cliente == 'ZEROUM':
                    partner_id = 180
                elif cliente == 'ENERGIABET':
                    partner_id = 181  # inferido por analogia -- ver nota acima
                else:
                    raise Exception(
                        "processa_agregacao_pix('%s', ...): partner_id não informado e "
                        "não há default conhecido para esse cliente. Confirme o "
                        "PartnerId correto de PaymentRequest e chame novamente "
                        "com partner_id=<valor confirmado>." % cliente
                    )

            self.logger.info(
                f"Iniciando carga de Agregação Pix - {cliente} (modo={modo}, partner_id={partner_id})"
            )
            self.logger.info("Fazendo a autenticação no Metabase")
            auth_id = self.conection(cliente)

            database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                        if cliente == 'ZEROUM'
                        else MetabaseDatabase.ClickhousePartnerEnergiabet.value)

            if modo == "incremental":
                self.logger.info("Recuperando data base para Agregação Pix")
                ConnectionDB.conecta(DB, cliente)
                data_importacao = ConnectionDB.recupera_dados(
                    'inplay.agg_pix_cliente', 'max(updated_at) as updated_at', ''
                )
                data_base = data_importacao[0][0]

                if data_base is None:
                    raise Exception(
                        "inplay.agg_pix_cliente está vazia. A primeira carga NÃO pode "
                        "ser incremental (não há cursor de partida). Chame "
                        "ConsumeAPI(cliente='%s_PIX', modo='full') uma única vez "
                        "para o backfill antes de ativar o modo incremental." % cliente
                    )

                # margem de segurança para cobrir latência de sync/relógio,
                # mesmo padrão usado em principal_zeroum/processa_saldo_diario
                data_inicial_dt = data_base - timedelta(hours=4)
                data_final_dt = datetime.fromisoformat(data_final) if data_final else datetime.now()

                data_inicial_str = data_inicial_dt.strftime('%Y-%m-%dT%H:%M:%S')
                data_final_str = data_final_dt.strftime('%Y-%m-%dT%H:%M:%S')

                self.logger.info(
                    f"Agregação Pix incremental [_peerdb_synced_at] de "
                    f"{data_inicial_str} a {data_final_str}"
                )
                sql = self._sql_agregacao_pix(
                    partner_id=partner_id, modo="incremental",
                    data_inicial=data_inicial_str, data_final=data_final_str
                )
            else:
                self.logger.info("Agregação Pix em modo FULL (backfill) — pode levar minutos")
                sql = self._sql_agregacao_pix(partner_id=partner_id, modo="full")

            csv = self.extrai_csv_nativo(auth_id, database, sql)
            df_pix = pd.read_csv(io.BytesIO(csv))
            df_pix = df_pix.replace({np.nan: None})

            if df_pix.empty:
                self.logger.info(
                    "Agregação Pix: nenhum registro impactado na janela atual — nada a carregar"
                )
            else:
                # is_active/deleted_at: a tabela é consumida via endpoint por
                # um parceiro externo, que faz full inicial + incremental
                # diário via `updated_at`. Um grupo que deixa de existir
                # (ex.: status saiu de 7/8/12) precisa ficar sinalizado como
                # removido — não pode simplesmente sumir da tabela, senão o
                # parceiro nunca fica sabendo e mantém dado obsoleto (ver
                # decisão de 17/ago/2026).
                df_pix['is_active'] = True
                df_pix['deleted_at'] = None

                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.deleta_dados('inplay.stg_agregacao_pix_cliente', "", self.logger)
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.insere_dados_bulk('inplay.stg_agregacao_pix_cliente', df_pix, self.logger)

                # 1) soft-delete: marca como inativo qualquer grupo que
                # ESTAVA ativo no destino para o(s) client_id do lote e NÃO
                # aparece mais no resultado recalculado. Em modo full, o
                # escopo é a tabela inteira (funciona também como
                # reconciliação de segurança contra remoções que o
                # incremental eventualmente não tenha capturado).
                if modo == "incremental":
                    client_ids = [int(c) for c in df_pix['client_id'].unique().tolist()]
                    ids_str = ", ".join(str(c) for c in client_ids)
                    filtro_escopo_destino = f"AND client_id IN ({ids_str})"
                else:
                    filtro_escopo_destino = ""

                sql_soft_delete = f"""
                    UPDATE inplay.agg_pix_cliente
                    SET is_active = false,
                        deleted_at = GETDATE(),
                        updated_at = GETDATE()
                    WHERE is_active = true
                      {filtro_escopo_destino}
                      AND NOT EXISTS (
                          SELECT 1
                          FROM inplay.stg_agregacao_pix_cliente s
                          WHERE s.client_id = inplay.agg_pix_cliente.client_id
                            AND s.pix_key = inplay.agg_pix_cliente.pix_key
                            AND s.pix_type = inplay.agg_pix_cliente.pix_type
                      )
                """
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.executa_dml(sql_soft_delete, self.logger)

                # 2) upsert: grupos ainda válidos (ou reativados, se tinham
                # sido marcados como inativos antes) recebem qtd_transacoes
                # e updated_at novos; grupos novos são inseridos.
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.mergeia_dados(
                    'inplay.stg_agregacao_pix_cliente', 'inplay.agg_pix_cliente',
                    df_pix, ['client_id', 'pix_key', 'pix_type'], self.logger
                )

            self.logger.info(f"Carga de Agregação Pix concluída com sucesso - {cliente}")

            self.db_logger.log_operation(
                operation=f'ETL_{cliente}_PIX',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente=cliente
            )

        except Exception as e:
            self.logger.error(f"Erro na carga de Agregação Pix {cliente}: {e}")
            self.db_logger.log_operation(
                operation=f'ETL_{cliente}_PIX',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente=cliente
            )
            b = f"Descrição do erro:\n{e}\n\n=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            send_email(subject=f"[FALHA ENGENHARIA] {cliente} - Erro na carga de Agregação Pix", body=b)
            raise

    @staticmethod
    def _sql_agregacao_pix(partner_id: int, modo: str, data_inicial: str = None, data_final: str = None):
        """
        SQL nativo (ClickHouse) para a agregação de PaymentRequest por
        ClientId/PixKey/PixType. Ver docstring de processa_agregacao_pix
        para a decisão de design (full vs incremental, dedup).

        modo="incremental": filtra primeiro por ClientId impactado (CTE
        `impactados`, baseada em _peerdb_synced_at) ANTES de deduplicar
        por Id — deduplicar só depois de restringir por ClientId evita
        rodar o ROW_NUMBER() sobre as 77M+ linhas da tabela inteira a
        cada execução diária (a tabela é ORDER BY Id, não por ClientId,
        então o filtro de ClientId não pula granules, mas ainda evita o
        custo do ROW_NUMBER/QUALIFY sobre o volume total).

        modo="full": mesma lógica, sem a CTE de impactados — dedup e
        agrupamento rodam sobre a tabela inteira. Só para backfill.
        """
        filtro_escopo = f"PartnerId = {partner_id} AND Type = 1"

        if modo == "incremental":
            if not data_inicial or not data_final:
                raise ValueError("modo='incremental' exige data_inicial e data_final")
            cte_impactados = f"""impactados AS (
    SELECT DISTINCT ClientId
    FROM PaymentRequest
    WHERE _peerdb_is_deleted = 0
      AND {filtro_escopo}
      AND _peerdb_synced_at BETWEEN '{data_inicial}' AND '{data_final}'
),
"""
            filtro_impactados = "AND ClientId IN (SELECT ClientId FROM impactados)"
        else:
            cte_impactados = ""
            filtro_impactados = ""

        return f"""
WITH {cte_impactados}pr_scope AS (
    SELECT Id, ClientId, PixKey, PixType, Status, _peerdb_version
    FROM PaymentRequest
    WHERE _peerdb_is_deleted = 0
      AND {filtro_escopo}
      {filtro_impactados}
),
pr_dedup AS (
    SELECT Id, ClientId, PixKey, PixType, Status, _peerdb_version
    FROM pr_scope
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY Id
        ORDER BY _peerdb_version DESC
    ) = 1
)
SELECT
    ClientId AS client_id,
    PixKey   AS pix_key,
    PixType  AS pix_type,
    count(0) AS qtd_transacoes,
    now()    AS updated_at,
    now()    AS import_date
FROM pr_dedup
WHERE Status IN (7, 8, 12)
  AND PixKey IS NOT NULL
GROUP BY ClientId, PixKey, PixType
"""

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
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.HTTPError) as e:
            # HTTPError só entra nesta subdivisão quando é 5xx (erro do
            # servidor Metabase/backend, potencialmente transitório ou
            # ligado ao tamanho/custo da consulta — subdividir pode
            # resolver, igual a um timeout). CORREÇÃO DESTA REVISÃO: até
            # aqui, um 500 do Metabase não caía em nenhum destes ramos —
            # `extrai_csv_nativo` já tentava 3x internamente e, ao
            # esgotar as tentativas, o HTTPError subia direto por todos
            # os níveis de recursão (período e ClientId) e derrubava o
            # processo inteiro, mesmo quando reduzir a janela/faixa teria
            # resolvido. Um 4xx (ex.: SQL malformado) não é tratado como
            # transitório: subdividir não conserta uma query com erro de
            # sintaxe, então deixamos subir imediatamente para não gastar
            # minutos refazendo a mesma consulta quebrada em pedaços cada
            # vez menores.
            if isinstance(e, requests.exceptions.HTTPError):
                status = e.response.status_code if e.response is not None else None
                if status is None or status < 500:
                    raise
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
                if not response.ok:
                    print("\n" + "=" * 100)
                    print("ERRO RETORNADO PELO METABASE")
                    print("=" * 100)
                    print(f"HTTP: {response.status_code}")
                    print(response.text)
                    print("=" * 100)
                    response.raise_for_status()
                return response.content
            except requests.exceptions.RequestException as e:
                ultimo_erro = e
                # CORREÇÃO DESTA REVISÃO: str(e) num HTTPError só traz
                # "500 Server Error: Internal Server Error for url: ...",
                # sem a mensagem real que o Metabase manda no corpo da
                # resposta (costuma vir com o motivo do erro de
                # SQL/backend). Sem isso, o e-mail de alerta e o log não
                # davam pista nenhuma do motivo real — só o status code.
                corpo_resposta = None
                if e.response is not None:
                    try:
                        corpo_resposta = e.response.text[:2000]
                    except Exception:
                        corpo_resposta = None
                self.logger.warning(
                    f"[extrai_csv_nativo] Tentativa {tentativa}/{max_tentativas} "
                    f"falhou (timeout={timeout}s): {e}"
                    + (f" | corpo da resposta: {corpo_resposta}" if corpo_resposta else "")
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

    def extrai_fact_user_bonus_por_periodo(self, auth_id, id_database,
                                        data_inicial_dt, data_final_dt,
                                        campo_filtro="AwardingTime", filtro_extra="", partner_id=180):
        data_inicial_str = data_inicial_dt.strftime('%Y-%m-%dT%H:%M:%S')
        data_final_str = data_final_dt.strftime('%Y-%m-%dT%H:%M:%S')
        duracao = data_final_dt - data_inicial_dt

        self.logger.info(
            f"Extraindo fact_user_bonus [SQL nativo, {campo_filtro}{filtro_extra and ', ' + filtro_extra}] de "
            f"{data_inicial_str} a {data_final_str}"
        )

        sql = self._sql_fact_user_bonus(data_inicial_str, data_final_str, campo_filtro, filtro_extra, partner_id)

        try:
            csv = self.extrai_csv_nativo(auth_id, id_database, sql)
            df = pd.read_csv(io.BytesIO(csv))
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if duracao <= timedelta(seconds=1):
                self.logger.error(
                    f"fact_user_bonus: falha de conexão ({type(e).__name__}) na "
                    f"janela mínima ({data_inicial_str}-{data_final_str}) — não é "
                    f"possível dividir mais. Erro: {e}"
                )
                raise
            self.logger.warning(
                f"fact_user_bonus: falha de conexão ({type(e).__name__}) na janela "
                f"{data_inicial_str}-{data_final_str}. Dividindo ao meio e tentando de novo."
            )
            meio = data_inicial_dt + duracao / 2
            df_primeira_metade = self.extrai_fact_user_bonus_por_periodo(
                auth_id, id_database, data_inicial_dt, meio, campo_filtro, filtro_extra, partner_id
            )
            df_segunda_metade = self.extrai_fact_user_bonus_por_periodo(
                auth_id, id_database, meio, data_final_dt, campo_filtro, filtro_extra, partner_id
            )
            return pd.concat([df_primeira_metade, df_segunda_metade], ignore_index=True)

        if len(df) < self.LIMITE_LINHAS_METABASE:
            return df

        if duracao <= timedelta(seconds=1):
            self.logger.warning(
                f"fact_user_bonus: janela mínima atingida ({data_inicial_str}-"
                f"{data_final_str}) e ainda assim retornou {len(df)} linhas — "
                f"possível truncamento residual. Revisar manualmente."
            )
            return df

        self.logger.warning(
            f"fact_user_bonus: {len(df)} linhas (teto do Metabase) para "
            f"{data_inicial_str}–{data_final_str}. Dividindo a janela ao meio."
        )
        meio = data_inicial_dt + duracao / 2
        df_primeira_metade = self.extrai_fact_user_bonus_por_periodo(
            auth_id, id_database, data_inicial_dt, meio, campo_filtro, filtro_extra, partner_id
        )
        df_segunda_metade = self.extrai_fact_user_bonus_por_periodo(
            auth_id, id_database, meio, data_final_dt, campo_filtro, filtro_extra, partner_id
        )
        return pd.concat([df_primeira_metade, df_segunda_metade], ignore_index=True)

    @staticmethod
    def _sql_fact_user_bonus(data_inicial, data_final, campo_filtro="AwardingTime",
                              filtro_extra="", partner_id=180):
        return f"""
        WITH bonus_dedup AS
        (
            SELECT
                Id, BonusId, ClientId, PartnerId, Status, SubStatus,
                BonusPrize, InitialBonusPrize, FinalAmount, TurnoverAmountLeft,
                ReuseNumber, Cost, CreationTime, AwardingTime, CalculationTime,
                ValidUntil, TriggerId, RefClientId, _peerdb_synced_at
            FROM ClientBonus
            WHERE _peerdb_is_deleted = 0
            AND PartnerId = {partner_id}
            AND {campo_filtro} >= '{data_inicial.replace("T", " ")}' AND {campo_filtro} < '{data_final.replace("T", " ")}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY Id
                ORDER BY _peerdb_version DESC, _peerdb_synced_at DESC
            ) = 1
        )
        SELECT
            Id                 AS id,
            BonusId            AS bonus_id,
            ClientId           AS client_id,
            PartnerId          AS partner_id,
            Status             AS status,
            SubStatus          AS sub_status,
            BonusPrize         AS bonus_prize,
            InitialBonusPrize  AS initial_bonus_prize,
            FinalAmount        AS final_amount,
            TurnoverAmountLeft AS turnover_amount_left,
            ReuseNumber        AS reuse_number,
            Cost               AS cost,
            CreationTime       AS creation_time,
            AwardingTime       AS awarding_time,
            CalculationTime    AS calculation_time,
            ValidUntil         AS valid_until,
            TriggerId          AS trigger_id,
            RefClientId        AS ref_client_id,
            _peerdb_synced_at  AS source_updated_at,
            now()              AS import_date
        FROM bonus_dedup
        WHERE 1=1
        {filtro_extra};
        """

    @staticmethod
    def _sql_dim_bonus(partner_id=180):
        # Achado da validação: Bonus também tem CDC (594 linhas vs. 406 Ids
        # distintos) -- precisa de dedup igual fact_user_bonus, sem filtro de
        # período (tabela pequena, ~600 linhas).
        # Filtro PartnerId: mesma lógica de fact_user_bonus -- escopo de
        # relevância, confirmado no-op nos dados atuais do ZEROUM (180),
        # mantido como proteção. CONFERIR o valor correto antes de rodar
        # para ENERGIABET -- pode não ser 180.
        #
        # Sem prefixo de schema (ex.: "partner_zeroum.") de propósito --
        # mesmo padrão de _sql_saldo_diario/_sql_agregacao_pix: a conexão
        # Metabase (parâmetro `database` em extrai_csv_nativo) já resolve
        # a origem correta por cliente. Um prefixo fixo aqui faria a
        # query sempre ler do ZEROUM, mesmo quando conectada via ENERGIABET
        # (achado de 18/ago/2026 -- ver processa_bonus).
        return f"""
        WITH dim_bonus_dedup AS
        (
            SELECT
                Id, Name, BonusType, Status, StartTime, FinishTime,
                ValidForAwarding, ValidForSpending, TurnoverCount,
                MinAmount, MaxAmount, Percent, AutoClaim,
                CreationTime, LastUpdateTime, _peerdb_synced_at
            FROM Bonus
            WHERE _peerdb_is_deleted = 0
              AND PartnerId = {partner_id}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY Id
                ORDER BY _peerdb_version DESC, _peerdb_synced_at DESC
            ) = 1
        )
        SELECT
            Id               AS bonus_id,
            Name             AS bonus_name,
            BonusType        AS bonus_type,
            Status           AS active,
            StartTime        AS start_date,
            FinishTime       AS end_date,
            ValidForAwarding AS valid_for_awarding,
            ValidForSpending AS valid_for_spending,
            TurnoverCount    AS turnover_count,
            MinAmount        AS min_amount,
            MaxAmount        AS max_amount,
            Percent          AS bonus_percent,
            AutoClaim        AS auto_claim,
            CreationTime     AS creation_time,
            LastUpdateTime   AS last_update_time,
            now()            AS import_date
        FROM dim_bonus_dedup;
        """

    @staticmethod
    def _sql_bridge_bonus_product(partner_id=180):
        # Dedup aplicado por padrão/segurança, mesmo com validação atual
        # (372=372) sem duplicidade -- protege contra futuras atualizações
        # em BonusProduct e mantém consistência com dim_bonus/fact_user_bonus.
        # BonusProduct não tem PartnerId próprio -- escopo aplicado
        # indiretamente via BonusId, restrito aos bônus que pertencem ao
        # parceiro informado (join contra Bonus, mesmo filtro usado em
        # dim_bonus). Sem prefixo de schema -- ver nota em _sql_dim_bonus.
        return f"""
        WITH bridge_dedup AS
        (
            SELECT
                bp.Id, bp.BonusId, bp.ProductId, bp.CashBackPercent, bp.FreeSpinCost,
                bp._peerdb_synced_at
            FROM BonusProduct bp
            INNER JOIN Bonus b
                ON bp.BonusId = b.Id
                AND b._peerdb_is_deleted = 0
                AND b.PartnerId = {partner_id}
            WHERE bp._peerdb_is_deleted = 0
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY bp.Id
                ORDER BY bp._peerdb_version DESC, bp._peerdb_synced_at DESC
            ) = 1
        )
        SELECT
            Id              AS id,
            BonusId         AS bonus_id,
            ProductId       AS product_id,
            CashBackPercent AS cashback_percent,
            FreeSpinCost    AS free_spin_cost,
            now()           AS import_date
        FROM bridge_dedup;
        """

    def processa_bonus(self, cliente, modo="incremental", data_final=None, carregar_dimensoes=True,
                        campo_filtro_override=None, filtro_extra="", partner_id=None):
        """
        Carga isolada de Bônus (ClientBonus / Bonus / BonusProduct).

        Não é chamada por principal_zeroum()/principal_energiabet() — roda
        como cliente próprio (ZEROUM_BONUS / ENERGIABET_BONUS), em cron
        separado, sem tocar nas tabelas do pipeline horário existente.

        modo="incremental" (padrão): usa max(source_updated_at) de
        inplay.fact_user_bonus e filtra a origem por _peerdb_synced_at
        (cursor CDC) — correto para operação contínua.

        modo="<DATA_ISO>" (ex.: "2025-04-01T00:00:00"): usado para backfill
        histórico. Filtra a origem por AwardingTime (data de negócio), NÃO
        por _peerdb_synced_at — mesma razão do Saldo Diário: cargas em lote
        sincronizam tudo com o mesmo timestamp.

        data_final (opcional): fecha a janela em modo de backfill. Se None
        em modo backfill, extrai até agora.

        carregar_dimensoes (opcional, default True): se False, PULA a carga
        de dim_bonus/bridge_bonus_product. OTIMIZAÇÃO para backfill em
        blocos -- essas 2 tabelas são pequenas e não mudam bloco a bloco,
        recarregar em toda chamada desperdiça um round-trip completo ao
        Metabase + delete + insert + merge. processa_bonus_backfill já usa
        isso automaticamente (só carrega no primeiro bloco).

        campo_filtro_override (opcional): força um campo de filtro
        específico, IGNORANDO a regra automática (_peerdb_synced_at no
        incremental, AwardingTime no backfill). Usado pelo backfill
        suplementar de registros com AwardingTime NULL (ver
        processa_bonus_backfill_awarding_nulo).

        filtro_extra (opcional): condição SQL adicional (ex.: "AND
        AwardingTime IS NULL") repassada para _sql_fact_user_bonus. Default
        vazio -- não afeta o comportamento existente.

        partner_id (opcional): PartnerId de origem (ClientBonus/Bonus/
        BonusProduct). Se None, usa 180 para ZEROUM (valor confirmado em
        produção) -- para ENERGIABET, CONFERIR e informar explicitamente
        antes de rodar; não há default seguro conhecido ainda (achado de
        18/ago/2026: as três queries de Bônus estavam com "partner_zeroum."
        e "PartnerId = 180" fixos no código, nunca antes parametrizados
        para ENERGIABET_BONUS -- corrigido nesta revisão).
        """
        try:
            start_time = datetime.now()

            if partner_id is None:
                if cliente == 'ZEROUM':
                    partner_id = 180
                elif cliente == 'ENERGIABET':
                    partner_id = 181
                else:
                    raise Exception(
                        "processa_bonus('%s', ...): partner_id não informado e não há "
                        "default seguro para esse cliente. Confirme o PartnerId correto "
                        "de ClientBonus/Bonus/BonusProduct e chame novamente com "
                        "partner_id=<valor confirmado>." % cliente
                    )

            self.logger.info(
                f"Iniciando carga de Bônus - {cliente} (modo={modo}, data_final={data_final}, partner_id={partner_id})"
            )
            self.logger.info("Fazendo a autenticação no Metabase")
            auth_id = self.conection(cliente)

            database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                        if cliente == 'ZEROUM'
                        else MetabaseDatabase.ClickhousePartnerEnergiabet.value)

            if carregar_dimensoes:
                # ================================================================
                # 1. DIM_BONUS — carga completa (tabela pequena, ~600 linhas)
                # ================================================================
                self.logger.info("Extraindo dim_bonus (carga completa)")
                sql_dim = self._sql_dim_bonus(partner_id=partner_id)
                csv_dim = self.extrai_csv_nativo(auth_id, database, sql_dim)
                df_dim_bonus = pd.read_csv(io.BytesIO(csv_dim))
                df_dim_bonus = df_dim_bonus.replace({np.nan: None})

                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.deleta_dados('inplay.stg_dim_bonus', "", self.logger)
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.insere_dados_bulk('inplay.stg_dim_bonus', df_dim_bonus, self.logger)
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.mergeia_dados(
                    'inplay.stg_dim_bonus', 'inplay.dim_bonus',
                    df_dim_bonus, ['bonus_id'], self.logger
                )
                self.logger.info(f"dim_bonus: {len(df_dim_bonus)} linhas carregadas")

                # ================================================================
                # 2. BRIDGE_BONUS_PRODUCT — carga completa
                # ================================================================
                self.logger.info("Extraindo bridge_bonus_product (carga completa)")
                sql_bridge = self._sql_bridge_bonus_product(partner_id=partner_id)
                csv_bridge = self.extrai_csv_nativo(auth_id, database, sql_bridge)
                df_bridge = pd.read_csv(io.BytesIO(csv_bridge))
                df_bridge = df_bridge.replace({np.nan: None})

                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.deleta_dados('inplay.stg_bridge_bonus_product', "", self.logger)
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.insere_dados_bulk('inplay.stg_bridge_bonus_product', df_bridge, self.logger)
                ConnectionDB.conecta(DB, cliente)
                ConnectionDB.mergeia_dados(
                    'inplay.stg_bridge_bonus_product', 'inplay.bridge_bonus_product',
                    df_bridge, ['id'], self.logger
                )
                self.logger.info(f"bridge_bonus_product: {len(df_bridge)} linhas carregadas")
            else:
                self.logger.info("Pulando dim_bonus/bridge_bonus_product (carregar_dimensoes=False)")

            # ================================================================
            # 3. FACT_USER_BONUS — chunking automático por período
            # ================================================================
            campo_filtro_periodo = campo_filtro_override or (
                "_peerdb_synced_at" if modo == "incremental" else "AwardingTime"
            )

            if modo == "incremental":
                self.logger.info("Recuperando data base para fact_user_bonus")
                ConnectionDB.conecta(DB, cliente)
                data_importacao = ConnectionDB.recupera_dados(
                    'inplay.fact_user_bonus', 'max(source_updated_at) as source_updated_at', ''
                )
                data_base = data_importacao[0][0]

                if data_base is None:
                    raise Exception(
                        "inplay.fact_user_bonus está vazia. A primeira carga NÃO pode "
                        "ser incremental. Chame ConsumeAPI(...).processa_bonus('%s', "
                        "modo='<DATA_CORTE_ISO>'), ex.: modo='2025-04-01T00:00:00', "
                        "informando uma data de corte explícita antes de rodar em "
                        "modo incremental." % cliente
                    )
                data_inicial_dt = data_base - timedelta(hours=4)
            else:
                data_inicial_dt = datetime.fromisoformat(modo)

            data_final_dt = datetime.fromisoformat(data_final) if data_final else datetime.now()

            self.logger.info(
                f"Extraindo fact_user_bonus [{campo_filtro_periodo}] de "
                f"{data_inicial_dt} a {data_final_dt}"
            )
            df_fact_bonus = self.extrai_fact_user_bonus_por_periodo(
                auth_id, database, data_inicial_dt, data_final_dt,
                campo_filtro=campo_filtro_periodo, filtro_extra=filtro_extra, partner_id=partner_id
            )
            df_fact_bonus = df_fact_bonus.replace({np.nan: None})

            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.deleta_dados('inplay.stg_fact_user_bonus', "", self.logger)
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_bonus', df_fact_bonus, self.logger)
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.mergeia_dados(
                'inplay.stg_fact_user_bonus', 'inplay.fact_user_bonus',
                df_fact_bonus, ['id'], self.logger
            )
            self.logger.info(f"fact_user_bonus: {len(df_fact_bonus)} linhas carregadas")

            # ================================================================
            # 4. AGG_BONUS_CONCESSOES — atualização incremental da visão
            #    agrupada bruta (client_id + bonus_id), consumida pelo
            #    endpoint de concessões (equivalente ao relatório de PIX).
            #    Recalcula SÓ os pares impactados nesta carga, não a tabela
            #    inteira. Falha aqui não derruba a carga de fact_user_bonus,
            #    que já está commitada neste ponto.
            #
            #    partner_id não precisa ser repassado aqui: o módulo deriva
            #    o partner_id de cada grupo a partir de fact_user_bonus (já
            #    carregado corretamente por cliente), não faz nenhuma
            #    extração adicional do Clickhouse/Metabase -- por isso não
            #    está sujeito ao mesmo bug de schema/PartnerId fixo que
            #    afetou as demais queries de Bônus (achado de 19/ago/2026).
            # ================================================================
            executar_agregacao_bonus_concessoes(df_fact_bonus, cliente, DB, self.logger)

            try:
                executar_agregacao_fraude_bonus(cliente, DB, self.logger)
                executar_disparo_alertas_bonus(df_fact_bonus, cliente, DB, partner_id, self.logger)
            #except Exception as e:
                #self.logger.error(f"[FRAUDE_BONUS] Falha no monitoramento antifraude (não derruba a carga de bônus): {e}")

            except Exception as e:
                import traceback
                print(f"[FRAUDE_BONUS] ERRO: {e}")
                traceback.print_exc()
                self.logger.error(f"[FRAUDE_BONUS] Falha no monitoramento antifraude (não derruba a carga de bônus): {e}")

            self.logger.info(f"Carga de Bônus concluída com sucesso - {cliente}")
            self.db_logger.log_operation(
                operation='ETL_BONUS',
                status='SUCCESS',
                start_time=start_time,
                end_time=datetime.now(),
                cliente=cliente
            )

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erro de rede na carga de Bônus {cliente}: {e}")
            self.db_logger.log_operation(
                operation='ETL_BONUS',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente=cliente
            )
            b = f"Descrição do erro:\n{e}\n\n=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            send_email(subject=f"[FALHA ENGENHARIA] {cliente} - Erro na carga de Bônus", body=b)
            raise Exception(f"Erro na carga de Bônus {cliente}: {e}")
        except Exception as e:
            self.logger.error(f"Erro na carga de Bônus {cliente}: {e}")
            self.db_logger.log_operation(
                operation='ETL_BONUS',
                status='FAILED',
                start_time=start_time,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente=cliente
            )
            b = f"Descrição do erro:\n{e}\n\n=== HISTÓRICO DO LOGGER ===\n{self.get_log_history()}"
            send_email(subject=f"[FALHA ENGENHARIA] {cliente} - Erro na carga de Bônus", body=b)
            raise

    def _perfil_diario_bonus(self, cliente, data_inicial_dt, data_final_dt, partner_id=180):
        """
        OTIMIZAÇÃO: roda UMA query leve (COUNT(*) agrupado por dia) cobrindo
        todo o intervalo do backfill, em vez de descobrir o volume "na
        marra" (baixar até estourar o teto, descartar, dividir, tentar de
        novo -- o padrão caro que causava lentidão: 4 downloads de ~1M
        linhas cada, jogados fora, só para um único bloco de 30 dias).
 
        Retorna uma lista de tuplas (data: date, contagem: int), ordenada.
        """
        auth_id = self.conection(cliente)
        database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                    if cliente == 'ZEROUM'
                    else MetabaseDatabase.ClickhousePartnerEnergiabet.value)
 
        data_ini_str = data_inicial_dt.strftime('%Y-%m-%d')
        data_fim_str = data_final_dt.strftime('%Y-%m-%d')
 
        # Nota: este COUNT conta linhas ANTES do dedup por Id (não usa
        # QUALIFY) -- é uma superestimativa leve (inclui versões antigas de
        # CDC), o que é seguro aqui: preferimos janelas um pouco menores do
        # que o necessário a estourar o teto de novo. O custo desse
        # over-estimate é pequeno dado que a duplicidade de ClientBonus é
        # ~0,8%, não muda a ordem de grandeza.
        #
        # Sem prefixo de schema (achado de 19/ago/2026: "partner_zeroum."
        # fixo aqui fazia o backfill do ENERGIABET tentar ler
        # partner_zeroum.ClientBonus e falhar com ACCESS_DENIED -- a
        # conexão do Metabase (`database`, acima) já resolve a origem
        # certa por cliente, mesmo padrão de _sql_fact_user_bonus).
        sql_perfil = f"""
        SELECT
            toDate(AwardingTime) AS dia,
            COUNT(*) AS total
        FROM ClientBonus
        WHERE _peerdb_is_deleted = 0
          AND PartnerId = {partner_id}
          AND toDate(AwardingTime) BETWEEN '{data_ini_str}' AND '{data_fim_str}'
        GROUP BY dia
        ORDER BY dia
        """
        csv_perfil = self.extrai_csv_nativo(auth_id, database, sql_perfil)
        df_perfil = pd.read_csv(io.BytesIO(csv_perfil))
        df_perfil['dia'] = pd.to_datetime(df_perfil['dia']).dt.date
 
        return list(df_perfil.itertuples(index=False, name=None))

    def _perfil_diario_bonus_generico(self, cliente, data_inicial_dt, data_final_dt,
                                   campo_data="CreationTime", filtro_extra="", partner_id=180):
        """
        Igual _perfil_diario_bonus, mas parametrizável -- usada pelo backfill
        suplementar de registros com AwardingTime NULL, que precisa perfilar
        por CreationTime (sempre preenchido) e escopar só o universo faltante.
        """
        auth_id = self.conection(cliente)
        database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                    if cliente == 'ZEROUM'
                    else MetabaseDatabase.ClickhousePartnerEnergiabet.value)

        data_ini_str = data_inicial_dt.strftime('%Y-%m-%d')
        data_fim_str = data_final_dt.strftime('%Y-%m-%d')

        # Sem prefixo de schema -- ver nota em _perfil_diario_bonus.
        sql_perfil = f"""
        SELECT
            toDate({campo_data}) AS dia,
            COUNT(*) AS total
        FROM ClientBonus
        WHERE _peerdb_is_deleted = 0
        AND PartnerId = {partner_id}
        AND toDate({campo_data}) BETWEEN '{data_ini_str}' AND '{data_fim_str}'
        {filtro_extra}
        GROUP BY dia
        ORDER BY dia
        """
        csv_perfil = self.extrai_csv_nativo(auth_id, database, sql_perfil)
        df_perfil = pd.read_csv(io.BytesIO(csv_perfil))
        df_perfil['dia'] = pd.to_datetime(df_perfil['dia']).dt.date

        return list(df_perfil.itertuples(index=False, name=None))
 
    @staticmethod
    def _monta_janelas_por_volume(perfil_diario, limite_linhas=900000):
        """
        Agrupa dias consecutivos em janelas cuja soma de linhas fica abaixo
        de `limite_linhas` (com margem de segurança sob LIMITE_LINHAS_METABASE
        = 1.048.575 -- 900K deixa ~15% de folga para o dedup por Id não
        reduzir tanto quanto o esperado, e para dias com pico de volume).
 
        Retorna lista de tuplas (data_inicio: date, data_fim: date).
        """
        if not perfil_diario:
            return []
 
        janelas = []
        inicio_janela = perfil_diario[0][0]
        soma_atual = 0
        fim_janela = inicio_janela
 
        for dia, total in perfil_diario:
            if soma_atual + total > limite_linhas and soma_atual > 0:
                janelas.append((inicio_janela, fim_janela))
                inicio_janela = dia
                soma_atual = 0
            soma_atual += total
            fim_janela = dia
 
        janelas.append((inicio_janela, fim_janela))
        return janelas
 
    def processa_bonus_backfill(self, cliente, data_inicio_historico, data_corte,
                                 tamanho_bloco_dias=None, validar_blocos=True,
                                 limite_linhas_por_janela=900000, partner_id=None):
        """
        Carga histórica (backfill) de Bônus, com janelas dimensionadas pelo
        volume real de cada período (não por um número fixo de dias).
 
        Parâmetros:
            cliente: 'ZEROUM' ou 'ENERGIABET'
            data_inicio_historico: string ISO, ex. '2023-01-01T00:00:00'
            data_corte: string ISO, ex. '2025-08-01T00:00:00'
            tamanho_bloco_dias: se informado (int), volta ao comportamento
                ANTIGO de blocos fixos em dias, ignorando o perfil de
                volume -- útil como fallback se a query de perfil falhar
                por algum motivo. Default None = usa o modo otimizado
                (recomendado).
            validar_blocos: mesma semântica de antes (fail-fast por bloco).
            limite_linhas_por_janela: teto de linhas por janela ao usar o
                modo otimizado (default 900.000, com margem sob o teto real
                do Metabase de 1.048.575).
 
        Retomada após falha: mesma lógica de antes -- o log de erro traz a
        data exata pra retomar.
        """
        data_atual = datetime.fromisoformat(data_inicio_historico)
        data_corte_dt = datetime.fromisoformat(data_corte)
 
        if data_atual >= data_corte_dt:
            raise ValueError(
                f"data_inicio_historico ({data_atual}) precisa ser anterior "
                f"a data_corte ({data_corte_dt})."
            )

        if partner_id is None:
            if cliente == 'ZEROUM':
                partner_id = 180
            elif cliente == 'ENERGIABET':
                partner_id = 181
            else:
                raise Exception(
                    "processa_bonus_backfill('%s', ...): partner_id não informado "
                    "e não há default conhecido para esse cliente. Confirme o "
                    "PartnerId correto e chame novamente com "
                    "partner_id=<valor confirmado>." % cliente
                )
 
        if tamanho_bloco_dias is not None:
            # Modo fallback: blocos fixos em dias (comportamento antigo)
            bloco = timedelta(days=tamanho_bloco_dias)
            janelas = []
            cursor = data_atual
            while cursor < data_corte_dt:
                fim = min(cursor + bloco, data_corte_dt)
                janelas.append((cursor.date(), fim.date()))
                cursor = fim
            self.logger.info(
                f"[BACKFILL BÔNUS] Modo fallback (blocos fixos de "
                f"{tamanho_bloco_dias} dias) — {len(janelas)} blocos"
            )
        else:
            # Modo otimizado: 1 query de perfil, janelas dimensionadas por volume
            self.logger.info(
                "[BACKFILL BÔNUS] Calculando perfil de volume diário "
                "(1 query, evita downloads desperdiçados)..."
            )
            perfil = self._perfil_diario_bonus(cliente, data_atual, data_corte_dt, partner_id=partner_id)
            janelas_data = self._monta_janelas_por_volume(perfil, limite_linhas_por_janela)
            # Converter (date, date) em (datetime, datetime) no formato que
            # processa_bonus espera, com o fim de cada janela avançado 1 dia
            # (já que a janela é [inicio, fim] inclusivo por dia).
            janelas = [
                (datetime.combine(ini, datetime.min.time()),
                 datetime.combine(fim, datetime.min.time()) + timedelta(days=1))
                for ini, fim in janelas_data
            ]
            total_linhas_perfil = sum(t for _, t in perfil)
            self.logger.info(
                f"[BACKFILL BÔNUS] Perfil calculado: {len(perfil)} dias, "
                f"~{total_linhas_perfil} linhas totais (antes de dedup), "
                f"{len(janelas)} janelas de até {limite_linhas_por_janela} linhas cada"
            )
 
        total_blocos_previsto = len(janelas)
        self.logger.info(
            f"[BACKFILL BÔNUS] Iniciando — {cliente}, de {data_atual} até "
            f"{data_corte_dt}, {total_blocos_previsto} janelas previstas"
        )
 
        inicio_execucao = time.monotonic()
        total_blocos = 0
        for data_ini_bloco, data_fim_bloco in janelas:
            total_blocos += 1
            inicio_bloco = time.monotonic()
 
            self.logger.info(
                f"[BACKFILL BÔNUS] Bloco {total_blocos}/{total_blocos_previsto}: "
                f"{data_ini_bloco.strftime('%Y-%m-%d')} -> {data_fim_bloco.strftime('%Y-%m-%d')}"
            )
 
            # OTIMIZAÇÃO/RESILIÊNCIA: retry automático para erros
            # TRANSITÓRIOS de conexão (server closed the connection
            # unexpectedly, etc.) -- diferente de bugs de lógica, esses
            # acontecem ocasionalmente em execuções longas (cluster,
            # VPN/firewall, blip de rede) e não são "culpa" do código. Como
            # o MERGE é idempotente, é seguro tentar o mesmo bloco de novo.
            MAX_TENTATIVAS_BLOCO = 3
            ERROS_TRANSITORIOS = (psycopg2.OperationalError, psycopg2.InterfaceError)
 
            for tentativa in range(1, MAX_TENTATIVAS_BLOCO + 1):
                try:
                    self.processa_bonus(
                        cliente,
                        modo=data_ini_bloco.strftime('%Y-%m-%dT%H:%M:%S'),
                        data_final=data_fim_bloco.strftime('%Y-%m-%dT%H:%M:%S'),
                        carregar_dimensoes=(total_blocos == 1),  # só no primeiro bloco
                        partner_id=partner_id
                    )
                    break  # sucesso, sai do loop de retry
                except ERROS_TRANSITORIOS as e:
                    if tentativa >= MAX_TENTATIVAS_BLOCO:
                        self.logger.error(
                            f"[BACKFILL BÔNUS] Bloco {data_ini_bloco.strftime('%Y-%m-%d')} "
                            f"-> {data_fim_bloco.strftime('%Y-%m-%d')} falhou {MAX_TENTATIVAS_BLOCO}x "
                            f"por erro de conexão. Para retomar, chame "
                            f"processa_bonus_backfill(cliente='{cliente}', "
                            f"data_inicio_historico='{data_ini_bloco.strftime('%Y-%m-%dT%H:%M:%S')}', "
                            f"data_corte='{data_corte}'). Erro: {e}"
                        )
                        raise
                    espera = 30 * tentativa  # 30s, depois 60s
                    self.logger.warning(
                        f"[BACKFILL BÔNUS] Erro de conexão no bloco "
                        f"{data_ini_bloco.strftime('%Y-%m-%d')} (tentativa {tentativa}/"
                        f"{MAX_TENTATIVAS_BLOCO}): {type(e).__name__}: {e}. "
                        f"Tentando de novo em {espera}s..."
                    )
                    time.sleep(espera)
                except Exception as e:
                    # Erros que NÃO são de conexão (ex.: validação de dado,
                    # erro de programação) não devem ser retentados
                    # cegamente -- param imediatamente, igual antes.
                    self.logger.error(
                        f"[BACKFILL BÔNUS] Falhou no bloco {data_ini_bloco.strftime('%Y-%m-%d')} "
                        f"-> {data_fim_bloco.strftime('%Y-%m-%d')}. Para retomar, chame "
                        f"processa_bonus_backfill(cliente='{cliente}', "
                        f"data_inicio_historico='{data_ini_bloco.strftime('%Y-%m-%dT%H:%M:%S')}', "
                        f"data_corte='{data_corte}'). Erro: {e}"
                    )
                    raise
 
            if validar_blocos:
                self._valida_bloco_bonus(cliente, data_ini_bloco, data_fim_bloco - timedelta(days=1), partner_id=partner_id)
 
            duracao_bloco = time.monotonic() - inicio_bloco
            tempo_decorrido = time.monotonic() - inicio_execucao
            media_por_bloco = tempo_decorrido / total_blocos
            blocos_restantes = max(total_blocos_previsto - total_blocos, 0)
            eta_restante = media_por_bloco * blocos_restantes
 
            self.logger.info(
                f"[BACKFILL BÔNUS] Bloco {total_blocos}/{total_blocos_previsto} OK em "
                f"{self._formata_duracao(duracao_bloco)} | decorrido: "
                f"{self._formata_duracao(tempo_decorrido)} | média/bloco: "
                f"{self._formata_duracao(media_por_bloco)} | restante (estimado): "
                f"{self._formata_duracao(eta_restante)}"
            )
 
        tempo_total = time.monotonic() - inicio_execucao
        self.logger.info(
            f"[BACKFILL BÔNUS] Concluído — {total_blocos} blocos processados, "
            f"{cliente}, até {data_corte_dt.strftime('%Y-%m-%d')}, "
            f"tempo total: {self._formata_duracao(tempo_total)}"
        )
 
    @staticmethod
    def _formata_duracao(segundos):
        return str(timedelta(seconds=int(segundos)))
 
    def _valida_bloco_bonus(self, cliente, data_inicio_bloco, data_fim_bloco, partner_id=180):
        """
        Validação automática de um bloco do backfill: compara a contagem
        de linhas na origem (ClientBonus, já deduplicado) com a contagem
        na tabela destino (fact_user_bonus), na mesma janela de
        AwardingTime. Mesma lógica do Passo 6 do guia, só que automática.
 
        Se não bater, loga ERROR e levanta exceção — o backfill para nesse
        bloco em vez de seguir acumulando dados possivelmente incorretos.
 
        ⚠️ CORREÇÃO IMPORTANTE (achado real, validado com dados de produção):
        a query do destino usa `awarding_time >= data_ini AND awarding_time
        < data_fim_exclusivo` (limite superior EXCLUSIVO), não
        `BETWEEN data_ini AND data_fim`. O motivo: `awarding_time` é
        TIMESTAMP (tem hora), e comparar com uma string de data pura
        (ex. '2025-05-11') faz o Redshift interpretar isso como
        '2025-05-11 00:00:00' — ou seja, um `BETWEEN` com limite superior
        assim exclui quase o dia inteiro do limite superior (só pega
        registros que caíram exatamente à meia-noite). Isso gerou um falso
        positivo de "perda de dados" (288.506 na origem vs. 269.464 no
        destino) quando na real os dados estavam 100% corretos — só a
        validação estava comparando errado. A origem já não tinha esse
        problema porque usa `toDate(AwardingTime) BETWEEN ...`, que trunca
        a hora antes de comparar.
        """
        auth_id = self.conection(cliente)
        database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                    if cliente == 'ZEROUM'
                    else MetabaseDatabase.ClickhousePartnerEnergiabet.value)
 
        data_ini_str = data_inicio_bloco.strftime('%Y-%m-%d')
        data_fim_str = data_fim_bloco.strftime('%Y-%m-%d')
 
        # Sem prefixo de schema -- ver nota em _perfil_diario_bonus.
        sql_origem = f"""
        SELECT COUNT(*) AS total FROM (
            SELECT Id
            FROM ClientBonus
            WHERE _peerdb_is_deleted = 0
              AND PartnerId = {partner_id}
              AND toDate(AwardingTime) BETWEEN '{data_ini_str}' AND '{data_fim_str}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY Id
                ORDER BY _peerdb_version DESC, _peerdb_synced_at DESC
            ) = 1
        )
        """
        csv_origem = self.extrai_csv_nativo(auth_id, database, sql_origem)
        df_origem = pd.read_csv(io.BytesIO(csv_origem))
        total_origem = int(df_origem['total'].iloc[0])
 
        # Limite superior EXCLUSIVO e um dia à frente, para cobrir o dia
        # data_fim_str por completo (24h), igual ao toDate() BETWEEN faz
        # do lado da origem. Ex.: data_fim_str='2025-05-11' -> filtro pega
        # tudo até '2025-05-12 00:00:00' (exclusivo).
        data_fim_exclusivo_str = (data_fim_bloco + timedelta(days=1)).strftime('%Y-%m-%d')
 
        ConnectionDB.conecta(DB, cliente)
        resultado_destino = ConnectionDB.recupera_dados(
            'inplay.fact_user_bonus',
            'COUNT(*)',
            f"WHERE partner_id = {partner_id} AND awarding_time >= '{data_ini_str}' AND awarding_time < '{data_fim_exclusivo_str}'"
        )
        total_destino = resultado_destino[0][0]
 
        if total_origem != total_destino:
            self.logger.error(
                f"[BACKFILL BÔNUS] VALIDAÇÃO FALHOU no bloco {data_ini_str}-{data_fim_str}: "
                f"origem={total_origem}, destino={total_destino} (diferença={total_origem - total_destino})"
            )
            raise Exception(
                f"Validação de contagem falhou no bloco {data_ini_str}-{data_fim_str}: "
                f"origem={total_origem} x destino={total_destino}"
            )
 
        self.logger.info(
            f"[BACKFILL BÔNUS] Bloco {data_ini_str}-{data_fim_str} validado OK "
            f"({total_origem} linhas em ambos os lados)"
        )

    @staticmethod
    def _sql_trigger_ref_backfill(data_inicial, data_final, campo_filtro="AwardingTime", partner_id=180):
        # Sem prefixo de schema -- ver nota em _perfil_diario_bonus.
        return f"""
        WITH bonus_dedup AS
        (
            SELECT Id, TriggerId, RefClientId, _peerdb_synced_at
            FROM ClientBonus
            WHERE _peerdb_is_deleted = 0
            AND PartnerId = {partner_id}
            AND {campo_filtro} >= '{data_inicial.replace("T", " ")}' AND {campo_filtro} < '{data_final.replace("T", " ")}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY Id
                ORDER BY _peerdb_version DESC, _peerdb_synced_at DESC
            ) = 1
        )
        SELECT
            Id          AS id,
            TriggerId   AS trigger_id,
            RefClientId AS ref_client_id
        FROM bonus_dedup;
        """

    def backfill_trigger_ref_client(self, cliente, data_inicio_historico, data_corte,
                                    limite_linhas_por_janela=900000, partner_id=None):
        """
        Backfill pontual de trigger_id/ref_client_id em inplay.fact_user_bonus,
        para o período JÁ carregado antes da inclusão dessas 2 colunas em
        _sql_fact_user_bonus. Reaproveita o perfil de volume/janelas já usado
        em processa_bonus_backfill -- mesma fonte (ClientBonus), então o
        dimensionamento das janelas é idêntico.

        Roda UPDATE direto (não merge/insert): os Ids já existem em
        fact_user_bonus, só falta popular as 2 colunas novas.
        """
        data_atual = datetime.fromisoformat(data_inicio_historico)
        data_corte_dt = datetime.fromisoformat(data_corte)

        if partner_id is None:
            if cliente == 'ZEROUM':
                partner_id = 180
            elif cliente == 'ENERGIABET':
                partner_id = 181
            else:
                raise Exception(
                    "backfill_trigger_ref_client('%s', ...): partner_id não "
                    "informado e não há default conhecido para esse cliente. "
                    "Chame novamente com partner_id=<valor confirmado>." % cliente
                )

        self.logger.info(
            f"[BACKFILL trigger_id/ref_client_id] Calculando perfil de volume "
            f"({data_atual} a {data_corte_dt})..."
        )
        perfil = self._perfil_diario_bonus(cliente, data_atual, data_corte_dt, partner_id=partner_id)
        janelas_data = self._monta_janelas_por_volume(perfil, limite_linhas_por_janela)
        janelas = [
            (datetime.combine(ini, datetime.min.time()),
            datetime.combine(fim, datetime.min.time()) + timedelta(days=1))
            for ini, fim in janelas_data
        ]

        auth_id = self.conection(cliente)
        database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                    if cliente == 'ZEROUM'
                    else MetabaseDatabase.ClickhousePartnerEnergiabet.value)

        total = len(janelas)
        for i, (ini, fim) in enumerate(janelas, start=1):
            self.logger.info(
                f"[BACKFILL trigger_id/ref_client_id] Janela {i}/{total}: "
                f"{ini.strftime('%Y-%m-%d')} -> {fim.strftime('%Y-%m-%d')}"
            )
            sql = self._sql_trigger_ref_backfill(
                ini.strftime('%Y-%m-%dT%H:%M:%S'),
                fim.strftime('%Y-%m-%dT%H:%M:%S'),
                partner_id=partner_id
            )
            csv = self.extrai_csv_nativo(auth_id, database, sql)
            df = pd.read_csv(io.BytesIO(csv)).replace({np.nan: None})

            if df.empty:
                continue

            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.deleta_dados('inplay.stg_backfill_trigger_ref_bonus', "", self.logger)
            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.insere_dados_bulk('inplay.stg_backfill_trigger_ref_bonus', df, self.logger)

            ConnectionDB.conecta(DB, cliente)
            ConnectionDB.executa_dml(
                """
                UPDATE inplay.fact_user_bonus fub
                SET trigger_id    = stg.trigger_id,
                    ref_client_id = stg.ref_client_id
                FROM inplay.stg_backfill_trigger_ref_bonus stg
                WHERE fub.id = stg.id
                """,
                self.logger
            )
            self.logger.info(f"[BACKFILL trigger_id/ref_client_id] Janela {i}/{total}: {len(df)} linhas atualizadas")

        self.logger.info("[BACKFILL trigger_id/ref_client_id] Concluído.")

    def valida_historico_bonus_total(self, cliente, data_inicio_historico, data_corte, partner_id=180):
        """
        Confere total geral (origem x destino) para todo o período do backfill.
        Rápido, mas não localiza EM QUAL dia está a diferença, se houver.
        """
        auth_id = self.conection(cliente)
        database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                    if cliente == 'ZEROUM'
                    else MetabaseDatabase.ClickhousePartnerEnergiabet.value)

        data_ini_str = datetime.fromisoformat(data_inicio_historico).strftime('%Y-%m-%d')
        data_fim_str = datetime.fromisoformat(data_corte).strftime('%Y-%m-%d')

        # Sem prefixo de schema -- ver nota em _perfil_diario_bonus.
        sql_origem = f"""
        SELECT COUNT(*) AS total FROM (
            SELECT Id
            FROM ClientBonus
            WHERE _peerdb_is_deleted = 0
            AND PartnerId = {partner_id}
            AND toDate(AwardingTime) BETWEEN '{data_ini_str}' AND '{data_fim_str}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY Id ORDER BY _peerdb_version DESC, _peerdb_synced_at DESC
            ) = 1
        )
        """
        csv_origem = self.extrai_csv_nativo(auth_id, database, sql_origem)
        total_origem = int(pd.read_csv(io.BytesIO(csv_origem))['total'].iloc[0])

        ConnectionDB.conecta(DB, cliente)
        resultado_destino = ConnectionDB.recupera_dados(
            'inplay.fact_user_bonus', 'COUNT(*)',
            f"WHERE partner_id = {partner_id} "
            f"AND awarding_time >= '{data_ini_str}' AND awarding_time < '{data_fim_str}'::date + 1"
        )
        total_destino = resultado_destino[0][0]

        self.logger.info(
            f"[VALIDAÇÃO HISTÓRICO] origem={total_origem}, destino={total_destino}, "
            f"diferença={total_origem - total_destino}"
        )
        return total_origem, total_destino

    def valida_historico_bonus_detalhado(self, cliente, data_inicio_historico, data_corte, partner_id=180):
        """
        Relatório dia a dia: contagem E soma de bonus_prize/cost, origem x destino.
        Não levanta exceção -- retorna um DataFrame com as divergências para
        inspeção manual (diferente de _valida_bloco_bonus, que é fail-fast
        durante o backfill).
        """
        auth_id = self.conection(cliente)
        database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                    if cliente == 'ZEROUM'
                    else MetabaseDatabase.ClickhousePartnerEnergiabet.value)

        data_ini_str = datetime.fromisoformat(data_inicio_historico).strftime('%Y-%m-%d')
        data_fim_str = datetime.fromisoformat(data_corte).strftime('%Y-%m-%d')

        # Sem prefixo de schema -- ver nota em _perfil_diario_bonus.
        sql_origem = f"""
        WITH dedup AS (
            SELECT Id, AwardingTime, BonusPrize, Cost
            FROM ClientBonus
            WHERE _peerdb_is_deleted = 0
            AND PartnerId = {partner_id}
            AND toDate(AwardingTime) BETWEEN '{data_ini_str}' AND '{data_fim_str}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY Id ORDER BY _peerdb_version DESC, _peerdb_synced_at DESC
            ) = 1
        )
        SELECT
            toDate(AwardingTime) AS dia,
            COUNT(*) AS qtd,
            SUM(BonusPrize) AS soma_bonus_prize,
            SUM(Cost) AS soma_cost
        FROM dedup
        GROUP BY dia
        ORDER BY dia
        """
        csv_origem = self.extrai_csv_nativo(auth_id, database, sql_origem)
        df_origem = pd.read_csv(io.BytesIO(csv_origem))
        df_origem['dia'] = pd.to_datetime(df_origem['dia']).dt.date

        ConnectionDB.conecta(DB, cliente)
        resultado_destino = ConnectionDB.recupera_dados(
            'inplay.fact_user_bonus',
            "awarding_time::date AS dia, COUNT(*) AS qtd, SUM(bonus_prize) AS soma_bonus_prize, SUM(cost) AS soma_cost",
            f"WHERE partner_id = {partner_id} "
            f"AND awarding_time >= '{data_ini_str}' AND awarding_time < '{data_fim_str}'::date + 1 GROUP BY awarding_time::date"
        )
        df_destino = pd.DataFrame(resultado_destino, columns=['dia', 'qtd', 'soma_bonus_prize', 'soma_cost'])

        comparacao = df_origem.merge(
            df_destino, on='dia', how='outer', suffixes=('_origem', '_destino')
        ).fillna(0)

        comparacao['diff_qtd'] = comparacao['qtd_origem'] - comparacao['qtd_destino']
        comparacao['diff_bonus_prize'] = comparacao['soma_bonus_prize_origem'] - comparacao['soma_bonus_prize_destino']
        comparacao['diff_cost'] = comparacao['soma_cost_origem'] - comparacao['soma_cost_destino']

        divergencias = comparacao[
            (comparacao['diff_qtd'] != 0) |
            (comparacao['diff_bonus_prize'].abs() > 0.01) |
            (comparacao['diff_cost'].abs() > 0.01)
        ]

        self.logger.info(
            f"[VALIDAÇÃO DETALHADA] {len(comparacao)} dias comparados, "
            f"{len(divergencias)} com divergência"
        )
        return comparacao, divergencias

    def processa_bonus_backfill_awarding_nulo(self, cliente, data_inicio_historico, data_corte,
                                           limite_linhas_por_janela=900000, validar_blocos=True,
                                           partner_id=None):
        """
        Backfill suplementar: captura os ClientBonus com AwardingTime IS NULL,
        que o backfill principal (processa_bonus_backfill, filtrado por
        AwardingTime) NUNCA teria capturado -- uma linha com AwardingTime NULL
        não satisfaz nenhuma condição de intervalo (>=/< sempre avalia UNKNOWN
        para NULL).

        Achado real (2025): ~9,6M de registros (Status=6, majoritariamente
        BonusType 12/14/15 -- freebet/freespin/riskfree, que não passam por
        ativação explícita) mais um resíduo de ~36 registros de outros status,
        todos com AwardingTime NULL.

        Usa CreationTime (sempre preenchido) para windowing e extração --
        NÃO usa AwardingTime (é justamente o que falta) nem _peerdb_synced_at
        (poderia estar concentrado no mesmo timestamp de sync do CDC histórico,
        mesmo risco de "cargas em lote" já observado no Saldo Diário).

        modo=incremental do dia a dia NÃO precisa de ajuste -- já filtra por
        _peerdb_synced_at, que cobre esses registros normalmente a partir de
        quando essa rodada suplementar terminar.
        """
        data_atual = datetime.fromisoformat(data_inicio_historico)
        data_corte_dt = datetime.fromisoformat(data_corte)
        filtro_extra = "AND AwardingTime IS NULL"

        if partner_id is None:
            if cliente == 'ZEROUM':
                partner_id = 180
            elif cliente == 'ENERGIABET':
                partner_id = 181
            else:
                raise Exception(
                    "processa_bonus_backfill_awarding_nulo('%s', ...): partner_id "
                    "não informado e não há default conhecido para esse cliente. "
                    "Confirme o PartnerId correto e chame novamente com "
                    "partner_id=<valor confirmado>." % cliente
                )

        self.logger.info(
            "[BACKFILL BÔNUS - AwardingTime NULO] Calculando perfil de volume "
            f"por CreationTime ({data_atual} a {data_corte_dt})..."
        )
        perfil = self._perfil_diario_bonus_generico(
            cliente, data_atual, data_corte_dt,
            campo_data="CreationTime", filtro_extra=filtro_extra, partner_id=partner_id
        )
        janelas_data = self._monta_janelas_por_volume(perfil, limite_linhas_por_janela)
        janelas = [
            (datetime.combine(ini, datetime.min.time()),
            datetime.combine(fim, datetime.min.time()) + timedelta(days=1))
            for ini, fim in janelas_data
        ]
        total_linhas_perfil = sum(t for _, t in perfil)
        self.logger.info(
            f"[BACKFILL BÔNUS - AwardingTime NULO] Perfil: {len(perfil)} dias, "
            f"~{total_linhas_perfil} linhas (antes de dedup), {len(janelas)} janelas"
        )

        total_blocos_previsto = len(janelas)
        total_blocos = 0
        for data_ini_bloco, data_fim_bloco in janelas:
            total_blocos += 1
            self.logger.info(
                f"[BACKFILL BÔNUS - AwardingTime NULO] Bloco {total_blocos}/{total_blocos_previsto}: "
                f"{data_ini_bloco.strftime('%Y-%m-%d')} -> {data_fim_bloco.strftime('%Y-%m-%d')}"
            )

            MAX_TENTATIVAS_BLOCO = 3
            ERROS_TRANSITORIOS = (psycopg2.OperationalError, psycopg2.InterfaceError)

            for tentativa in range(1, MAX_TENTATIVAS_BLOCO + 1):
                try:
                    self.processa_bonus(
                        cliente,
                        modo=data_ini_bloco.strftime('%Y-%m-%dT%H:%M:%S'),
                        data_final=data_fim_bloco.strftime('%Y-%m-%dT%H:%M:%S'),
                        carregar_dimensoes=False,  # dim_bonus/bridge já carregadas
                        campo_filtro_override="CreationTime",
                        filtro_extra=filtro_extra,
                        partner_id=partner_id
                    )
                    break
                except ERROS_TRANSITORIOS as e:
                    if tentativa >= MAX_TENTATIVAS_BLOCO:
                        self.logger.error(
                            f"[BACKFILL BÔNUS - AwardingTime NULO] Bloco "
                            f"{data_ini_bloco.strftime('%Y-%m-%d')} falhou "
                            f"{MAX_TENTATIVAS_BLOCO}x. Para retomar, chame "
                            f"processa_bonus_backfill_awarding_nulo(cliente='{cliente}', "
                            f"data_inicio_historico='{data_ini_bloco.strftime('%Y-%m-%dT%H:%M:%S')}', "
                            f"data_corte='{data_corte}'). Erro: {e}"
                        )
                        raise
                    espera = 30 * tentativa
                    self.logger.warning(
                        f"[BACKFILL BÔNUS - AwardingTime NULO] Erro de conexão "
                        f"(tentativa {tentativa}/{MAX_TENTATIVAS_BLOCO}): {e}. "
                        f"Tentando de novo em {espera}s..."
                    )
                    time.sleep(espera)
                except Exception as e:
                    self.logger.error(
                        f"[BACKFILL BÔNUS - AwardingTime NULO] Falhou no bloco "
                        f"{data_ini_bloco.strftime('%Y-%m-%d')}. Para retomar, chame "
                        f"processa_bonus_backfill_awarding_nulo(cliente='{cliente}', "
                        f"data_inicio_historico='{data_ini_bloco.strftime('%Y-%m-%dT%H:%M:%S')}', "
                        f"data_corte='{data_corte}'). Erro: {e}"
                    )
                    raise

            if validar_blocos:
                self._valida_bloco_bonus_awarding_nulo(cliente, data_ini_bloco, data_fim_bloco - timedelta(days=1), partner_id=partner_id)

        self.logger.info(
            f"[BACKFILL BÔNUS - AwardingTime NULO] Concluído — {total_blocos} blocos, {cliente}"
        )

    def _valida_bloco_bonus_awarding_nulo(self, cliente, data_inicio_bloco, data_fim_bloco, partner_id=180):
        """
        Validação do backfill suplementar -- compara por CreationTime (não
        AwardingTime, que é NULL nesse universo) com filtro AwardingTime IS NULL
        dos dois lados.
        """
        auth_id = self.conection(cliente)
        database = (MetabaseDatabase.ClickhousePartnerZeroum.value
                    if cliente == 'ZEROUM'
                    else MetabaseDatabase.ClickhousePartnerEnergiabet.value)

        data_ini_str = data_inicio_bloco.strftime('%Y-%m-%d')
        data_fim_str = data_fim_bloco.strftime('%Y-%m-%d')

        # Sem prefixo de schema -- ver nota em _perfil_diario_bonus.
        sql_origem = f"""
        SELECT COUNT(*) AS total FROM (
            SELECT * FROM (
                SELECT
                    Id,
                    argMax(AwardingTime, _peerdb_version) AS AwardingTime
                FROM ClientBonus
                WHERE _peerdb_is_deleted = 0
                AND PartnerId = {partner_id}
                AND toDate(CreationTime) BETWEEN '{data_ini_str}' AND '{data_fim_str}'
                GROUP BY Id
            )
            WHERE AwardingTime IS NULL
        )
        """
        csv_origem = self.extrai_csv_nativo(auth_id, database, sql_origem)
        total_origem = int(pd.read_csv(io.BytesIO(csv_origem))['total'].iloc[0])

        data_fim_exclusivo_str = (data_fim_bloco + timedelta(days=1)).strftime('%Y-%m-%d')

        ConnectionDB.conecta(DB, cliente)
        resultado_destino = ConnectionDB.recupera_dados(
            'inplay.fact_user_bonus', 'COUNT(*)',
            f"WHERE partner_id = {partner_id} "
            f"AND creation_time >= '{data_ini_str}' AND creation_time < '{data_fim_exclusivo_str}' "
            f"AND awarding_time IS NULL"
        )
        total_destino = resultado_destino[0][0]

        if total_origem != total_destino:
            self.logger.error(
                f"[BACKFILL BÔNUS - AwardingTime NULO] VALIDAÇÃO FALHOU no bloco "
                f"{data_ini_str}-{data_fim_str}: origem={total_origem}, destino={total_destino}"
            )
            raise Exception(
                f"Validação falhou no bloco {data_ini_str}-{data_fim_str}: "
                f"origem={total_origem} x destino={total_destino}"
            )

        self.logger.info(
            f"[BACKFILL BÔNUS - AwardingTime NULO] Bloco {data_ini_str}-{data_fim_str} validado OK "
            f"({total_origem} linhas)"
        )