import requests
import pandas as pd
import numpy as np
import io
from datetime import datetime, timedelta, date
from config import API_AUTH, API_USER_ZEROUM, API_PASS_ZEROUM, API_USER_ENERGIABET, API_PASS_ENERGIABET, API_ROTA_CSV 
from enums import MetabaseTable, MetabaseDatabase, MetabaseCard
from database import ConnectionDB
from config import DB
import logging
from logging.handlers import RotatingFileHandler

class ConsumeAPI:
    def __init__(self, cliente):
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
        if cliente == 'ZEROUM':
            self.principal_zeroum()
        elif cliente == 'ENERGIABET':
            self.principal_energiabet()
        else:
            self.logger.error("Cliente inválido")

    def principal_zeroum(self):
        self.logger.info("Iniciando ETL")
        self.logger.info("Fazendo a autenticação no Metabase")
        auth_id = self.conection('ZEROUM')
        #auth_id = "2a30d8ef-dcfd-4753-bb87-b06dbefdd1d8"
        ConnectionDB.conecta(DB, 'ZEROUM')
        self.logger.info("Recuperando a data base para consulta")
        data_importacao = ConnectionDB.recupera_dados('inplay.fact_user_daily_new', 'max(updated_at) as updated_at', '')
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
        data_inicial = data_base - timedelta(hours=1)
        data_inicial = data_inicial.strftime('%Y-%m-%d')
        #data_final = data_final.strftime('%Y-%m-%d')

        print('datas')
        print(data_inicial)

        self.logger.info("Deletando dados das stages")
        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.deleta_dados('inplay.stg_fact_user_daily_new', "", self.logger)
        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.deleta_dados('inplay.stg_fact_user_daily_sport_new', "", self.logger)
        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.deleta_dados('inplay.stg_game', "", self.logger)
        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.deleta_dados('inplay.stg_fact_deposits_withdraws_summarized', "", self.logger)
        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.deleta_dados('inplay.stg_fact_casino_games_hourly', "", self.logger)
        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.deleta_dados('inplay.stg_usuario', "", self.logger)

        #data_inicial = "2025-12-26"
        #data_final = "2025-12-31"
        self.logger.info("Iniciando as consultas ao metabase e inserção dos dados")
        df_stg = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_Stage.value, data_inicial, 0)
        df_deposito = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_Deposito.value, data_inicial, 0)
        df_saque = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_Saque.value, data_inicial, 0)
        df_primeira_aposta = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_PrimeiraAposta.value, 0, 0)
        df_ultima_aposta = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_UltimaAposta.value, 0, 0)
        df_bonus = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_Bonus.value, data_inicial, 0)
        df_bonus_ativado = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_BonusAtivado.value, data_inicial, 0)
        
        #print("df_primeira_aposta")
        #print(df_primeira_aposta)
        df_stg = df_stg.merge(df_deposito[['usuario', 'data_referencia', 'deposit_amount', 'deposit_quantity', 'deposit_pending_amount', 'deposit_pending_quantity', 'ftd_date']], 
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
                                      how='left')
        
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

        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily_new', df_stg, self.logger)
        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily_new', 'inplay.fact_user_daily_new', df_stg, ['usuario', 'data_referencia'], self.logger)
        
        df_stg_sport = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_StageSport.value, data_inicial, 0)

        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily_sport_new', df_stg_sport, self.logger)
        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily_sport_new', 'inplay.fact_user_daily_sport_new', df_stg_sport, ['usuario', 'data_referencia'], self.logger)

        df_fact_dep_saq_dias = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_DepositoSaqueDias.value, data_inicial, 0)
        df_fact_dep_saq_dias_ajust = df_fact_dep_saq_dias[['date', 'hora', 'tipo', 'qtd', 'amount', 'updated_at', 'import_date']]

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
                                       'last_withdraw_date','last_withdraw_amount']]
        df_usuario_totalizador = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_UsuariosTotalizador.value, 0, 0)
        df_usuario_totalizador_ajust = df_usuario_totalizador[['id','total_quantity_deposit','total_amount_deposit','total_quantity_withdraw','total_amount_withdraw']]
        df_usuario_totalizador_bet = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerZeroum.value, MetabaseCard.ZeroUm_UsuariosTotalizadorBet.value, 0, 0)
        df_usuario_totalizador_bet_ajust = df_usuario_totalizador_bet[['id','total_quantity_bet','total_amount_bet','total_quantity_win','total_amount_win', 'total_quantity_bonus',
                                                                       'total_amount_bonus']]

        #nome_arquivo = f"df_usuario_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        #df_usuario.to_csv(nome_arquivo, index=False, encoding="utf-8")
        #nome_arquivo = f"df_usuario_totalizador_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        #df_usuario_totalizador.to_csv(nome_arquivo, index=False, encoding="utf-8")

        df_dim_usuario = df_usuario_ajust.merge(df_usuario_totalizador_ajust, 
                                      on=['id'],
                                      how='outer')
        df_dim_usuario = df_usuario_ajust.merge(df_usuario_totalizador_bet_ajust, 
                                      on=['id'],
                                      how='outer')
        
        df_dim_usuario = df_dim_usuario.replace({np.nan: None})

        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.insere_dados_bulk('inplay.stg_usuario', df_dim_usuario, self.logger)
        ConnectionDB.conecta(DB, 'ZEROUM')
        ConnectionDB.mergeia_dados('inplay.stg_usuario', 'inplay.dim_usuario', df_dim_usuario, ['id'], self.logger)
        
    def principal_energiabet(self):
        self.logger.info("Iniciando ETL")
        self.logger.info("Fazendo a autenticação no Metabase")
        auth_id = self.conection('ENERGIABET')
        #auth_id = "2a30d8ef-dcfd-4753-bb87-b06dbefdd1d8"
        """ConnectionDB.conecta(DB, 'ENERGIABET')
        self.logger.info("Recuperando a data base para consulta")
        data_importacao = ConnectionDB.recupera_dados('inplay.fact_user_daily_new', 'max(updated_at) as updated_at', '')
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
        data_inicial = data_base - timedelta(hours=1)
        data_inicial = data_inicial.strftime('%Y-%m-%d')
        #data_final = data_final.strftime('%Y-%m-%d')

        print('datas')
        print(data_inicial)
"""
        self.logger.info("Deletando dados das stages")
        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.deleta_dados('inplay.stg_fact_user_daily_new', "", self.logger)
        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.deleta_dados('inplay.stg_fact_user_daily_sport_new', "", self.logger)
        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.deleta_dados('inplay.stg_game', "", self.logger)
        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.deleta_dados('inplay.stg_fact_deposits_withdraws_summarized', "", self.logger)
        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.deleta_dados('inplay.stg_fact_casino_games_hourly', "", self.logger)
        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.deleta_dados('inplay.stg_usuario', "", self.logger)

        data_inicial = "2025-05-11"
        data_final = "2025-05-16"
        self.logger.info("Iniciando as consultas ao metabase e inserção dos dados")
        df_stg = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_Stage.value, data_inicial, data_final)
        df_deposito = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_Deposito.value, data_inicial, data_final)
        df_saque = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_Saque.value, data_inicial, data_final)
        df_primeira_aposta = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_PrimeiraAposta.value, 0, 0)
        df_ultima_aposta = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_UltimaAposta.value, 0, 0)
        df_bonus = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_Bonus.value, data_inicial, data_final)
        df_bonus_ativado = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_BonusAtivado.value, data_inicial, data_final)
        
        #print("df_primeira_aposta")
        #print(df_primeira_aposta)
        df_stg = df_stg.merge(df_deposito[['usuario', 'data_referencia', 'deposit_amount', 'deposit_quantity', 'deposit_pending_amount', 'deposit_pending_quantity', 'ftd_date']], 
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
                                      how='left')
        
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

        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily_new', df_stg, self.logger)
        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily_new', 'inplay.fact_user_daily_new', df_stg, ['usuario', 'data_referencia'], self.logger)
        
        df_stg_sport = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_StageSport.value, data_inicial, data_final)

        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.insere_dados_bulk('inplay.stg_fact_user_daily_sport_new', df_stg_sport, self.logger)
        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.mergeia_dados('inplay.stg_fact_user_daily_sport_new', 'inplay.fact_user_daily_sport_new', df_stg_sport, ['usuario', 'data_referencia'], self.logger)

        df_fact_dep_saq_dias = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_DepositoSaqueDias.value, data_inicial, data_final)
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

        df_fact_cassino_game_hourly = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_ApostasJogosHora.value, data_inicial, data_final)
        df_fact_cassino_game_hourly_ajust = df_fact_cassino_game_hourly[['reference', 'game_id', 'with_bonus', 'bet_amount', 'win_amount', 'bet_qty', 'win_qty', 'ggr_amount', 'updated_at', 'data_importacao']]

        #nome_arquivo = f"df_game_hourly_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        #df_fact_cassino_game_hourly_ajust.to_csv(nome_arquivo, index=False, encoding="utf-8")

        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.insere_dados_bulk('inplay.stg_fact_casino_games_hourly', df_fact_cassino_game_hourly_ajust, self.logger)
        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.mergeia_dados('inplay.stg_fact_casino_games_hourly', 'inplay.fact_casino_games_hourly', df_fact_cassino_game_hourly_ajust, ['reference', 'game_id'], self.logger)

        df_usuario = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_Usuarios.value, data_inicial, data_final)
        df_usuario_ajust = df_usuario[['id','core_account_status','core_user_language','core_wallet_currency','birth_date','first_name','email_verified','email',
                                       'mobile_number_verified','mobile_number','refer_id','sms_allowed','email_allowed','city','state','updated_at','registration_date','import_date',
                                       'first_deposit_date','first_deposit_amount','first_withdraw_date','first_withdraw_amount','last_deposit_date','last_deposit_amount',
                                       'last_withdraw_date','last_withdraw_amount']]
        df_usuario_totalizador = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_UsuariosTotalizador.value, 0, 0)
        df_usuario_totalizador_ajust = df_usuario_totalizador[['id','total_quantity_deposit','total_amount_deposit','total_quantity_withdraw','total_amount_withdraw']]
        df_usuario_totalizador_bet = self.extrai_dados_card(auth_id, MetabaseDatabase.ClickhousePartnerEnergiabet.value, MetabaseCard.EnergiaBet_UsuariosTotalizadorBet.value, 0, 0)
        df_usuario_totalizador_bet_ajust = df_usuario_totalizador_bet[['id','total_quantity_bet','total_amount_bet','total_quantity_win','total_amount_win', 'total_quantity_bonus',
                                                                       'total_amount_bonus']]

        #nome_arquivo = f"df_usuario_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        #df_usuario.to_csv(nome_arquivo, index=False, encoding="utf-8")
        #nome_arquivo = f"df_usuario_totalizador_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        #df_usuario_totalizador.to_csv(nome_arquivo, index=False, encoding="utf-8")

        df_dim_usuario = df_usuario_ajust.merge(df_usuario_totalizador_ajust, 
                                      on=['id'],
                                      how='outer')
        df_dim_usuario = df_usuario_ajust.merge(df_usuario_totalizador_bet_ajust, 
                                      on=['id'],
                                      how='outer')
        
        df_dim_usuario = df_dim_usuario.replace({np.nan: None})

        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.insere_dados_bulk('inplay.stg_usuario', df_dim_usuario, self.logger)
        ConnectionDB.conecta(DB, 'ENERGIABET')
        ConnectionDB.mergeia_dados('inplay.stg_usuario', 'inplay.dim_usuario', df_dim_usuario, ['id'], self.logger)
        
    
    def conection(self, cliente):
        id = ''
        try:
            self.auth_url = API_AUTH
            self.body = {
                "username": API_USER_ZEROUM if cliente == 'ZEROUM' else API_USER_ENERGIABET,
                "password": API_PASS_ZEROUM if cliente == 'ZEROUM' else API_PASS_ENERGIABET
            }

            response = requests.post(self.auth_url, json=self.body)

            print("requisição realizada")
            id = response.json()['id']
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erro ao conectar na API: {e}")
        return id
        
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
            
            print('body')
            print(self.body)
            if filter:
                self.body["query"]["query"]["filter"] = filter

            response = requests.post(self.rota, headers=self.header, json=self.body)

            print("requisição realizada")
            #print(response)
            #print(response.content)

            resultado_csv = response.content
            #df = pd.DataFrame(resultado_csv)
            return resultado_csv
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erro ao conectar na API: {e}")
        
    def extrai_dados_bet(self, auth_id, data_inicial, data_final):
        try:
            id_database = MetabaseDatabase.ClickhousePartnerZeroum.value
            id_table = MetabaseTable.Bet.value
            csv = self.extrai_csv(auth_id, id_database, id_table, "", ["and",[">=",["field",25456,None], data_inicial],
            ["<",["field",25456,None], data_final]])
            df = pd.read_csv(io.BytesIO(csv))
            #print('dataframe:')
            #print(df)
            #print(df['LastUpdateTime'])
            #df['LastUpdateTime'] = pd.to_datetime(df['LastUpdateTime'])
            #df['LastUpdateTime'] = df['LastUpdateTime'].dt.date
            nome_arquivo = f"bet_dados_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            df.to_csv(nome_arquivo, index=False, encoding="utf-8")
            return df
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erro ao extrair os dados da tabela Bet: {e}")
        
    def extrai_dados_deposito(self, auth_id, data_inicial, data_final):
        try:
            id_database = MetabaseDatabase.ClickhousePartnerZeroum.value
            id_table = MetabaseTable.PaymentRequest.value
            csv = self.extrai_csv(auth_id, id_database, id_table, "", ["and",[">=",["field",25843,None], data_inicial],
            ["<",["field",25843,None], data_final],
            ["=", ["field", 25819,None], 1]])
            df = pd.read_csv(io.BytesIO(csv))
            #df['LastUpdateTime'] = pd.to_datetime(df['LastUpdateTime'])
            #df['LastUpdateTime'] = df['LastUpdateTime'].dt.date
            nome_arquivo = f"deposito_dados_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            df.to_csv(nome_arquivo, index=False, encoding="utf-8")
            return df
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erro ao extrair os dados da tabela Payment - Deposito: {e}")
        
    def extrai_dados_saque(self, auth_id, data_inicial, data_final):
        try:
            id_database = MetabaseDatabase.ClickhousePartnerZeroum.value
            id_table = MetabaseTable.PaymentRequest.value
            csv = self.extrai_csv(auth_id, id_database, id_table, "", ["and",[">=",["field",25843,None], data_inicial],
            ["<",["field",25843,None], data_final],
            ["=", ["field", 25819,None], 2]])
            df = pd.read_csv(io.BytesIO(csv))
            #df['LastUpdateTime'] = pd.to_datetime(df['LastUpdateTime'])
            #df['LastUpdateTime'] = df['LastUpdateTime'].dt.date

            nome_arquivo = f"saque_dados_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            df.to_csv(nome_arquivo, index=False, encoding="utf-8")
            return df
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erro ao extrair os dados da tabela Payment: {e}")
        
    def extrai_dados_cliente(self, auth_id, data_inicial, data_final):
        try:
            id_database = MetabaseDatabase.ClickhousePartnerZeroum.value
            id_table = MetabaseTable.Client.value
            csv = self.extrai_csv(auth_id, id_database, id_table, "", ["and",[">=",["field",25601,None], data_inicial],
            ["<",["field",25601,None], data_final]])
            df = pd.read_csv(io.BytesIO(csv))
            #df['LastUpdateTime'] = pd.to_datetime(df['LastUpdateTime'])
            #df['LastUpdateTime'] = df['LastUpdateTime'].dt.date
            
            nome_arquivo = f"cliente_dados_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            df.to_csv(nome_arquivo, index=False, encoding="utf-8")
            return df
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erro ao extrair os dados da tabela Cliente: {e}")
        
    def extrai_dados_cliente_bonus(self, auth_id, data_inicial, data_final):
        try:
            id_database = MetabaseDatabase.ClickhousePartnerZeroum.value
            id_table = MetabaseTable.ClientBonus.value
            csv = self.extrai_csv(auth_id, id_database, id_table, "", ["and",[">=",["field",25663,None], data_inicial],
            ["<",["field",25663,None], data_final]])
            df = pd.read_csv(io.BytesIO(csv))
            #df['CreationTime'] = pd.to_datetime(df['CreationTime'])
            #df['CreationTime'] = df['CreationTime'].dt.date
            df_retorno = df[['ClientId', 'CreationTime', 'FinalAmount']]
            df_retorno = df_retorno.rename(columns={'CreationTime': 'BonusDate', 'FinalAmount': 'BonusAmount'})
            nome_arquivo = f"cliente_bonus_dados_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            df_retorno.to_csv(nome_arquivo, index=False, encoding="utf-8")
            return df_retorno
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erro ao extrair os dados da tabela Bonus Cliente: {e}")
        
    def extrai_dados_sportsbook_bet(self, auth_id, data_inicial, data_final):
        try:
            id_database = MetabaseDatabase.ClickhousePartnerZeroum.value
            id_table = MetabaseTable.SportsbookBet.value
            csv = self.extrai_csv(auth_id, id_database, id_table, "", ["and",[">=",["field",25888,None], data_inicial],
            ["<",["field",25888,None], data_final]])
            df = pd.read_csv(io.BytesIO(csv))
            #df['LastUpdateTime'] = pd.to_datetime(df['LastUpdateTime'])
            #df['LastUpdateTime'] = df['LastUpdateTime'].dt.date
            df_retorno = df[['ID', 'ClientId', 'LastUpdateTime', 'ProductId', 'BetTime', 'BetAmount', 'WinAmount', 'Ggr', 'BetBonusAmount', 'WinBonusAmount']]
            #nome_arquivo = f"bet_dados_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            #df_retorno.to_csv(nome_arquivo, index=False, encoding="utf-8")
            return df_retorno
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erro ao extrair os dados da tabela Sportsbook Bet: {e}")
        
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
                csv = self.extrai_csv(auth_id, id_database, 0, id_card, [">",["field","updated_at",{"base-type":"type/Date"}], data_inicial])
            else:
                csv = self.extrai_csv(auth_id, id_database, 0, id_card, ["between",["field","updated_at",{"base-type":"type/Date"}], data_inicial, data_final])
            df = pd.read_csv(io.BytesIO(csv))
            return df
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erro ao extrair os dados da consulta {id_card}")
            raise Exception(f"Erro ao extrair os dados do card {id}: {e}")