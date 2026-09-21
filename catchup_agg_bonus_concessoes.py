"""
catchup_agg_bonus_concessoes.py
=================================
Recupera os pares client_id+bonus_id que ficaram atrasados em
inplay.agg_bonus_concessoes por causa do bug do NaN (ver
agregacao_bonus.py corrigido -- df_agg.replace({np.nan: None})).

Como o gatilho normal da agregação é o df_fact_bonus DAQUELA execução
específica (não uma releitura independente), simplesmente rodar
ENERGIABET_BONUS/ZEROUM_BONUS de novo não reprocessa pares já
sincronizados -- o cursor incremental (_peerdb_synced_at) já avançou.
Este script contorna isso: identifica os pares atrasados direto no
Redshift (fact_user_bonus x agg_bonus_concessoes) e chama
executar_agregacao_bonus_concessoes() manualmente para eles, sem
precisar de nenhuma extração nova do Metabase.

PRÉ-REQUISITO: aplicar o agregacao_bonus.py corrigido (com o fix do NaN)
antes de rodar este script -- senão o catch-up falha com o MESMO erro.

Uso (PowerShell):
    python catchup_agg_bonus_concessoes.py --cliente ENERGIABET
    python catchup_agg_bonus_concessoes.py --cliente ZEROUM --janela-dias 3
"""
import sys
import argparse
import logging
import pandas as pd
from logging.handlers import RotatingFileHandler

from database import ConnectionDB
from agregacao_bonus import executar_agregacao_bonus_concessoes
from config import DB


def monta_logger():
    handler = RotatingFileHandler('etl.log', maxBytes=10 * 1024 * 1024, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('catchup_agg_bonus')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        logger.addHandler(handler)
        logger.addHandler(logging.StreamHandler(sys.stdout))
    return logger


def main():
    parser = argparse.ArgumentParser(description="Catch-up de agg_bonus_concessoes")
    parser.add_argument("--cliente", required=True, choices=["ZEROUM", "ENERGIABET"])
    parser.add_argument("--janela-dias", type=int, default=2,
                         help="Quantos dias atrás olhar em fact_user_bonus.source_updated_at (default: 2)")
    args = parser.parse_args()

    logger = monta_logger()
    cliente = args.cliente

    sql_pares_atrasados = f"""
        WITH pares_recentes_fact AS (
            SELECT client_id, bonus_id, max(source_updated_at) AS ultima_no_fact
            FROM inplay.fact_user_bonus
            WHERE source_updated_at >= dateadd(day, -{args.janela_dias}, getdate())
            GROUP BY client_id, bonus_id
        )
        SELECT f.client_id, f.bonus_id
        FROM pares_recentes_fact f
        LEFT JOIN inplay.agg_bonus_concessoes a
            ON a.client_id = f.client_id AND a.bonus_id = f.bonus_id
        WHERE a.client_id IS NULL OR a.ultima_atualizacao < f.ultima_no_fact
    """

    print(f"Buscando pares atrasados em {cliente} (janela: últimos {args.janela_dias} dias)...")
    ConnectionDB.conecta(DB, cliente)
    df_pares_atrasados = ConnectionDB.executa_script(sql_pares_atrasados, logger)

    if df_pares_atrasados.empty:
        print("Nenhum par atrasado encontrado. Nada a fazer.")
        sys.exit(0)

    print(f"{len(df_pares_atrasados)} pares atrasados encontrados.")
    print(df_pares_atrasados.head(10).to_string())

    confirmacao = input(f"\nRecalcular esses {len(df_pares_atrasados)} pares agora? [s/N] ")
    if confirmacao.strip().lower() != 's':
        print("Cancelado.")
        sys.exit(0)

    # executar_agregacao_bonus_concessoes só precisa de client_id/bonus_id
    # no DataFrame recebido -- os demais campos de df_fact_bonus não são
    # usados dentro da função (ela recalcula tudo direto de
    # fact_user_bonus via o JOIN com stg_pares_bonus_impactados)
    executar_agregacao_bonus_concessoes(df_pares_atrasados, cliente, DB, logger)

    print("\nCatch-up concluído. Confira novamente com diagnostico_atraso_agg_bonus.sql "
          "-- a query deve voltar vazia agora.")


if __name__ == "__main__":
    main()
