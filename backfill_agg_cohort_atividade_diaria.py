"""
backfill_agg_cohort_atividade_diaria.py
=========================================
Carga histórica de inplay.agg_cohort_atividade_diaria.

ESCOPO: cliente ZEROUM apenas.

Diferente de agregacao_cohort_retencao.executar_agregacao_cohort_retencao(),
que só processa cohorts (data_ftd) de usuarios impactados na carga do dia E
dentro da janela de 90 dias, este script processa TODO o histórico de
usuarios com FTD já resolvido.

SEGURO PARA RODAR COM A CARGA INCREMENTAL JÁ ATIVA EM PRODUÇÃO: cada lote
mensal faz DELETE (só daquele intervalo de data_ftd) + INSERT, exatamente
como o processo incremental já faz por cohort -- não existe mais um
TRUNCATE geral no início. Isso significa que não importa a ordem em que este
script e a carga incremental de hora em hora escrevem: o último a escrever
um determinado data_ftd deixa o dado correto, sem duplicar nem apagar dado
de outro mês por engano. Não é necessário pausar o agendamento para rodar
isto.

RODA EM LOTES MENSAIS (não tudo de uma vez) porque fact_user_atividade_diaria
tem ~30 milhões de linhas -- um único INSERT cobrindo todo o histórico
arriscaria estourar tempo/memória num cluster pequeno. Cada mês é processado
e commitado separadamente, com log de progresso.

Uso:
    python backfill_agg_cohort_atividade_diaria.py            # todos os meses
    python backfill_agg_cohort_atividade_diaria.py --resume   # pula meses que
                                                                # já têm dado
                                                                # (só ganho de
                                                                # tempo, não é
                                                                # mais necessário
                                                                # por segurança)
"""

import argparse
import logging
import pandas as pd
from datetime import date
from logging.handlers import RotatingFileHandler
from database import ConnectionDB
from config import DB

CLIENTE = 'ZEROUM'


def _primeiro_dia_mes_seguinte(d: date) -> date:
    """Retorna o dia 1 do mês seguinte ao de `d`, sem depender de libs externas."""
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


SQL_MIN_MAX_DATA_FTD = """
    SELECT
        MIN(data_ftd) AS data_min,
        MAX(data_ftd) AS data_max
    FROM (
        SELECT
            COALESCE(
                CASE
                    WHEN du.first_deposit_date::date = DATE '1970-01-01'
                    THEN am.primeira_data_deposito_fato
                    ELSE du.first_deposit_date::date
                END,
                DATE '1970-01-01'
            ) AS data_ftd
        FROM inplay.dim_usuario du
        LEFT JOIN inplay.agg_usuario_metricas am
               ON du.id = am.usuario
    ) x
    WHERE data_ftd <> DATE '1970-01-01'
"""

SQL_CHECK_MES_JA_CARREGADO = """
    SELECT COUNT(*) AS qtd
    FROM inplay.agg_cohort_atividade_diaria
    WHERE data_ftd >= DATE '{inicio}'
      AND data_ftd <  DATE '{fim}'
"""

# {inicio} e {fim} são strings 'YYYY-MM-DD' formatadas em Python -- não vêm
# de input do usuário, são geradas internamente a partir de um range de
# datas, então não há risco de SQL injection aqui.
SQL_DELETE_MES = """
    DELETE FROM inplay.agg_cohort_atividade_diaria
    WHERE data_ftd >= DATE '{inicio}'
      AND data_ftd <  DATE '{fim}'
"""

SQL_DELETE_MES_JANELA = """
    DELETE FROM inplay.agg_cohort_retencao_janela
    WHERE data_ftd >= DATE '{inicio}'
      AND data_ftd <  DATE '{fim}'
"""

SQL_BACKFILL_COHORT_ATIVIDADE_MES = """
    INSERT INTO inplay.agg_cohort_atividade_diaria (
        data_ftd, dias_desde_ftd, usuarios_cohort, usuarios_ativos, ggr_dia, updated_at
    )
    WITH usuario_ftd_resolvido AS (
        -- mesma resolução usada no processo incremental e na
        -- vw_dim_usuario_v2: prioriza first_deposit_date de origem
        -- (dim_usuario); só cai para o valor calculado da fato
        -- (agg_usuario_metricas) quando o campo de origem está no
        -- sentinela 1970-01-01.
        SELECT
            du.id AS usuario,
            COALESCE(
                CASE
                    WHEN du.first_deposit_date::date = DATE '1970-01-01'
                    THEN am.primeira_data_deposito_fato
                    ELSE du.first_deposit_date::date
                END,
                DATE '1970-01-01'
            ) AS data_ftd
        FROM inplay.dim_usuario du
        LEFT JOIN inplay.agg_usuario_metricas am
               ON du.id = am.usuario
    ),
    cohort_base AS (
        -- só o mês deste lote
        SELECT usuario, data_ftd
        FROM usuario_ftd_resolvido
        WHERE data_ftd >= DATE '{inicio}'
          AND data_ftd <  DATE '{fim}'
    ),
    cohort_tamanho AS (
        SELECT
            data_ftd,
            COUNT(DISTINCT usuario) AS usuarios_cohort
        FROM cohort_base
        GROUP BY data_ftd
    ),
    atividade AS (
        -- escopo já reduzido: só usuarios cujo FTD caiu neste mês
        SELECT
            f.id_usuario,
            f.data,
            COALESCE(f.ggr, 0) AS ggr,
            f.usuario_ativo
        FROM inplay.fact_user_atividade_diaria f
        WHERE f.id_usuario IN (SELECT usuario FROM cohort_base)
          AND f.data >= DATE '{inicio}'
          AND f.data <  DATE '{fim}' + INTERVAL '90 days'
    ),
    retencao AS (
        SELECT
            cb.data_ftd,
            (a.data - cb.data_ftd) AS dias_desde_ftd,
            COUNT(DISTINCT CASE WHEN a.usuario_ativo THEN a.id_usuario END) AS usuarios_ativos,
            SUM(a.ggr) AS ggr_dia
        FROM cohort_base cb
        JOIN atividade a
          ON cb.usuario = a.id_usuario
         AND a.data >= cb.data_ftd
         AND (a.data - cb.data_ftd) BETWEEN 0 AND 90
        GROUP BY cb.data_ftd, (a.data - cb.data_ftd)
    )
    SELECT
        r.data_ftd,
        r.dias_desde_ftd,
        ct.usuarios_cohort,
        r.usuarios_ativos,
        r.ggr_dia,
        CURRENT_DATE AS updated_at
    FROM retencao r
    JOIN cohort_tamanho ct
      ON ct.data_ftd = r.data_ftd
"""

SQL_BACKFILL_COHORT_RETENCAO_JANELA_MES = """
    INSERT INTO inplay.agg_cohort_retencao_janela (
        data_ftd, janela_retencao, dias_janela, usuarios_cohort, usuarios_retidos, ltv_dia, updated_at
    )
    WITH usuario_ftd_resolvido AS (
        SELECT
            du.id AS usuario,
            COALESCE(
                CASE
                    WHEN du.first_deposit_date::date = DATE '1970-01-01'
                    THEN am.primeira_data_deposito_fato
                    ELSE du.first_deposit_date::date
                END,
                DATE '1970-01-01'
            ) AS data_ftd
        FROM inplay.dim_usuario du
        LEFT JOIN inplay.agg_usuario_metricas am
               ON du.id = am.usuario
    ),
    cohort_base AS (
        SELECT usuario, data_ftd
        FROM usuario_ftd_resolvido
        WHERE data_ftd >= DATE '{inicio}'
          AND data_ftd <  DATE '{fim}'
    ),
    cohort_tamanho AS (
        SELECT
            data_ftd,
            COUNT(DISTINCT usuario) AS usuarios_cohort
        FROM cohort_base
        GROUP BY data_ftd
    ),
    atividade AS (
        SELECT
            f.id_usuario,
            f.data,
            COALESCE(f.ggr, 0) AS ggr,
            f.usuario_ativo
        FROM inplay.fact_user_atividade_diaria f
        WHERE f.id_usuario IN (SELECT usuario FROM cohort_base)
          AND f.data >= DATE '{inicio}'
          AND f.data <  DATE '{fim}' + INTERVAL '90 days'
    ),
    retencao_janela AS (
        SELECT
            cb.data_ftd,
            CASE
                WHEN (a.data - cb.data_ftd) = 1 THEN 'D1'
                WHEN (a.data - cb.data_ftd) BETWEEN 2 AND 7  THEN 'D7'
                WHEN (a.data - cb.data_ftd) BETWEEN 8 AND 30 THEN 'D30'
                WHEN (a.data - cb.data_ftd) BETWEEN 31 AND 60 THEN 'D60'
                WHEN (a.data - cb.data_ftd) BETWEEN 61 AND 90 THEN 'D90'
            END AS janela_retencao,
            CASE
                WHEN (a.data - cb.data_ftd) = 1 THEN 1
                WHEN (a.data - cb.data_ftd) BETWEEN 2 AND 7  THEN 7
                WHEN (a.data - cb.data_ftd) BETWEEN 8 AND 30 THEN 30
                WHEN (a.data - cb.data_ftd) BETWEEN 31 AND 60 THEN 60
                WHEN (a.data - cb.data_ftd) BETWEEN 61 AND 90 THEN 90
            END AS dias_janela,
            COUNT(DISTINCT CASE WHEN a.usuario_ativo THEN a.id_usuario END) AS usuarios_retidos,
            SUM(a.ggr) AS ltv_dia
        FROM cohort_base cb
        JOIN atividade a
          ON cb.usuario = a.id_usuario
         AND a.data >= cb.data_ftd
         AND (a.data - cb.data_ftd) BETWEEN 1 AND 90
        GROUP BY
            cb.data_ftd,
            CASE
                WHEN (a.data - cb.data_ftd) = 1 THEN 'D1'
                WHEN (a.data - cb.data_ftd) BETWEEN 2 AND 7  THEN 'D7'
                WHEN (a.data - cb.data_ftd) BETWEEN 8 AND 30 THEN 'D30'
                WHEN (a.data - cb.data_ftd) BETWEEN 31 AND 60 THEN 'D60'
                WHEN (a.data - cb.data_ftd) BETWEEN 61 AND 90 THEN 'D90'
            END,
            CASE
                WHEN (a.data - cb.data_ftd) = 1 THEN 1
                WHEN (a.data - cb.data_ftd) BETWEEN 2 AND 7  THEN 7
                WHEN (a.data - cb.data_ftd) BETWEEN 8 AND 30 THEN 30
                WHEN (a.data - cb.data_ftd) BETWEEN 31 AND 60 THEN 60
                WHEN (a.data - cb.data_ftd) BETWEEN 61 AND 90 THEN 90
            END
    )
    SELECT
        r.data_ftd,
        r.janela_retencao,
        r.dias_janela,
        ct.usuarios_cohort,
        r.usuarios_retidos,
        r.ltv_dia,
        CURRENT_DATE AS updated_at
    FROM retencao_janela r
    JOIN cohort_tamanho ct
      ON ct.data_ftd = r.data_ftd
    WHERE r.janela_retencao IS NOT NULL
"""


def gerar_lotes_mensais(data_min, data_max):
    """Gera lista de (inicio, fim) em formato 'YYYY-MM-DD', um por mês,
    cobrindo do mês de data_min até o mês de data_max (fim exclusivo)."""
    lotes = []
    cursor = data_min.replace(day=1)
    limite = _primeiro_dia_mes_seguinte(data_max.replace(day=1))
    while cursor < limite:
        proximo = _primeiro_dia_mes_seguinte(cursor)
        lotes.append((cursor.strftime('%Y-%m-%d'), proximo.strftime('%Y-%m-%d')))
        cursor = proximo
    return lotes


def mes_ja_carregado(inicio, fim, logger):
    ConnectionDB.conecta(DB, CLIENTE)
    df = ConnectionDB.executa_script(
        SQL_CHECK_MES_JA_CARREGADO.format(inicio=inicio, fim=fim),
        logger
    )
    return int(df.iloc[0]['qtd']) > 0


def main():
    parser = argparse.ArgumentParser(description="Backfill agg_cohort_atividade_diaria (ZEROUM)")
    parser.add_argument("--resume", action="store_true",
                         help="Pula meses que já têm dado carregado (só ganho de tempo)")
    args = parser.parse_args()

    handler = RotatingFileHandler(
        'backfill_cohort_retencao.log',
        maxBytes=10 * 1024 * 1024,
        backupCount=3
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger('backfill_cohort')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logging.basicConfig(handlers=[handler], level=logging.INFO)

    try:
        logger.info(f"[BACKFILL] Iniciando (resume={args.resume})")

        confirm = input(
            "Isso vai recarregar (DELETE + INSERT, mês a mês) o histórico de "
            "inplay.agg_cohort_atividade_diaria (cliente ZEROUM). Seguro rodar "
            "com a carga incremental de produção ativa ao mesmo tempo. "
            "Confirma? (digite SIM): "
        )
        if confirm.strip().upper() != "SIM":
            logger.info("[BACKFILL] Cancelado pelo usuário")
            print("Cancelado.")
            return

        logger.info("[BACKFILL] Descobrindo intervalo de datas (min/max FTD)")
        ConnectionDB.conecta(DB, CLIENTE)
        df_range = ConnectionDB.executa_script(SQL_MIN_MAX_DATA_FTD, logger)
        data_min = df_range.iloc[0]['data_min']
        data_max = df_range.iloc[0]['data_max']

        if data_min is None or data_max is None or pd.isna(data_min) or pd.isna(data_max):
            logger.info("[BACKFILL] Nenhuma cohort encontrada (data_min/data_max nulos). Nada a fazer.")
            print("Nenhuma cohort encontrada. Nada a fazer.")
            return

        # normaliza para date puro (executa_script pode retornar Timestamp)
        data_min = pd.Timestamp(data_min).date()
        data_max = pd.Timestamp(data_max).date()

        lotes = gerar_lotes_mensais(data_min, data_max)
        total_lotes = len(lotes)
        logger.info(f"[BACKFILL] Intervalo: {data_min} até {data_max} -> {total_lotes} lotes mensais")
        print(f"Processando {total_lotes} lotes mensais, de {data_min} até {data_max}...")

        for i, (inicio, fim) in enumerate(lotes, start=1):
            if args.resume and mes_ja_carregado(inicio, fim, logger):
                logger.info(f"[BACKFILL] ({i}/{total_lotes}) {inicio} já carregado, pulando")
                print(f"  [{i}/{total_lotes}] {inicio} -- já carregado, pulando")
                continue

            logger.info(f"[BACKFILL] ({i}/{total_lotes}) Processando cohort {inicio} a {fim}")
            print(f"  [{i}/{total_lotes}] {inicio} -- processando...")

            # DELETE + INSERT do próprio mês -- não mexe em nenhum outro
            # data_ftd, então não conflita com o que a carga incremental
            # possa estar escrevendo em paralelo em outros meses
            ConnectionDB.conecta(DB, CLIENTE)
            ConnectionDB.executa_dml(
                SQL_DELETE_MES.format(inicio=inicio, fim=fim),
                logger
            )
            ConnectionDB.conecta(DB, CLIENTE)
            ConnectionDB.executa_dml(
                SQL_BACKFILL_COHORT_ATIVIDADE_MES.format(inicio=inicio, fim=fim),
                logger
            )

            # mesma coisa para a tabela de retenção por janela (D1/D7/D30/D60/D90)
            ConnectionDB.conecta(DB, CLIENTE)
            ConnectionDB.executa_dml(
                SQL_DELETE_MES_JANELA.format(inicio=inicio, fim=fim),
                logger
            )
            ConnectionDB.conecta(DB, CLIENTE)
            ConnectionDB.executa_dml(
                SQL_BACKFILL_COHORT_RETENCAO_JANELA_MES.format(inicio=inicio, fim=fim),
                logger
            )

            logger.info(f"[BACKFILL] ({i}/{total_lotes}) Concluído {inicio}")
            print(f"  [{i}/{total_lotes}] {inicio} -- OK")

        logger.info("[BACKFILL] Concluído com sucesso -- todos os lotes processados")
        print("\nBackfill concluído com sucesso. Próximo passo: rodar "
              "views_atualizadas_cohort_retencao_kpi.sql (se ainda não rodou) "
              "e conferir os números do BI.")

    except Exception as e:
        logger.error(f"[BACKFILL] Erro no backfill: {e}")
        print(f"\nErro no backfill: {e}")
        print("Rode novamente (com ou sem --resume) para continuar -- é "
              "seguro repetir meses já processados, cada um se sobrescreve "
              "sozinho.")
        raise


if __name__ == "__main__":
    main()
