"""
agregacao_dim_usuario.py
========================
Módulo responsável por alimentar as tabelas físicas que substituem o processamento
pesado das views vw_dim_usuario e vw_fato_usuarios_diario.

Não altera nenhum objeto existente em produção.
Chamado ao final de principal_zeroum() e principal_energiabet() em consume_api.py.

Compatível com Redshift 1.0.331549 (PostgreSQL 8.0.2):
- Sem IS DISTINCT FROM (não suportado nessa versão)
- MERGE padrão para tabelas onde o SET não precisa do valor atual do target
- UPDATE + INSERT separados para agg_usuario_reativacao (SET usa GREATEST entre
  valor antigo e novo, o que exige referenciar a própria tabela alvo no SET)
- DISTSTYLE ALL em stg_usuarios_impactados para semi-join co-located em todos os nós
- DISTKEY(usuario) em todas as tabelas agg_* para joins locais com dim_usuario e fact
"""

import pandas as pd
from database import ConnectionDB


# =============================================================================
# SQL - PASSO 1: FATO DIÁRIA FÍSICA
# Replica vw_fato_usuarios_diario, escopada aos usuarios impactados na carga.
# =============================================================================

SQL_STG_FATO_DIARIA = """
    INSERT INTO inplay.stg_fact_user_atividade_diaria (
        id_usuario, valor_deposito, qtd_deposito,
        valor_retirada, qtd_retirada,
        data, importacao,
        valor_aposta, qtd_aposta,
        valor_bonus, qtd_bonus,
        ggr,
        realizou_aposta, realizou_deposito, realizou_saque,
        usou_bonus, usuario_ativo
    )
    SELECT
        f.usuario,
        COALESCE(f.deposit_amount, 0),
        COALESCE(f.deposit_quantity, 0),
        COALESCE(f.withdraw_amount, 0),
        COALESCE(f.withdraw_quantity, 0),
        f.data_referencia,
        f.data_importacao,
        COALESCE(f.cassino_bet_amount, 0),
        COALESCE(f.cassino_bet_quantity, 0),
        COALESCE(f.bonus_amount, 0),
        COALESCE(f.bonus_quantity, 0),
        COALESCE(f.cassino_ggr, 0),
        CASE WHEN COALESCE(f.cassino_bet_amount,0) > 0
                  OR COALESCE(f.cassino_bet_quantity,0) > 0
             THEN TRUE ELSE FALSE END,
        CASE WHEN COALESCE(f.deposit_amount,0) > 0
                  OR COALESCE(f.deposit_quantity,0) > 0
             THEN TRUE ELSE FALSE END,
        CASE WHEN COALESCE(f.withdraw_amount,0) > 0
                  OR COALESCE(f.withdraw_quantity,0) > 0
             THEN TRUE ELSE FALSE END,
        CASE WHEN COALESCE(f.bonus_amount,0) > 0
                  OR COALESCE(f.bonus_quantity,0) > 0
             THEN TRUE ELSE FALSE END,
        CASE WHEN COALESCE(f.cassino_bet_amount,0) > 0
                  OR COALESCE(f.cassino_bet_quantity,0) > 0
                  OR COALESCE(f.deposit_amount,0) > 0
                  OR COALESCE(f.deposit_quantity,0) > 0
                  OR COALESCE(f.withdraw_amount,0) > 0
                  OR COALESCE(f.withdraw_quantity,0) > 0
             THEN TRUE ELSE FALSE END
    FROM inplay.fact_user_daily f
    WHERE f.data_referencia IS NOT NULL
      AND (
            COALESCE(f.cassino_bet_amount,0) > 0
            OR COALESCE(f.cassino_bet_quantity,0) > 0
            OR COALESCE(f.deposit_amount,0) > 0
            OR COALESCE(f.deposit_quantity,0) > 0
            OR COALESCE(f.withdraw_amount,0) > 0
            OR COALESCE(f.withdraw_quantity,0) > 0
          )
      AND f.usuario IN (SELECT usuario FROM inplay.stg_usuarios_impactados)
"""

SQL_MERGE_FATO_DIARIA = """
    DELETE FROM inplay.fact_user_atividade_diaria
    USING inplay.stg_fact_user_atividade_diaria src
    WHERE inplay.fact_user_atividade_diaria.id_usuario = src.id_usuario
    AND inplay.fact_user_atividade_diaria.data = src.data;

    INSERT INTO inplay.fact_user_atividade_diaria (
        id_usuario,
        valor_deposito,
        qtd_deposito,
        valor_retirada,
        qtd_retirada,
        data,
        importacao,
        valor_aposta,
        qtd_aposta,
        valor_bonus,
        qtd_bonus,
        ggr,
        realizou_aposta,
        realizou_deposito,
        realizou_saque,
        usou_bonus,
        usuario_ativo
    )
    SELECT
        src.id_usuario,
        src.valor_deposito,
        src.qtd_deposito,
        src.valor_retirada,
        src.qtd_retirada,
        src.data,
        src.importacao,
        src.valor_aposta,
        src.qtd_aposta,
        src.valor_bonus,
        src.qtd_bonus,
        src.ggr,
        src.realizou_aposta,
        src.realizou_deposito,
        src.realizou_saque,
        src.usou_bonus,
        src.usuario_ativo
    FROM inplay.stg_fact_user_atividade_diaria src;
"""


# =============================================================================
# SQL - PASSO 2: MÉTRICAS ACUMULADAS POR USUÁRIO
# Replica CTE usuario_metricas. Recalcula histórico completo apenas dos
# usuarios impactados na carga (não escaneia a tabela inteira).
# =============================================================================

SQL_STG_METRICAS = """
    INSERT INTO inplay.stg_agg_usuario_metricas (
        usuario,
        primeira_aposta_data,
        primeira_data_deposito_fato,
        realizou_aposta,
        data_ultima_aposta,
        data_ultimo_deposito,
        data_ultimo_saque,
        data_ultima_atividade,
        ltv_lifetime,
        total_apostas_valor,
        total_apostas_qtd,
        total_depositos_valor,
        total_depositos_qtd,
        total_saques_valor,
        total_saques_qtd,
        updated_at
    )
    WITH atividade_usuario AS (
        SELECT
            usuario,
            data_referencia::date,

            CASE
                WHEN COALESCE(cassino_bet_amount,0) > 0
                OR COALESCE(cassino_bet_quantity,0) > 0
                OR COALESCE(deposit_amount,0) > 0
                OR COALESCE(deposit_quantity,0) > 0
                OR COALESCE(withdraw_amount,0) > 0
                OR COALESCE(withdraw_quantity,0) > 0
                THEN 1 ELSE 0
            END AS teve_atividade,

            casino_bet_first_date::date,
            deposit_amount,
            deposit_quantity,
            cassino_bet_amount,
            cassino_bet_quantity,
            withdraw_amount,
            withdraw_quantity,
            cassino_ggr

        FROM inplay.fact_user_daily
    ),

    usuario_metricas AS (
        SELECT
            usuario,

            MIN(casino_bet_first_date) AS primeira_aposta_data,

            MIN(
                CASE
                    WHEN COALESCE(deposit_amount,0) > 0
                    OR COALESCE(deposit_quantity,0) > 0
                    THEN data_referencia
                END
            ) AS primeira_data_deposito_fato,

            CASE
                WHEN MAX(
                    CASE
                        WHEN casino_bet_first_date IS NOT NULL
                        OR COALESCE(cassino_bet_amount,0) > 0
                        OR COALESCE(cassino_bet_quantity,0) > 0
                        THEN 1 ELSE 0
                    END
                ) = 1
                THEN TRUE ELSE FALSE
            END AS realizou_aposta,

            MAX(
                CASE
                    WHEN COALESCE(cassino_bet_amount,0) > 0
                    OR COALESCE(cassino_bet_quantity,0) > 0
                    THEN data_referencia
                END
            ) AS data_ultima_aposta,

            MAX(
                CASE
                    WHEN COALESCE(deposit_amount,0) > 0
                    OR COALESCE(deposit_quantity,0) > 0
                    THEN data_referencia
                END
            ) AS data_ultimo_deposito,

            MAX(
                CASE
                    WHEN COALESCE(withdraw_amount,0) > 0
                    OR COALESCE(withdraw_quantity,0) > 0
                    THEN data_referencia
                END
            ) AS data_ultimo_saque,

            MAX(
                CASE
                    WHEN teve_atividade = 1
                    THEN data_referencia
                END
            ) AS data_ultima_atividade,

            SUM(COALESCE(cassino_ggr,0)) AS ltv_lifetime,

            SUM(COALESCE(cassino_bet_amount,0)) AS total_apostas_valor,
            SUM(COALESCE(cassino_bet_quantity,0)) AS total_apostas_qtd,

            SUM(COALESCE(deposit_amount,0)) AS total_depositos_valor,
            SUM(COALESCE(deposit_quantity,0)) AS total_depositos_qtd,

            SUM(COALESCE(withdraw_amount,0)) AS total_saques_valor,
            SUM(COALESCE(withdraw_quantity,0)) AS total_saques_qtd

        FROM atividade_usuario 
        WHERE atividade_usuario.usuario IN (
        SELECT usuario FROM inplay.stg_usuarios_impactados
    )
        GROUP BY usuario
    )
    

    SELECT
        usuario,
        primeira_aposta_data,
        primeira_data_deposito_fato,
        realizou_aposta,
        data_ultima_aposta,
        data_ultimo_deposito,
        data_ultimo_saque,
        data_ultima_atividade,
        ltv_lifetime,
        total_apostas_valor,
        total_apostas_qtd,
        total_depositos_valor,
        total_depositos_qtd,
        total_saques_valor,
        total_saques_qtd,
        CURRENT_DATE AS updated_at
    FROM usuario_metricas;
    """


SQL_MERGE_METRICAS = """
    DELETE FROM inplay.agg_usuario_metricas
    USING inplay.stg_agg_usuario_metricas src
    WHERE inplay.agg_usuario_metricas.usuario = src.usuario;

    INSERT INTO inplay.agg_usuario_metricas (
        usuario,
        primeira_aposta_data,
        primeira_data_deposito_fato,
        realizou_aposta,
        data_ultima_aposta,
        data_ultimo_deposito,
        data_ultimo_saque,
        data_ultima_atividade,
        ltv_lifetime,
        total_apostas_valor,
        total_apostas_qtd,
        total_depositos_valor,
        total_depositos_qtd,
        total_saques_valor,
        total_saques_qtd,
        updated_at
    )
    SELECT
        src.usuario,
        src.primeira_aposta_data,
        src.primeira_data_deposito_fato,
        src.realizou_aposta,
        src.data_ultima_aposta,
        src.data_ultimo_deposito,
        src.data_ultimo_saque,
        src.data_ultima_atividade,
        src.ltv_lifetime,
        src.total_apostas_valor,
        src.total_apostas_qtd,
        src.total_depositos_valor,
        src.total_depositos_qtd,
        src.total_saques_valor,
        src.total_saques_qtd,
        src.updated_at
    FROM inplay.stg_agg_usuario_metricas src;
"""


# =============================================================================
# SQL - PASSO 3: RECORRÊNCIA 30 DIAS
# Janela curta: recalcula TODOS os usuarios com atividade nos últimos 30 dias.
# Não usa escopo de impactados — é uma janela móvel, não há como incrementalizar.
# O custo é baixo porque o filtro de data já limita o volume lido da fato.
# =============================================================================

SQL_RECORRENCIA = """
    INSERT INTO inplay.agg_usuario_recorrencia_30d (
        usuario, dias_ativos_30d, recorrente_30d, updated_at
    )
    SELECT
        f.usuario,
        COUNT(DISTINCT f.data_referencia)                      AS dias_ativos_30d,
        CASE WHEN COUNT(DISTINCT f.data_referencia) >= 2
             THEN TRUE ELSE FALSE END                           AS recorrente_30d,
        GETDATE()                                               AS updated_at
    FROM inplay.fact_user_daily f
    WHERE f.data_referencia >= CURRENT_DATE - INTERVAL '30 days'
      AND (
            COALESCE(f.cassino_bet_amount,0) > 0
            OR COALESCE(f.cassino_bet_quantity,0) > 0
            OR COALESCE(f.deposit_amount,0) > 0
            OR COALESCE(f.deposit_quantity,0) > 0
            OR COALESCE(f.withdraw_amount,0) > 0
            OR COALESCE(f.withdraw_quantity,0) > 0
          )
    GROUP BY f.usuario
"""


# =============================================================================
# SQL - PASSO 4: ESTADO DE REATIVAÇÃO
# Replica CTEs reativacao_base/reativacao sem LAG sobre histórico inteiro.
# O LAG roda só sobre os usuarios impactados na carga atual; a borda entre
# a carga atual e o histórico anterior é resolvida consultando o estado salvo
# (data_ultima_atividade_conhecida) na própria tabela agg_usuario_reativacao.
#
# UPDATE e INSERT são separados (em vez de MERGE) porque o UPDATE precisa de
# GREATEST(valor_atual_do_target, valor_novo) — ou seja, precisa referenciar
# a coluna do target no SET, o que o MERGE do Redshift nessa versão não suporta
# de forma confiável na cláusula WHEN MATCHED THEN UPDATE SET.
# =============================================================================

SQL_STG_REATIVACAO = """
    INSERT INTO inplay.stg_agg_usuario_reativacao (
        usuario,
        data_ultima_atividade_conhecida,
        data_ultima_reativacao,
        foi_reativado_30d,
        updated_at
    )
    WITH atividade_recente AS (
        SELECT DISTINCT f.usuario, f.data_referencia
        FROM inplay.fact_user_daily f
        WHERE f.usuario IN (SELECT usuario FROM inplay.stg_usuarios_impactados)
          AND (
                COALESCE(f.cassino_bet_amount,0) > 0
                OR COALESCE(f.cassino_bet_quantity,0) > 0
                OR COALESCE(f.deposit_amount,0) > 0
                OR COALESCE(f.deposit_quantity,0) > 0
                OR COALESCE(f.withdraw_amount,0) > 0
                OR COALESCE(f.withdraw_quantity,0) > 0
              )
    ),
    atividade_com_lag_local AS (
        -- LAG roda apenas sobre o lote recente (barato)
        SELECT
            usuario,
            data_referencia,
            LAG(data_referencia) OVER (
                PARTITION BY usuario ORDER BY data_referencia
            ) AS atividade_anterior_local
        FROM atividade_recente
    ),
    combinado AS (
        -- Para a primeira atividade de cada usuario no lote atual,
        -- a "atividade anterior" vem do estado salvo (representa todo o histórico).
        -- Para as demais atividades dentro do lote, usa o LAG local.
        SELECT
            a.usuario,
            a.data_referencia,
            COALESCE(
                a.atividade_anterior_local,
                e.data_ultima_atividade_conhecida
            ) AS atividade_anterior
        FROM atividade_com_lag_local a
        LEFT JOIN inplay.agg_usuario_reativacao e
            ON e.usuario = a.usuario
    )
    SELECT
        usuario,
        MAX(data_referencia)                              AS data_ultima_atividade_conhecida,
        MAX(CASE
                WHEN atividade_anterior IS NOT NULL
                     AND data_referencia - atividade_anterior >= 30
                THEN data_referencia
            END)                                          AS data_ultima_reativacao,
        CASE
            WHEN MAX(CASE
                         WHEN atividade_anterior IS NOT NULL
                              AND data_referencia - atividade_anterior >= 30
                         THEN 1 ELSE 0
                     END) = 1
            THEN TRUE ELSE FALSE
        END                                               AS foi_reativado_30d,
        GETDATE()                                         AS updated_at
    FROM combinado
    GROUP BY usuario
"""

# UPDATE separado: referencia o valor atual do target (tgt.col) para GREATEST
# -- isso não é possível de forma segura dentro do MERGE WHEN MATCHED THEN UPDATE SET
SQL_UPDATE_REATIVACAO = """
    UPDATE inplay.agg_usuario_reativacao
    SET
        data_ultima_atividade_conhecida = s.data_ultima_atividade_conhecida,
        data_ultima_reativacao = GREATEST(
            COALESCE(inplay.agg_usuario_reativacao.data_ultima_reativacao,
                     '1900-01-01'::date),
            COALESCE(s.data_ultima_reativacao,
                     '1900-01-01'::date)
        ),
        foi_reativado_30d = (
            inplay.agg_usuario_reativacao.foi_reativado_30d OR s.foi_reativado_30d
        ),
        updated_at = s.updated_at
    FROM inplay.stg_agg_usuario_reativacao s
    WHERE inplay.agg_usuario_reativacao.usuario = s.usuario
"""

# INSERT somente de usuarios que ainda não existem na tabela destino
SQL_INSERT_REATIVACAO = """
    INSERT INTO inplay.agg_usuario_reativacao (
        usuario,
        data_ultima_atividade_conhecida,
        data_ultima_reativacao,
        foi_reativado_30d,
        updated_at
    )
    SELECT
        s.usuario,
        s.data_ultima_atividade_conhecida,
        s.data_ultima_reativacao,
        s.foi_reativado_30d,
        s.updated_at
    FROM inplay.stg_agg_usuario_reativacao s
    WHERE NOT EXISTS (
        SELECT 1
        FROM inplay.agg_usuario_reativacao e
        WHERE e.usuario = s.usuario
    )
"""


# =============================================================================
# FUNÇÃO PRINCIPAL
# Chamada ao final de principal_zeroum() / principal_energiabet(),
# depois que todos os merges de fact_user_daily já foram executados.
# =============================================================================

def executar_agregacao_dim_usuario(
    df_list: list,
    cliente: str,
    db: str,
    logger,
    normalize_numpy=True
):
    """
    Alimenta as tabelas físicas de agregação derivadas de fact_user_daily.

    Parâmetros
    ----------
    df_list : list[pd.DataFrame]
        Lista de DataFrames extraídos do Metabase na carga atual (df_stg,
        df_deposito, df_saque, df_primeira_aposta, df_ultima_aposta,
        df_bonus, df_bonus_ativado, df_stg_sport).
        A coluna 'usuario' deve existir em pelo menos um deles.
    cliente : str
        'ZEROUM' ou 'ENERGIABET' — usado para conectar no banco correto.
    db : str
        Valor da variável DB do ambiente ('redshift' ou 'postgres').
    logger : logging.Logger
        Logger do ETL principal.
    """
    try:
        logger.info("[AGG] Iniciando atualização das tabelas de agregação")

        # ------------------------------------------------------------------
        # 0. Coleta os usuarios distintos impactados nesta carga
        # ------------------------------------------------------------------
        series_list = []
        for df in df_list:
            if df is not None and 'usuario' in df.columns:
                series_list.append(df['usuario'].dropna().astype(int))

        if not series_list:
            logger.warning("[AGG] Nenhum DataFrame com coluna 'usuario' encontrado. "
                           "Agregação pulada.")
            return

        usuarios_unicos = pd.concat(series_list).drop_duplicates().reset_index(drop=True)
        df_impactados = pd.DataFrame({'usuario': usuarios_unicos})

        logger.info(f"[AGG] {len(df_impactados)} usuarios impactados nesta carga")

        # ------------------------------------------------------------------
        # 0a. Limpa e popula stg_usuarios_impactados
        # ------------------------------------------------------------------
        logger.info("[AGG] Atualizando stg_usuarios_impactados")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.deleta_dados('inplay.stg_usuarios_impactados', '', logger)

        ConnectionDB.conecta(db, cliente)
        ConnectionDB.insere_dados_bulk('inplay.stg_usuarios_impactados',
                                       df_impactados, logger,
                                            normalize_numpy=True)

        # ------------------------------------------------------------------
        # 1. Fato diária física
        # ------------------------------------------------------------------
        logger.info("[AGG] Limpando stg_fact_user_atividade_diaria")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.deleta_dados('inplay.stg_fact_user_atividade_diaria', '', logger)

        logger.info("[AGG] Populando stg_fact_user_atividade_diaria")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_STG_FATO_DIARIA, logger)

        logger.info("[AGG] Merge fact_user_atividade_diaria")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_MERGE_FATO_DIARIA, logger)

        # ------------------------------------------------------------------
        # 2. Métricas acumuladas
        # ------------------------------------------------------------------
        logger.info("[AGG] Limpando stg_agg_usuario_metricas")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.deleta_dados('inplay.stg_agg_usuario_metricas', '', logger)

        logger.info("[AGG] Populando stg_agg_usuario_metricas")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_STG_METRICAS, logger)

        logger.info("[AGG] Merge agg_usuario_metricas")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_MERGE_METRICAS, logger)

        # ------------------------------------------------------------------
        # 3. Recorrência 30 dias (full recompute da janela, sem escopo)
        # ------------------------------------------------------------------
        logger.info("[AGG] Recalculando agg_usuario_recorrencia_30d")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.deleta_dados('inplay.agg_usuario_recorrencia_30d', '', logger)

        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_RECORRENCIA, logger)

        # ------------------------------------------------------------------
        # 4. Estado de reativação
        # ------------------------------------------------------------------
        logger.info("[AGG] Limpando stg_agg_usuario_reativacao")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.deleta_dados('inplay.stg_agg_usuario_reativacao', '', logger)

        logger.info("[AGG] Populando stg_agg_usuario_reativacao")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_STG_REATIVACAO, logger)

        logger.info("[AGG] Update agg_usuario_reativacao (existentes)")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_UPDATE_REATIVACAO, logger)

        logger.info("[AGG] Insert agg_usuario_reativacao (novos)")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_INSERT_REATIVACAO, logger)

        logger.info("[AGG] Atualização das tabelas de agregação concluída com sucesso")

    except Exception as e:
        logger.error(f"[AGG] Erro na atualização das tabelas de agregação: {e}")
        raise
