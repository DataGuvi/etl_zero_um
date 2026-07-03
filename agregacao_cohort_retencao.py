"""
agregacao_cohort_retencao.py
=============================
ESCOPO: cliente ZEROUM apenas. Chamar somente dentro de principal_zeroum(),
com cliente='ZEROUM'. Não chamar para 'ENERGIABET' -- esse cliente não usa
agregação de dim_usuario/cohort hoje.

Módulo responsável por alimentar a tabela física inplay.agg_cohort_atividade_diaria,
que substitui o processamento pesado das views:

    - inplay.vw_fato_cohort_ltv_retencao          (grão: cohort mês x dia 0-90)
    - inplay.vw_fato_cohort_retention              (grão: cohort mês x janela D1..D90)
    - inplay.vw_fato_retention_diaria_mensageria   (grão: cohort dia  x janela D1..D90)

Também alimenta inplay.agg_kpi_diario_datatalk, snapshot histórico (1 linha/dia)
dos indicadores usados na mensagem diária do Datatalk (antes recalculados do
zero a cada leitura de vw_kpi_diario_jogadores_datatalk, sem guardar histórico).

Não cria nenhuma view de KPI (vw_kpi_diario_jogadores_datatalk) -- ela nunca
existiu em produção e não traria ganho de performance (view não-materializada
reexecuta a query inteira a cada leitura). A mensageria deve ler direto de
inplay.agg_kpi_diario_datatalk (tabela física, 1 linha/dia).

Recria 3 views (todas novas, nenhuma existia em produção -- deploy, não
substituição) para apontarem para inplay.agg_cohort_atividade_diaria:
    vw_fato_cohort_ltv_retencao, vw_fato_cohort_retention,
    vw_fato_retention_diaria_mensageria
(ver views_atualizadas_cohort_retencao_kpi.sql).

DEPENDÊNCIAS / ORDEM DE EXECUÇÃO
---------------------------------
    1. executar_agregacao_dim_usuario()      (agregacao_dim_usuario.py) -- já existente
    2. executar_agregacao_cohort_retencao()  (este módulo)              -- depende do passo 1
    3. executar_kpi_diario_datatalk()        (este módulo)              -- depende do passo 2,
       roda só 1x/dia (não a cada carga incremental do ETL)

Compatível com Redshift 1.0.331549 (PostgreSQL 8.0.2):
    - Sem IS DISTINCT FROM
    - DELETE + INSERT por cohort (data_ftd) afetada, mesmo padrão usado em
      agg_usuario_reativacao / agg_fato_diaria
    - Só recalcula cohorts cujos usuários foram impactados nesta carga E que
      ainda estão dentro da janela de 90 dias (cohorts fora dessa janela nunca
      mais mudam, então não entram no escopo de reprocessamento)

ATENÇÃO - CARGA INICIAL (BACKFILL)
------------------------------------
executar_agregacao_cohort_retencao() só processa cohorts (data_ftd) associadas
a usuários impactados na carga corrente. Isso é ótimo para o dia a dia, mas
significa que a tabela agg_cohort_atividade_diaria PRECISA ser populada uma
vez, do zero, para todo o histórico existente, antes de trocar as views em
produção -- senão cohorts antigas ficam com dados incompletos ou ausentes.
Um script de backfill (full, sem filtro de stg_usuarios_impactados/90 dias)
deve ser rodado manualmente uma única vez; posso montar em seguida se
necessário.
"""

import pandas as pd
from database import ConnectionDB


# =============================================================================
# PASSO 1: STAGING — agregado diário de atividade pós-FTD, escopado às cohorts
# (data_ftd) afetadas pela carga atual.
# =============================================================================

SQL_STG_COHORT_ATIVIDADE = """
    INSERT INTO inplay.stg_agg_cohort_atividade_diaria (
        data_ftd, dias_desde_ftd, usuarios_cohort, usuarios_ativos, ggr_dia, updated_at
    )
    WITH usuario_ftd_resolvido AS (
        -- replica EXATAMENTE a resolução de primeiro_deposito_real da
        -- vw_dim_usuario_v2: prioriza o first_deposit_date de origem
        -- (dim_usuario); só cai para o valor calculado da fato
        -- (agg_usuario_metricas) quando o campo de origem está no sentinela
        -- 1970-01-01. NÃO usar agg_usuario_metricas.primeira_data_deposito_fato
        -- isoladamente -- diverge do FTD oficial usado em produção.
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
    cohorts_afetadas AS (
        -- cohorts (data_ftd resolvido) de usuarios impactados nesta carga,
        -- ainda dentro da janela de 90 dias (fora disso, o cohort já está
        -- "fechado" e não recebe mais linhas novas de dias_desde_ftd).
        --
        -- Confirmado com o time: first_deposit_date de origem só transiciona
        -- de vazio (1970-01-01) para preenchido UMA única vez, nunca é
        -- retificado depois. Ou seja, um usuario nunca "migra" de cohort
        -- após o primeiro preenchimento -- o filtro de 90 dias é suficiente
        -- e não é necessária uma tabela de mapeamento usuario -> data_ftd
        -- para cobrir reatribuição de cohort.
        SELECT DISTINCT r.data_ftd
        FROM usuario_ftd_resolvido r
        WHERE r.usuario IN (SELECT usuario FROM inplay.stg_usuarios_impactados)
          AND r.data_ftd IS NOT NULL
          AND r.data_ftd <> DATE '1970-01-01'
          AND r.data_ftd >= CURRENT_DATE - 90
    ),
    cohort_base AS (
        -- todos os usuarios que pertencem às cohorts afetadas (não só os
        -- impactados nesta carga -- precisa da cohort inteira para o total
        -- correto de usuarios_cohort e para não perder atividade de outros
        -- usuarios da mesma cohort que não mudaram hoje)
        SELECT
            r.usuario,
            r.data_ftd
        FROM usuario_ftd_resolvido r
        JOIN cohorts_afetadas c
          ON c.data_ftd = r.data_ftd
    ),
    cohort_tamanho AS (
        -- tamanho da cohort calculado UMA vez por data_ftd, sem join com
        -- atividade (evita o erro clássico de contar só quem teve atividade)
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

SQL_MERGE_COHORT_ATIVIDADE = """
    DELETE FROM inplay.agg_cohort_atividade_diaria
    USING (
        SELECT DISTINCT data_ftd FROM inplay.stg_agg_cohort_atividade_diaria
    ) s
    WHERE inplay.agg_cohort_atividade_diaria.data_ftd = s.data_ftd;

    INSERT INTO inplay.agg_cohort_atividade_diaria (
        data_ftd, dias_desde_ftd, usuarios_cohort, usuarios_ativos, ggr_dia, updated_at
    )
    SELECT
        data_ftd, dias_desde_ftd, usuarios_cohort, usuarios_ativos, ggr_dia, updated_at
    FROM inplay.stg_agg_cohort_atividade_diaria;
"""


# =============================================================================
# PASSO 2: SNAPSHOT DIÁRIO DOS INDICADORES DO DATATALK
# Roda 1x/dia (não a cada carga incremental). Espelha a lógica de
# vw_kpi_diario_jogadores_datatalk, mas grava histórico e lê, onde já dá,
# das tabelas físicas otimizadas (agg_cohort_atividade_diaria via a view
# vw_fato_retention_diaria_mensageria já recriada) em vez de recomputar tudo.
#
# registrados / ftd / conversao_ftd_d7 / base_engajada / churn_30_59 continuam
# lendo de vw_dim_usuario_v2 porque essa view não foi analisada em detalhe
# aqui -- preservei a lógica original exatamente para não arriscar
# divergência de resultado. Se quiser, dá pra otimizar essa parte também
# depois que soubermos exatamente como vw_dim_usuario_v2 é montada por
# dentro (provavelmente já pode ler de agg_usuario_metricas +
# agg_usuario_recorrencia_30d diretamente, sem passar pela view).
# =============================================================================

SQL_DELETE_KPI_HOJE = """
    DELETE FROM inplay.agg_kpi_diario_datatalk
    WHERE data_execucao = CURRENT_DATE
"""

SQL_INSERT_KPI_DIARIO = """
    INSERT INTO inplay.agg_kpi_diario_datatalk (
        data_execucao,
        registrados_ontem, var_registrados_pct,
        ftd_ontem, var_ftd_pct,
        conversao_ftd_d7,
        activation_rate,
        retention_d1, retention_d1_media_30d, var_retention_d1_pp,
        retention_d7, retention_d7_media_30d, var_retention_d7_pp,
        base_engajada,
        churn_30_59
    )
    WITH parametros AS (
        SELECT
            CURRENT_DATE - 1 AS dt_ontem,
            CURRENT_DATE - 2 AS dt_anteontem,
            CURRENT_DATE - 7 AS dt_d7
    ),
    registrados AS (
        SELECT
            SUM(CASE WHEN registro = p.dt_ontem THEN 1 ELSE 0 END) AS registrados_ontem,
            SUM(CASE WHEN registro = p.dt_anteontem THEN 1 ELSE 0 END) AS registrados_anteontem
        FROM inplay.vw_dim_usuario_v2
        CROSS JOIN parametros p
    ),
    ftd AS (
        SELECT
            SUM(CASE WHEN primeiro_deposito = p.dt_ontem THEN 1 ELSE 0 END) AS ftd_ontem,
            SUM(CASE WHEN primeiro_deposito = p.dt_anteontem THEN 1 ELSE 0 END) AS ftd_anteontem
        FROM inplay.vw_dim_usuario_v2
        CROSS JOIN parametros p
    ),
    conversao_d7 AS (
        SELECT
            ROUND(
                (SUM(CASE WHEN realizou_deposito = TRUE THEN 1 ELSE 0 END)::NUMERIC
                 / NULLIF(COUNT(*), 0)) * 100
            , 2) AS conversao_ftd_d7
        FROM inplay.vw_dim_usuario_v2
        CROSS JOIN parametros p
        WHERE registro = p.dt_d7
    ),
    activation AS (
        -- swap de vw_fato_usuarios_ativos -> fact_user_atividade_diaria:
        -- ambas têm o mesmo grão (id_usuario, data, realizou_aposta) e
        -- fact_user_atividade_diaria já é a réplica física validada dessa view
        SELECT
            ROUND(
                (COUNT(DISTINCT CASE WHEN fa.realizou_aposta = TRUE THEN fa.id_usuario END)::NUMERIC
                 / NULLIF(COUNT(DISTINCT du.id), 0)) * 100
            , 2) AS activation_rate
        FROM inplay.vw_dim_usuario_v2 du
        CROSS JOIN parametros p
        LEFT JOIN inplay.fact_user_atividade_diaria fa
               ON du.id = fa.id_usuario
              AND fa.data = p.dt_ontem
        WHERE du.primeiro_deposito = p.dt_ontem
    ),
    retention_d1 AS (
        -- lê da view já recriada sobre agg_cohort_atividade_diaria (barata)
        SELECT MAX(retention_rate) AS retention_d1
        FROM inplay.vw_fato_retention_diaria_mensageria
        WHERE data_ftd = CURRENT_DATE - 1 AND janela_retencao = 'D1'
    ),
    retention_d7 AS (
        SELECT MAX(retention_rate) AS retention_d7
        FROM inplay.vw_fato_retention_diaria_mensageria
        WHERE data_ftd = CURRENT_DATE - 7 AND janela_retencao = 'D7'
    ),
    retention_d1_media AS (
        SELECT ROUND(AVG(retention_rate), 2) AS retention_d1_media_30d
        FROM inplay.vw_fato_retention_diaria_mensageria
        WHERE janela_retencao = 'D1'
          AND data_ftd BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE - 1
    ),
    retention_d7_media AS (
        SELECT ROUND(AVG(retention_rate), 2) AS retention_d7_media_30d
        FROM inplay.vw_fato_retention_diaria_mensageria
        WHERE janela_retencao = 'D7'
          AND data_ftd BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE - 7
    ),
    engajamento AS (
        -- mantido igual à view original (lê recorrente_30d de vw_dim_usuario_v2)
        -- para não arriscar divergência sem saber como essa coluna é montada
        SELECT
            ROUND(
                (SUM(CASE WHEN recorrente_30d = TRUE THEN 1 ELSE 0 END)::NUMERIC
                 / NULLIF(SUM(CASE WHEN realizou_deposito = TRUE THEN 1 ELSE 0 END), 0)) * 100
            , 2) AS base_engajada
        FROM inplay.vw_dim_usuario_v2
    ),
    churn AS (
        SELECT
            ROUND(
                (SUM(CASE WHEN realizou_deposito = TRUE
                           AND dias_sem_atividade BETWEEN 30 AND 59
                          THEN 1 ELSE 0 END)::NUMERIC
                 / NULLIF(SUM(CASE WHEN realizou_deposito = TRUE THEN 1 ELSE 0 END), 0)) * 100
            , 2) AS churn_30_59
        FROM inplay.vw_dim_usuario_v2
    )
    SELECT
        CURRENT_DATE,
        r.registrados_ontem,
        ROUND((r.registrados_ontem::NUMERIC / NULLIF(r.registrados_anteontem, 0) - 1) * 100, 2),
        f.ftd_ontem,
        ROUND((f.ftd_ontem::NUMERIC / NULLIF(f.ftd_anteontem, 0) - 1) * 100, 2),
        c.conversao_ftd_d7,
        a.activation_rate,
        rd1.retention_d1, rd1m.retention_d1_media_30d,
        ROUND(rd1.retention_d1 - rd1m.retention_d1_media_30d, 2),
        rd7.retention_d7, rd7m.retention_d7_media_30d,
        ROUND(rd7.retention_d7 - rd7m.retention_d7_media_30d, 2),
        e.base_engajada,
        ch.churn_30_59
    FROM registrados r
    CROSS JOIN ftd f
    CROSS JOIN conversao_d7 c
    CROSS JOIN activation a
    CROSS JOIN retention_d1 rd1
    CROSS JOIN retention_d7 rd7
    CROSS JOIN retention_d1_media rd1m
    CROSS JOIN retention_d7_media rd7m
    CROSS JOIN engajamento e
    CROSS JOIN churn ch
"""


# =============================================================================
# FUNÇÕES PRINCIPAIS
# =============================================================================

def executar_agregacao_cohort_retencao(cliente: str, db: str, logger):
    """
    Atualiza inplay.agg_cohort_atividade_diaria, escopado às cohorts (data_ftd)
    afetadas pela carga atual.

    IMPORTANTE: deve ser chamada logo após executar_agregacao_dim_usuario()
    na mesma execução, pois depende de inplay.stg_usuarios_impactados e
    inplay.agg_usuario_metricas já atualizados nesta carga.
    """
    try:
        logger.info("[COHORT] Iniciando atualização de agg_cohort_atividade_diaria")

        logger.info("[COHORT] Limpando stg_agg_cohort_atividade_diaria")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.deleta_dados('inplay.stg_agg_cohort_atividade_diaria', '', logger)

        logger.info("[COHORT] Populando stg_agg_cohort_atividade_diaria")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_STG_COHORT_ATIVIDADE, logger)

        logger.info("[COHORT] Merge (delete+insert por cohort) agg_cohort_atividade_diaria")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_MERGE_COHORT_ATIVIDADE, logger)

        logger.info("[COHORT] Atualização concluída com sucesso")

    except Exception as e:
        logger.error(f"[COHORT] Erro na atualização de agg_cohort_atividade_diaria: {e}")
        raise


def executar_kpi_diario_datatalk(cliente: str, db: str, logger):
    """
    Grava o snapshot diário dos indicadores do Datatalk em
    inplay.agg_kpi_diario_datatalk.

    Deve rodar 1x/dia (idealmente na primeira carga do dia, não em toda
    execução incremental do ETL), depois de executar_agregacao_cohort_retencao(),
    pois depende de agg_cohort_atividade_diaria já atualizada para o dia.

    A rotina de mensageria (WhatsApp) NÃO deve consultar uma view -- deve ler
    direto desta tabela:
        SELECT * FROM inplay.agg_kpi_diario_datatalk WHERE data_execucao = CURRENT_DATE
    """
    try:
        logger.info("[KPI_DATATALK] Iniciando snapshot diário")

        logger.info("[KPI_DATATALK] Limpando linha de hoje (se existir)")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_DELETE_KPI_HOJE, logger)

        logger.info("[KPI_DATATALK] Inserindo snapshot de hoje")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_INSERT_KPI_DIARIO, logger)

        logger.info("[KPI_DATATALK] Snapshot gravado com sucesso")

    except Exception as e:
        logger.error(f"[KPI_DATATALK] Erro ao gravar snapshot diário: {e}")
        raise
