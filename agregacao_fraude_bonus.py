"""
agregacao_fraude_bonus.py
==========================
Módulo responsável por alimentar inplay.agg_fraude_bonus_diario -- a tabela
física que sustenta os relatórios curados #1 (alerta diário) e #5 (watchlist
persistente) da proposta de bônus/antifraude.

Não altera nenhum objeto existente em produção (não mexe em fact_user_bonus
nem em agg_bonus_concessoes, só lê deles).

Chamado dentro de processa_bonus() em consume_api.py, logo após
executar_agregacao_bonus_concessoes(df_fact_bonus, ...) -- roda de hora em
hora (confirmado 26/08/2026: ETL_BONUS não é diário, é horário, script
próprio via `python main.py --cliente ZEROUM_BONUS`).

Compatível com Redshift 1.0.331549 (mesmas regras dos demais agregacao_*.py):
- Sem IS DISTINCT FROM
- UPDATE + INSERT separados (não MERGE), porque o UPDATE precisa referenciar
  o valor atual da própria tabela alvo (score anterior, contagem de dias
  sinalizado) -- mesma razão pela qual agg_usuario_reativacao usa esse
  padrão em vez de MERGE.

DECISÃO DE ESCOPO: recompute completo, não incremental
--------------------------------------------------------
Diferente de agregacao_dim_usuario.py (que escopa por usuarios_impactados),
aqui o score de CADA cliente depende do percentil 99 calculado sobre TODA a
população -- se um único cliente novo mudasse o percentil, o score de todos
os outros teria que ser recalculado mesmo assim. Por isso a stg_ é
repopulada com um recompute completo (não escopado), lendo de
agg_bonus_concessoes (~6M linhas agregadas por cliente), não de
fact_user_bonus (58M+ linhas brutas) -- ainda assim muito mais barato que
reprocessar a fato inteira.

DECISÃO DE ESCOPO: percentil recalculado só 1x/dia (não a cada execução)
--------------------------------------------------------------------------
Como este módulo agora roda de hora em hora (dentro de processa_bonus(),
logo após executar_agregacao_bonus_concessoes -- confirmado no
consume_api.py real em 26/08/2026), recalcular o percentil 99 (que exige
ordenar ~6M linhas) toda hora seria 24x mais caro que o necessário. O
"normal" da base não muda relevantemente de uma hora para outra dentro do
mesmo dia. Por isso: os CORTES são calculados uma vez por dia (guardados em
inplay.corte_percentil_bonus_diario) e reaproveitados nas execuções
seguintes do mesmo dia -- só os TOTAIS por cliente (que dependem do
incremental) são recalculados a cada hora.
"""

from database import ConnectionDB

# Threshold mínimo de score para entrar no alerta/watchlist -- default do
# item 6.3 da proposta (>= 2). Ajustar aqui quando o cliente/compliance
# validar a sensibilidade desejada.
LIMIAR_ALERTA = 2

SINAIS = ["qtd_bonus", "custo_total", "max_reuso", "rollover_pendente"]


# =============================================================================
# DDL -- rodar uma vez por ambiente antes do primeiro uso
# =============================================================================

DDL_AGG_FRAUDE_BONUS_DIARIO = """
    CREATE TABLE IF NOT EXISTS inplay.agg_fraude_bonus_diario (
        client_id             BIGINT NOT NULL         ENCODE az64,
        partner_id            BIGINT                  ENCODE az64,
        qtd_bonus             INTEGER                 ENCODE az64,
        custo_total           NUMERIC(18,4)            ENCODE az64,
        max_reuso             INTEGER                 ENCODE az64,
        rollover_pendente     NUMERIC(18,4)            ENCODE az64,
        score                 SMALLINT                ENCODE az64,
        score_anterior        SMALLINT                ENCODE az64,
        cruzou_limiar_hoje    BOOLEAN                 ENCODE RAW,
        qtd_dias_sinalizado   INTEGER                 ENCODE az64,
        data_primeiro_alerta  DATE                    ENCODE az64,
        data_ultimo_alerta    DATE                    ENCODE az64,
        updated_at            TIMESTAMP WITHOUT TIME ZONE ENCODE RAW,
        PRIMARY KEY (client_id, partner_id)
    )
    DISTSTYLE KEY
    DISTKEY (client_id)
    SORTKEY (updated_at, client_id)
"""

DDL_STG_AGG_FRAUDE_BONUS_DIARIO = """
    CREATE TABLE IF NOT EXISTS inplay.stg_agg_fraude_bonus_diario (
        client_id          BIGINT         ENCODE az64,
        partner_id         BIGINT         ENCODE az64,
        qtd_bonus          INTEGER        ENCODE az64,
        custo_total        NUMERIC(18,4)   ENCODE az64,
        max_reuso          INTEGER        ENCODE az64,
        rollover_pendente  NUMERIC(18,4)   ENCODE az64,
        score              SMALLINT       ENCODE az64
    )
    DISTSTYLE KEY
    DISTKEY (client_id)
"""

# Tabela minúscula (4 linhas por dia) -- guarda o corte de percentil já
# calculado hoje, pra não recalcular a cada execução horária.
DDL_CORTE_PERCENTIL_BONUS_DIARIO = """
    CREATE TABLE IF NOT EXISTS inplay.corte_percentil_bonus_diario (
        data   DATE         NOT NULL,
        sinal  VARCHAR(30)  NOT NULL,
        valor  NUMERIC(18,4),
        PRIMARY KEY (data, sinal)
    )
    DISTSTYLE ALL
"""


# =============================================================================
# SQL -- CACHE DE CORTES (calculado só 1x por dia civil)
# =============================================================================

SQL_CONTA_CORTES_DE_HOJE = """
    SELECT COUNT(*) AS qtd
    FROM inplay.corte_percentil_bonus_diario
    WHERE data = CURRENT_DATE
"""

SQL_CALCULA_CORTES_DE_HOJE = """
    INSERT INTO inplay.corte_percentil_bonus_diario (data, sinal, valor)
    WITH metricas_cliente AS (
        SELECT
            client_id,
            partner_id,
            SUM(qtd_concessoes)                  AS qtd_bonus,
            COALESCE(SUM(custo_total), 0)        AS custo_total,
            MAX(max_reuso)                       AS max_reuso,
            COALESCE(SUM(rollover_pendente), 0)  AS rollover_pendente
        FROM inplay.agg_bonus_concessoes
        GROUP BY client_id, partner_id
    )
    SELECT CURRENT_DATE, 'qtd_bonus',
           PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY qtd_bonus) FROM metricas_cliente
    UNION ALL
    SELECT CURRENT_DATE, 'custo_total',
           PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY custo_total) FROM metricas_cliente
    UNION ALL
    SELECT CURRENT_DATE, 'max_reuso',
           PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY max_reuso) FROM metricas_cliente
    UNION ALL
    SELECT CURRENT_DATE, 'rollover_pendente',
           PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY rollover_pendente) FROM metricas_cliente
"""

SQL_LE_CORTES_DE_HOJE = """
    SELECT sinal, valor
    FROM inplay.corte_percentil_bonus_diario
    WHERE data = CURRENT_DATE
"""


# =============================================================================
# SQL -- PASSO 1: recompute dos TOTAIS por cliente + score, usando os
# CORTES JÁ CALCULADOS hoje (não recalcula percentil aqui -- só compara
# contra o valor fixo lido de corte_percentil_bonus_diario).
# =============================================================================

def sql_stg_score_diario(cortes: dict) -> str:
    return f"""
        INSERT INTO inplay.stg_agg_fraude_bonus_diario (
            client_id, partner_id, qtd_bonus, custo_total, max_reuso,
            rollover_pendente, score
        )
        WITH metricas_cliente AS (
            SELECT
                client_id,
                partner_id,
                SUM(qtd_concessoes)                  AS qtd_bonus,
                COALESCE(SUM(custo_total), 0)        AS custo_total,
                MAX(max_reuso)                       AS max_reuso,
                COALESCE(SUM(rollover_pendente), 0)  AS rollover_pendente
            FROM inplay.agg_bonus_concessoes
            GROUP BY client_id, partner_id
        )
        SELECT
            client_id,
            partner_id,
            qtd_bonus,
            custo_total,
            max_reuso,
            rollover_pendente,
            (CASE WHEN qtd_bonus > {cortes['qtd_bonus']} THEN 1 ELSE 0 END
           + CASE WHEN custo_total > {cortes['custo_total']} THEN 1 ELSE 0 END
           + CASE WHEN max_reuso > {cortes['max_reuso']} THEN 1 ELSE 0 END
           + CASE WHEN rollover_pendente > {cortes['rollover_pendente']} THEN 1 ELSE 0 END) AS score
        FROM metricas_cliente
    """


# =============================================================================
# SQL -- PASSO 2: UPDATE dos clientes já existentes na tabela alvo
# Referencia o valor atual do target (score antigo) para:
#   - detectar cruzamento do limiar hoje (score_anterior < LIMIAR <= score)
#   - incrementar contador de dias sinalizado
#   - preservar data_primeiro_alerta (só seta se ainda for NULL)
# Por isso é UPDATE separado, não MERGE (mesma razão de agg_usuario_reativacao).
# =============================================================================

SQL_UPDATE_FRAUDE_BONUS_DIARIO = f"""
    UPDATE inplay.agg_fraude_bonus_diario
    SET
        qtd_bonus            = s.qtd_bonus,
        custo_total           = s.custo_total,
        max_reuso             = s.max_reuso,
        rollover_pendente     = s.rollover_pendente,
        score_anterior        = inplay.agg_fraude_bonus_diario.score,
        score                 = s.score,
        cruzou_limiar_hoje    = (
            s.score >= {LIMIAR_ALERTA}
            AND inplay.agg_fraude_bonus_diario.score < {LIMIAR_ALERTA}
        ),
        qtd_dias_sinalizado   = inplay.agg_fraude_bonus_diario.qtd_dias_sinalizado
            + CASE
                WHEN s.score >= {LIMIAR_ALERTA}
                     -- BUGFIX (26/08/2026): só conta 1x por dia civil -- se o
                     -- job rodar 2x no mesmo dia (ex.: retry), não pode somar
                     -- de novo. Só incrementa se a última contagem foi ONTEM
                     -- ou antes (ou nunca aconteceu).
                     AND (
                         inplay.agg_fraude_bonus_diario.data_ultimo_alerta IS NULL
                         OR inplay.agg_fraude_bonus_diario.data_ultimo_alerta < CURRENT_DATE
                     )
                THEN 1
                ELSE 0
              END,
        data_primeiro_alerta  = COALESCE(
            inplay.agg_fraude_bonus_diario.data_primeiro_alerta,
            CASE WHEN s.score >= {LIMIAR_ALERTA} THEN CURRENT_DATE END
        ),
        data_ultimo_alerta    = CASE
            WHEN s.score >= {LIMIAR_ALERTA} THEN CURRENT_DATE
            ELSE inplay.agg_fraude_bonus_diario.data_ultimo_alerta
        END,
        updated_at            = GETDATE()
    FROM inplay.stg_agg_fraude_bonus_diario s
    WHERE inplay.agg_fraude_bonus_diario.client_id = s.client_id
      AND inplay.agg_fraude_bonus_diario.partner_id = s.partner_id
"""


# =============================================================================
# SQL -- PASSO 3: INSERT dos clientes que ainda não existem na tabela alvo
# (primeira vez que aparecem em agg_bonus_concessoes)
# =============================================================================

SQL_INSERT_FRAUDE_BONUS_DIARIO = f"""
    INSERT INTO inplay.agg_fraude_bonus_diario (
        client_id, partner_id, qtd_bonus, custo_total, max_reuso,
        rollover_pendente, score, score_anterior, cruzou_limiar_hoje,
        qtd_dias_sinalizado, data_primeiro_alerta, data_ultimo_alerta,
        updated_at
    )
    SELECT
        s.client_id,
        s.partner_id,
        s.qtd_bonus,
        s.custo_total,
        s.max_reuso,
        s.rollover_pendente,
        s.score,
        NULL                                                       AS score_anterior,
        (s.score >= {LIMIAR_ALERTA})                                AS cruzou_limiar_hoje,
        CASE WHEN s.score >= {LIMIAR_ALERTA} THEN 1 ELSE 0 END      AS qtd_dias_sinalizado,
        CASE WHEN s.score >= {LIMIAR_ALERTA} THEN CURRENT_DATE END AS data_primeiro_alerta,
        CASE WHEN s.score >= {LIMIAR_ALERTA} THEN CURRENT_DATE END AS data_ultimo_alerta,
        GETDATE()                                                   AS updated_at
    FROM inplay.stg_agg_fraude_bonus_diario s
    WHERE NOT EXISTS (
        SELECT 1
        FROM inplay.agg_fraude_bonus_diario e
        WHERE e.client_id = s.client_id
          AND e.partner_id = s.partner_id
    )
"""


# =============================================================================
# FUNÇÃO PRINCIPAL
# Chamada ao final de principal_zeroum() / principal_energiabet(), depois
# que agg_bonus_concessoes já foi atualizada na carga do dia.
# =============================================================================

def executar_agregacao_fraude_bonus(cliente: str, db: str, logger):
    """
    Recalcula o score de todos os clientes com base em agg_bonus_concessoes
    e atualiza inplay.agg_fraude_bonus_diario -- a tabela que sustenta os
    relatórios #1 (alerta diário) e #5 (watchlist persistente).

    Chamada de hora em hora, dentro de processa_bonus() em consume_api.py,
    logo após executar_agregacao_bonus_concessoes(df_fact_bonus, ...).

    Parâmetros
    ----------
    cliente : 'ZEROUM' ou 'ENERGIABET'
    db      : 'redshift' ou 'postgres'
    logger  : logger do ETL principal
    """
    try:
        logger.info("[FRAUDE_BONUS] Iniciando atualização de agg_fraude_bonus_diario")

        # Garante as tabelas (idempotente, CREATE TABLE IF NOT EXISTS)
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(DDL_AGG_FRAUDE_BONUS_DIARIO, logger)
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(DDL_STG_AGG_FRAUDE_BONUS_DIARIO, logger)
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(DDL_CORTE_PERCENTIL_BONUS_DIARIO, logger)

        # Cache de cortes: só recalcula percentil se ainda não foi feito hoje
        ConnectionDB.conecta(db, cliente)
        df_conta = ConnectionDB.executa_script(SQL_CONTA_CORTES_DE_HOJE, logger)
        cortes_ja_calculados = int(df_conta.iloc[0]["qtd"]) >= len(SINAIS)

        if not cortes_ja_calculados:
            logger.info("[FRAUDE_BONUS] Cortes de hoje ainda não calculados -- calculando (1x/dia)")
            ConnectionDB.conecta(db, cliente)
            ConnectionDB.executa_dml(SQL_CALCULA_CORTES_DE_HOJE, logger)
        else:
            logger.info("[FRAUDE_BONUS] Reaproveitando cortes já calculados hoje (sem recalcular percentil)")

        ConnectionDB.conecta(db, cliente)
        df_cortes = ConnectionDB.executa_script(SQL_LE_CORTES_DE_HOJE, logger)
        cortes = dict(zip(df_cortes["sinal"], df_cortes["valor"]))
        logger.info(f"[FRAUDE_BONUS] Cortes do dia: {cortes}")

        # Passo 1: recompute dos totais + score em staging, usando os cortes fixos
        logger.info("[FRAUDE_BONUS] Limpando stg_agg_fraude_bonus_diario")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.deleta_dados('inplay.stg_agg_fraude_bonus_diario', '', logger)

        logger.info("[FRAUDE_BONUS] Recalculando totais e score do momento (via agg_bonus_concessoes)")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(sql_stg_score_diario(cortes), logger)

        # Passo 2: UPDATE dos clientes já conhecidos
        logger.info("[FRAUDE_BONUS] Atualizando clientes existentes")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_UPDATE_FRAUDE_BONUS_DIARIO, logger)

        # Passo 3: INSERT dos clientes novos
        logger.info("[FRAUDE_BONUS] Inserindo clientes novos")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(SQL_INSERT_FRAUDE_BONUS_DIARIO, logger)

        logger.info("[FRAUDE_BONUS] Atualização de agg_fraude_bonus_diario concluída com sucesso")

    except Exception as e:
        logger.error(f"[FRAUDE_BONUS] Erro na atualização de agg_fraude_bonus_diario: {e}")
        raise
