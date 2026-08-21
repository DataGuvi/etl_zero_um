"""
agregacao_bonus.py
====================
Atualiza inplay.agg_bonus_concessoes -- visão agrupada bruta por
client_id + bonus_id (equivalente ao relatório de chave PIX usada por
cliente), consumida pelo endpoint de concessões de bônus.

POR QUE ESSE MÓDULO EXISTE
---------------------------
inplay.fact_user_bonus tem 58M+ linhas. Se o endpoint calculasse
GROUP BY client_id, bonus_id em tempo real a cada chamada, seria lento e
caro. Em vez disso, pré-calculamos o agregado numa tabela física
(agg_bonus_concessoes) e o endpoint só lê dali.

COMO FUNCIONA (estratégia incremental)
----------------------------------------
Não recalculamos as 58M+ linhas todo dia -- só os pares (client_id,
bonus_id) que tiveram alguma linha tocada na carga incremental do dia
(df_fact_bonus, o mesmo DataFrame que processa_bonus() já extraiu e
carregou em fact_user_bonus). O fluxo é:

    1. Extrai os pares únicos (client_id, bonus_id) de df_fact_bonus.
    2. Sobe esses pares para inplay.stg_pares_bonus_impactados.
    3. Recalcula o GROUP BY em fact_user_bonus, FILTRADO só nesses pares
       (join com a staging acima) -- não escaneia a tabela inteira.
    4. Faz MERGE (upsert) do resultado em agg_bonus_concessoes.

Chamado dentro de processa_bonus() (consume_api.py), logo após o merge de
fact_user_bonus, recebendo o mesmo df_fact_bonus já extraído -- não faz
nenhuma extração adicional do Metabase.

BACKFILL INICIAL
------------------
Este módulo só cobre o que passa pela carga incremental A PARTIR DE quando
for ativado. O histórico já existente em fact_user_bonus (58M+ linhas)
precisa de UMA carga inicial completa, feita uma única vez, antes de
ativar o hook incremental -- ver instruções separadas de backfill inicial.
"""

import pandas as pd
import numpy as np
from database import ConnectionDB


def executar_agregacao_bonus_concessoes(df_fact_bonus: pd.DataFrame, cliente: str, db: str, logger):
    """
    Parâmetros
    ----------
    df_fact_bonus : DataFrame já extraído e carregado em fact_user_bonus
                    nesta rodada de processa_bonus() (contém ao menos as
                    colunas 'client_id' e 'bonus_id').
    cliente       : 'ZEROUM' ou 'ENERGIABET'.
    db            : valor de DB do ambiente ('redshift' ou 'postgres').
    logger        : logger do ETL principal.
    """
    try:
        if df_fact_bonus is None or df_fact_bonus.empty:
            logger.info("[AGG BONUS] Nenhuma linha na carga incremental. Agregação pulada.")
            return

        logger.info("[AGG BONUS] Iniciando atualização de agg_bonus_concessoes")

        # ------------------------------------------------------------------
        # 1. Pares (client_id, bonus_id) impactados nesta carga
        # ------------------------------------------------------------------
        pares_impactados = (
            df_fact_bonus[['client_id', 'bonus_id']]
            .dropna()
            .drop_duplicates()
            .reset_index(drop=True)
        )

        if pares_impactados.empty:
            logger.info("[AGG BONUS] Nenhum par client_id+bonus_id válido. Agregação pulada.")
            return

        logger.info(f"[AGG BONUS] {len(pares_impactados)} pares client_id+bonus_id impactados nesta carga")

        # ------------------------------------------------------------------
        # 2. Sobe para staging os pares impactados
        # ------------------------------------------------------------------
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.deleta_dados('inplay.stg_pares_bonus_impactados', "", logger)
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.insere_dados_bulk(
            'inplay.stg_pares_bonus_impactados', pares_impactados, logger,
            normalize_numpy=True
        )

        # ------------------------------------------------------------------
        # 3. Recalcula o agregado SOMENTE para os pares impactados
        # ------------------------------------------------------------------
        sql_agg = """
            SELECT
                f.client_id,
                f.bonus_id,
                MAX(f.partner_id)              AS partner_id,
                COUNT(*)                       AS qtd_concessoes,
                SUM(f.bonus_prize)              AS total_concedido,
                SUM(f.cost)                     AS custo_total,
                MAX(f.reuse_number)             AS max_reuso,
                SUM(f.turnover_amount_left)     AS rollover_pendente,
                MIN(f.awarding_time)            AS primeira_concessao,
                MAX(f.awarding_time)            AS ultima_concessao,
                MAX(f.source_updated_at)        AS ultima_atualizacao
            FROM inplay.fact_user_bonus f
            INNER JOIN inplay.stg_pares_bonus_impactados p
                ON p.client_id = f.client_id AND p.bonus_id = f.bonus_id
            GROUP BY f.client_id, f.bonus_id
        """
        ConnectionDB.conecta(db, cliente)
        df_agg = ConnectionDB.executa_script(sql_agg, logger)
        logger.info(f"[AGG BONUS] {len(df_agg)} pares recalculados a partir de fact_user_bonus")

        if df_agg.empty:
            logger.warning(
                "[AGG BONUS] Recalculo retornou vazio -- pares impactados não "
                "encontrados em fact_user_bonus. Verificar se o merge de "
                "fact_user_bonus rodou antes desta chamada."
            )
            return

        # NaN -> None: colunas de SUM (total_concedido, custo_total,
        # rollover_pendente) retornam NULL do SQL quando TODOS os
        # registros do par têm o campo de origem nulo -- pd.read_sql
        # representa isso como NaN. Sem essa conversão, psycopg2 tenta
        # gravar o literal "nan" numa coluna NUMERIC e o Redshift rejeita
        # (achado em produção, 21/ago/2026: "invalid input syntax for
        # type numeric: nan", 100% das execuções falhando). Mesmo padrão
        # aplicado em toda extração de consume_api.py antes de
        # insere_dados_bulk -- este módulo tinha ficado de fora.
        df_agg = df_agg.replace({np.nan: None})

        # ------------------------------------------------------------------
        # 4. Merge no destino (upsert por client_id + bonus_id)
        # ------------------------------------------------------------------
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.deleta_dados('inplay.stg_agg_bonus_concessoes', "", logger)
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.insere_dados_bulk(
            'inplay.stg_agg_bonus_concessoes', df_agg, logger,
            normalize_numpy=True
        )
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.mergeia_dados(
            'inplay.stg_agg_bonus_concessoes', 'inplay.agg_bonus_concessoes',
            df_agg, ['client_id', 'bonus_id'], logger
        )

        logger.info("[AGG BONUS] agg_bonus_concessoes atualizada com sucesso")

    except Exception as e:
        logger.error(f"[AGG BONUS] Erro na atualização de agg_bonus_concessoes: {e}")
        # Não relança -- uma falha na agregação NÃO deve derrubar a carga de
        # fact_user_bonus, que já terminou com sucesso quando este módulo é
        # chamado. Fica registrado no log para investigação, mas a carga
        # principal de bônus é considerada bem-sucedida de qualquer forma.