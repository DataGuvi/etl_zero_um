"""
backfill_agregacao_dim_usuario_zeroum.py
=========================================
Backfill historico para GARANTIR A INTEGRIDADE das tabelas agg_* do
cliente ZEROUM apos um periodo em que a agregacao incremental ficou
sem carregar corretamente (delay no deploy em producao).

Diferente da carga incremental normal, este script:
  1) Reprocessa TODOS os usuarios de inplay.dim_usuario (ZEROUM), nao so
     os que tiveram atividade recente — cobre qualquer buraco deixado
     pelo periodo de delay, independente de saber exatamente quais dias
     falharam.
  2) Salva um CHECKPOINT local (JSON) a cada lote concluido, para poder
     ser interrompido e retomado sem reprocessar o que ja foi feito.
  3) Ao final, roda uma CHECAGEM DE INTEGRIDADE comparando a quantidade
     de usuarios em dim_usuario x agg_usuario_metricas x
     agg_usuario_reativacao, para confirmar que ninguem ficou de fora.

Uso
---
    # primeira execucao (ou reiniciar do zero)
    python backfill_agregacao_dim_usuario_zeroum.py

    # ajustar tamanho do lote (default 20000)
    python backfill_agregacao_dim_usuario_zeroum.py --lote-tamanho 15000

    # se o processo foi interrompido (queda de conexao, terminal fechado,
    # etc.), rode de novo sem nenhuma flag extra: ele detecta o checkpoint
    # e continua do lote seguinte automaticamente.

    # para forcar reprocessar tudo do zero, ignorando o checkpoint salvo:
    python backfill_agregacao_dim_usuario_zeroum.py --reiniciar
"""

import argparse
import json
import logging
import os
from logging.handlers import RotatingFileHandler

import pandas as pd

from database import ConnectionDB
from config import DB
from agregacao_dim_usuario import (
    SQL_STG_FATO_DIARIA,
    SQL_MERGE_FATO_DIARIA,
    SQL_STG_METRICAS,
    SQL_MERGE_METRICAS,
    SQL_RECORRENCIA,
    SQL_STG_REATIVACAO,
    SQL_UPDATE_REATIVACAO,
    SQL_INSERT_REATIVACAO,
)

CLIENTE = "ZEROUM"
CHECKPOINT_PATH = f"backfill_checkpoint_{CLIENTE}.json"


def get_logger():
    handler = RotatingFileHandler(
        f"backfill_agregacao_{CLIENTE}.log", maxBytes=10 * 1024 * 1024, backupCount=5
    )
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    logger = logging.getLogger(f"backfill_agregacao_{CLIENTE}")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logging.basicConfig(handlers=[handler], level=logging.INFO)
    return logger


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

def carrega_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def salva_checkpoint(lote_concluido: int, total_lotes: int, total_usuarios: int):
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "cliente": CLIENTE,
                "lote_concluido": lote_concluido,
                "total_lotes": total_lotes,
                "total_usuarios": total_usuarios,
            },
            f,
        )


def limpa_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)


# ---------------------------------------------------------------------------
# Extracao e processamento
# ---------------------------------------------------------------------------

def busca_todos_usuarios(logger) -> pd.DataFrame:
    logger.info("[BACKFILL] Lendo todos os usuarios de dim_usuario (ZEROUM)")
    ConnectionDB.conecta(DB, CLIENTE)
    ids = ConnectionDB.recupera_dados("inplay.dim_usuario", "id", "")
    df_todos = pd.DataFrame(ids, columns=["usuario"])
    df_todos["usuario"] = df_todos["usuario"].dropna().astype(int)
    df_todos = df_todos.sort_values("usuario").reset_index(drop=True)
    logger.info(f"[BACKFILL] {len(df_todos)} usuarios encontrados em dim_usuario (ZEROUM)")
    return df_todos


def popula_impactados(df_lote: pd.DataFrame, logger):
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.deleta_dados("inplay.stg_usuarios_impactados", "", logger)

    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.insere_dados_bulk(
        "inplay.stg_usuarios_impactados", df_lote, logger, normalize_numpy=True
    )


def roda_lote(df_lote: pd.DataFrame, logger, lote_idx: int, total_lotes: int):
    logger.info(f"[BACKFILL] Lote {lote_idx}/{total_lotes} (ZEROUM) - {len(df_lote)} usuarios")

    popula_impactados(df_lote, logger)

    # Passo 1 - fato diaria fisica
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.deleta_dados("inplay.stg_fact_user_atividade_diaria", "", logger)
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.executa_dml(SQL_STG_FATO_DIARIA, logger)
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.executa_dml(SQL_MERGE_FATO_DIARIA, logger)

    # Passo 2 - metricas acumuladas
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.deleta_dados("inplay.stg_agg_usuario_metricas", "", logger)
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.executa_dml(SQL_STG_METRICAS, logger)
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.executa_dml(SQL_MERGE_METRICAS, logger)

    # Passo 4 - reativacao
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.deleta_dados("inplay.stg_agg_usuario_reativacao", "", logger)
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.executa_dml(SQL_STG_REATIVACAO, logger)
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.executa_dml(SQL_UPDATE_REATIVACAO, logger)
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.executa_dml(SQL_INSERT_REATIVACAO, logger)

    logger.info(f"[BACKFILL] Lote {lote_idx}/{total_lotes} (ZEROUM) concluido")


def checagem_integridade(logger):
    """
    Confere se sobrou algum usuario de dim_usuario sem linha em
    agg_usuario_metricas ou agg_usuario_reativacao apos o backfill.
    """
    logger.info("[BACKFILL] Rodando checagem de integridade")
    ConnectionDB.conecta(DB, CLIENTE)
    script = """
        SELECT
            (SELECT COUNT(*) FROM inplay.dim_usuario)                    AS total_dim_usuario,
            (SELECT COUNT(*) FROM inplay.agg_usuario_metricas)           AS total_agg_metricas,
            (SELECT COUNT(*) FROM inplay.agg_usuario_reativacao)         AS total_agg_reativacao,
            (SELECT COUNT(*) FROM inplay.dim_usuario d
                WHERE NOT EXISTS (
                    SELECT 1 FROM inplay.agg_usuario_metricas m
                    WHERE m.usuario = d.id
                ))                                                       AS faltantes_em_metricas,
            (SELECT COUNT(*) FROM inplay.dim_usuario d
                WHERE NOT EXISTS (
                    SELECT 1 FROM inplay.agg_usuario_reativacao r
                    WHERE r.usuario = d.id
                ))                                                       AS faltantes_em_reativacao
    """
    df = ConnectionDB.executa_script(script, logger)
    print("\n=== CHECAGEM DE INTEGRIDADE (ZEROUM) ===")
    print(df.to_string(index=False))
    logger.info(f"[BACKFILL] Resultado da checagem: {df.to_dict(orient='records')[0]}")

    faltantes_metricas = int(df.iloc[0]["faltantes_em_metricas"])
    faltantes_reativacao = int(df.iloc[0]["faltantes_em_reativacao"])

    if faltantes_metricas > 0 or faltantes_reativacao > 0:
        logger.warning(
            "[BACKFILL] AINDA HA USUARIOS SEM LINHA NAS TABELAS AGG "
            f"(metricas={faltantes_metricas}, reativacao={faltantes_reativacao}). "
            "Rode o script novamente para cobrir os faltantes."
        )
        print(
            f"\nATENCAO: ainda faltam {faltantes_metricas} usuarios em agg_usuario_metricas "
            f"e {faltantes_reativacao} em agg_usuario_reativacao. Rode novamente."
        )
    else:
        logger.info("[BACKFILL] Integridade OK: todos os usuarios de dim_usuario estao cobertos.")
        print("\nIntegridade OK: todos os usuarios de dim_usuario estao cobertos nas tabelas agg.")


def roda_backfill(logger, lote_tamanho: int, reiniciar: bool):
    if reiniciar:
        limpa_checkpoint()

    checkpoint = carrega_checkpoint()

    df_todos = busca_todos_usuarios(logger)
    if df_todos.empty:
        logger.warning("[BACKFILL] Nenhum usuario encontrado para ZEROUM. Nada a fazer.")
        return

    total_lotes = (len(df_todos) // lote_tamanho) + (1 if len(df_todos) % lote_tamanho else 0)

    lote_inicial = 0
    if checkpoint and checkpoint.get("total_usuarios") == len(df_todos) and checkpoint.get("total_lotes") == total_lotes:
        lote_inicial = checkpoint.get("lote_concluido", 0)
        logger.info(
            f"[BACKFILL] Checkpoint encontrado: retomando a partir do lote {lote_inicial + 1}/{total_lotes}"
        )
        print(f"Retomando do checkpoint: lote {lote_inicial + 1}/{total_lotes}")
    else:
        if checkpoint:
            logger.warning(
                "[BACKFILL] Checkpoint encontrado mas base de usuarios mudou "
                "(total_usuarios/total_lotes diferente). Reiniciando do zero."
            )
        limpa_checkpoint()

    for i in range(lote_inicial, total_lotes):
        inicio = i * lote_tamanho
        fim = inicio + lote_tamanho
        df_lote = df_todos.iloc[inicio:fim]

        roda_lote(df_lote, logger, i + 1, total_lotes)
        salva_checkpoint(i + 1, total_lotes, len(df_todos))

    # Recorrencia 30 dias: full recompute, roda uma unica vez ao final
    logger.info("[BACKFILL] Recalculando agg_usuario_recorrencia_30d (ZEROUM)")
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.deleta_dados("inplay.agg_usuario_recorrencia_30d", "", logger)
    ConnectionDB.conecta(DB, CLIENTE)
    ConnectionDB.executa_dml(SQL_RECORRENCIA, logger)

    checagem_integridade(logger)

    limpa_checkpoint()
    logger.info(f"[BACKFILL] Concluido para ZEROUM ({len(df_todos)} usuarios, {total_lotes} lotes)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill historico das tabelas agg_* de dim_usuario (ZEROUM), com checkpoint"
    )
    parser.add_argument(
        "--lote-tamanho",
        type=int,
        default=20000,
        help="Quantidade de usuarios processados por lote (default: 20000)",
    )
    parser.add_argument(
        "--reiniciar",
        action="store_true",
        help="Ignora checkpoint salvo e reprocessa tudo do zero",
    )
    args = parser.parse_args()

    logger = get_logger()

    try:
        roda_backfill(logger, args.lote_tamanho, args.reiniciar)
    except Exception as e:
        logger.error(f"[BACKFILL] Erro ao rodar backfill ZEROUM: {e}")
        print(
            f"\nERRO: {e}\n"
            f"O checkpoint foi salvo no ultimo lote concluido ({CHECKPOINT_PATH}). "
            "Rode o script novamente para retomar de onde parou."
        )
        raise
