"""
run_bonus_backfill_energiabet.py
==================================
Roda o backfill histórico COMPLETO de Bônus para ENERGIABET -- os dois
passos obrigatórios, em sequência:

  1) processa_bonus_backfill                -- carga principal, filtrada por AwardingTime
  2) processa_bonus_backfill_awarding_nulo   -- suplementar, captura bônus
     (freebet/freespin/riskfree) que NUNCA têm AwardingTime preenchido e
     por isso o passo 1 sozinho não pega.

100% não-interativo (sem input()) -- seguro para rodar com `*>` ou
Start-Process, sem risco de travar esperando teclado num terminal que
não está visível.

USO
---

1) Descobrir a data de início sugerida pela origem, sem gravar nada:
    python run_bonus_backfill_energiabet.py

2) Rodar de verdade, usando a sugestão automática da origem:
    python run_bonus_backfill_energiabet.py --usar-sugestao --confirmo

3) Rodar de verdade, com data de início explícita:
    python run_bonus_backfill_energiabet.py --data-inicio 2025-08-24T00:00:00 --confirmo

4) Com data de corte customizada (default: agora):
    python run_bonus_backfill_energiabet.py --usar-sugestao --data-corte 2026-08-01T00:00:00 --confirmo

Em segundo plano (PowerShell):
    Start-Process python -ArgumentList "run_bonus_backfill_energiabet.py --usar-sugestao --confirmo" `
        -RedirectStandardOutput backfill_bonus_energiabet.log `
        -RedirectStandardError backfill_bonus_energiabet_err.log `
        -NoNewWindow

Acompanhar:
    Get-Content backfill_bonus_energiabet.log -Wait
"""
import io
import sys
import logging
import argparse
import pandas as pd
from datetime import datetime
from logging.handlers import RotatingFileHandler

from consume_api import ConsumeAPI
from db_logger import DBLogger

PARTNER_ID_ENERGIABET = 181
DATABASE_ENERGIABET = 103  # MetabaseDatabase.ClickhousePartnerEnergiabet
CLIENTE = 'ENERGIABET'


def monta_instancia():
    """
    Monta uma instância de ConsumeAPI com o setup real (logger + db_logger),
    SEM passar pelo __init__ completo -- este roda o dispatch de cliente
    normal, que dispararia uma carga diferente (ex.: principal_energiabet)
    dependendo do valor de `cliente`.
    """
    api = ConsumeAPI.__new__(ConsumeAPI)

    handler = RotatingFileHandler('etl.log', maxBytes=10 * 1024 * 1024, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    api.logger = logging.getLogger(__name__)
    api.logger.setLevel(logging.INFO)
    if not api.logger.handlers:
        api.logger.addHandler(handler)
        api.logger.addHandler(logging.StreamHandler(sys.stdout))
    logging.basicConfig(handlers=[handler], level=logging.INFO)

    api.db_logger = DBLogger(cliente=CLIENTE)
    return api


def sugere_data_inicio(api):
    print("Consultando a origem para sugerir a data de inicio do historico...")
    auth_id = api.conection(CLIENTE)
    sql = f"""
        SELECT min(CreationTime) AS data_minima, count(*) AS total_linhas
        FROM ClientBonus
        WHERE _peerdb_is_deleted = 0 AND PartnerId = {PARTNER_ID_ENERGIABET}
    """
    csv = api.extrai_csv_nativo(auth_id, DATABASE_ENERGIABET, sql, timeout=300)
    df = pd.read_csv(io.BytesIO(csv))
    data_minima = df.iloc[0]['data_minima']
    total = df.iloc[0]['total_linhas']
    print(f"  MIN(CreationTime) encontrado: {data_minima}")
    print(f"  Total de linhas (ClientBonus, PartnerId={PARTNER_ID_ENERGIABET}): {total:,}")
    return data_minima


def main():
    parser = argparse.ArgumentParser(
        description="Backfill historico de Bonus - ENERGIABET (nao-interativo)"
    )
    parser.add_argument(
        "--data-inicio", default=None,
        help="Data de inicio do historico, ISO (ex: 2025-08-24T00:00:00). "
             "Se omitido, use --usar-sugestao para detectar automaticamente."
    )
    parser.add_argument(
        "--usar-sugestao", action="store_true",
        help="Usa automaticamente o MIN(CreationTime) da origem como data de inicio."
    )
    parser.add_argument(
        "--data-corte", default=None,
        help="Data de corte (fim do historico), ISO. Default: agora."
    )
    parser.add_argument(
        "--confirmo", action="store_true",
        help="Confirma a gravacao real no banco. Sem essa flag, o script so "
             "mostra o plano (data de inicio/corte) e sai, sem gravar nada."
    )
    args = parser.parse_args()

    api = monta_instancia()

    if args.data_inicio:
        data_inicio_historico = args.data_inicio
    elif args.usar_sugestao:
        sugestao = sugere_data_inicio(api)
        data_inicio_historico = str(sugestao)[:19]
    else:
        print("Nenhuma --data-inicio informada.")
        sugestao = sugere_data_inicio(api)
        print(f"\nSugestao encontrada: {sugestao}")
        print("Rode de novo com uma das opcoes:")
        print(f"  --data-inicio {str(sugestao)[:19]} --confirmo")
        print("  --usar-sugestao --confirmo")
        sys.exit(0)

    data_corte = args.data_corte if args.data_corte else datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    print(f"\nIntervalo: {data_inicio_historico} ate {data_corte}")

    if not args.confirmo:
        print("\nModo simulacao (sem --confirmo) -- NADA foi gravado.")
        print("Rode de novo adicionando --confirmo para executar de verdade.")
        sys.exit(0)

    print("\n" + "=" * 70)
    print("PASSO 1/2 - processa_bonus_backfill (carga principal, por AwardingTime)")
    print("=" * 70)
    api.processa_bonus_backfill(
        cliente=CLIENTE,
        data_inicio_historico=data_inicio_historico,
        data_corte=data_corte,
        partner_id=PARTNER_ID_ENERGIABET,
    )

    print("\n" + "=" * 70)
    print("PASSO 2/2 - processa_bonus_backfill_awarding_nulo (suplementar, por CreationTime)")
    print("=" * 70)
    api.processa_bonus_backfill_awarding_nulo(
        cliente=CLIENTE,
        data_inicio_historico=data_inicio_historico,
        data_corte=data_corte,
        partner_id=PARTNER_ID_ENERGIABET,
    )

    print("\n" + "=" * 70)
    print("Backfill de Bonus concluido (2/2 passos). Confira:")
    print("  - inplay.etl_execution_logs (dlenergiabet), operations "
          "'ETL_ENERGIABET_BONUS' -> status de cada janela")
    print("  - SELECT count(*) FROM inplay.fact_user_bonus -- volume total carregado")
    print("=" * 70)


if __name__ == "__main__":
    main()