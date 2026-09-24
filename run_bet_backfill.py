"""
run_bet_backfill.py
====================
Roda o backfill historico de Bet (fact_bet) para ZEROUM ou ENERGIABET.

Diferenca importante em relacao ao backfill de Bonus: os blocos sao
processados do MAIS RECENTE para o MAIS ANTIGO (ver processa_bet_backfill
em consume_api.py) -- decisao deliberada para permitir que o cron
incremental comece a rodar sobre dados recentes mais cedo, mesmo com
meses antigos ainda sendo carregados.

100% nao-interativo (sem input()) -- seguro para rodar com `*>` ou
Start-Process, sem risco de travar esperando teclado num terminal que
nao esta visivel.

Passo unico (diferente do backfill de Bonus, que tem uma segunda passada
suplementar para AwardingTime nulo): nao ha equivalente identificado para
Bet ate o momento. Se surgir, tratar em script/metodo separado.

USO
---

1) Descobrir a data de inicio sugerida pela origem, sem gravar nada:
    python run_bet_backfill.py --cliente ZEROUM

2) Rodar de verdade, usando a sugestao automatica da origem:
    python run_bet_backfill.py --cliente ZEROUM --usar-sugestao --confirmo

3) Rodar de verdade, com data de inicio explicita (ZEROUM ja confirmado
   nesta investigacao: 2025-08-29T03:11:00):
    python run_bet_backfill.py --cliente ZEROUM --data-inicio 2025-08-29T03:11:00 --confirmo

4) ENERGIABET (partner_id=181, default automatico -- data de inicio ainda
   nao foi calculada, usar --usar-sugestao na primeira vez):
    python run_bet_backfill.py --cliente ENERGIABET --usar-sugestao --confirmo

5) Com data de corte customizada (default: agora):
    python run_bet_backfill.py --cliente ZEROUM --usar-sugestao --data-corte 2026-08-01T00:00:00 --confirmo

Em segundo plano (PowerShell):
    Start-Process python -ArgumentList "run_bet_backfill.py --cliente ZEROUM --usar-sugestao --confirmo" `
        -RedirectStandardOutput backfill_bet_zeroum.log `
        -RedirectStandardError backfill_bet_zeroum_err.log `
        -NoNewWindow

Acompanhar:
    Get-Content backfill_bet_zeroum.log -Wait
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

PARTNER_ID_PADRAO = {'ZEROUM': 180, 'ENERGIABET': 181}
DATABASE_ID = {'ZEROUM': 67, 'ENERGIABET': 103}  # MetabaseDatabase.ClickhousePartnerZeroum/Energiabet


def monta_instancia(cliente):
    """
    Monta uma instancia de ConsumeAPI com o setup real (logger + db_logger),
    SEM passar pelo __init__ completo -- este roda o dispatch de cliente
    normal, que dispararia uma carga diferente dependendo do valor de
    `cliente` (ex.: principal_zeroum).
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

    api.db_logger = DBLogger(cliente=cliente)
    return api


def sugere_data_inicio(api, cliente, partner_id):
    print("Consultando a origem para sugerir a data de inicio do historico...")
    auth_id = api.conection(cliente)
    sql = f"""
        SELECT min(BetTime) AS data_minima, count(*) AS total_linhas
        FROM Bet
        WHERE _peerdb_is_deleted = 0 AND ProductId != 6 AND PartnerId = {partner_id}
    """
    csv = api.extrai_csv_nativo(auth_id, DATABASE_ID[cliente], sql, timeout=300)
    df = pd.read_csv(io.BytesIO(csv))
    data_minima = df.iloc[0]['data_minima']
    total = df.iloc[0]['total_linhas']
    print(f"  MIN(BetTime) encontrado: {data_minima}")
    print(f"  Total de linhas (Bet, PartnerId={partner_id}): {total:,}")
    return data_minima


def main():
    parser = argparse.ArgumentParser(
        description="Backfill historico de Bet (nao-interativo) -- ZEROUM ou ENERGIABET"
    )
    parser.add_argument(
        "--cliente", required=True, choices=["ZEROUM", "ENERGIABET"],
        help="Cliente a processar."
    )
    parser.add_argument(
        "--data-inicio", default=None,
        help="Data de inicio do historico, ISO (ex: 2025-08-29T03:11:00). "
             "Se omitido, use --usar-sugestao para detectar automaticamente."
    )
    parser.add_argument(
        "--usar-sugestao", action="store_true",
        help="Usa automaticamente o MIN(BetTime) da origem como data de inicio."
    )
    parser.add_argument(
        "--data-corte", default=None,
        help="Data de corte (fim do historico), ISO. Default: meia-noite de "
             "ontem (dia atual - 1) -- NUNCA hoje, para nao pegar um dia "
             "ainda em andamento (ver achado real de 24/set/2026: um dia "
             "corrente pode terminar de acontecer DURANTE a extracao, que "
             "leva horas, causando carga incompleta)."
    )
    parser.add_argument(
        "--partner-id", type=int, default=None,
        help="PartnerId a usar. Default: 180 (ZEROUM) / 181 (ENERGIABET)."
    )
    parser.add_argument(
        "--limite-linhas-por-janela", type=int, default=950000,
        help="Teto de linhas por janela no modo otimizado (default 950000)."
    )
    parser.add_argument(
        "--confirmo", action="store_true",
        help="Confirma a gravacao real no banco. Sem essa flag, o script so "
             "mostra o plano (cliente, partner_id, data de inicio/corte, ordem) "
             "e sai, sem gravar nada."
    )
    args = parser.parse_args()

    cliente = args.cliente
    partner_id = args.partner_id if args.partner_id is not None else PARTNER_ID_PADRAO[cliente]

    api = monta_instancia(cliente)

    if args.data_inicio:
        data_inicio_historico = args.data_inicio
    elif args.usar_sugestao:
        sugestao = sugere_data_inicio(api, cliente, partner_id)
        data_inicio_historico = str(sugestao)[:19]
    else:
        print("Nenhuma --data-inicio informada.")
        sugestao = sugere_data_inicio(api, cliente, partner_id)
        print(f"\nSugestao encontrada: {sugestao}")
        print("Rode de novo com uma das opcoes:")
        print(f"  --cliente {cliente} --data-inicio {str(sugestao)[:19]} --confirmo")
        print(f"  --cliente {cliente} --usar-sugestao --confirmo")
        sys.exit(0)

    if args.data_corte:
        data_corte = args.data_corte
    else:
        # Default = meia-noite de HOJE -- limite EXCLUSIVO. Combinado com o
        # fix de _perfil_diario_bet (que trata meia-noite exata como "para
        # nesse dia, exclusive"), o último dia realmente incluído no perfil
        # fica sendo ontem (dia atual - 1), nunca hoje. Ver achado real de
        # 24/set/2026 no help de --data-corte acima.
        hoje_meia_noite = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        data_corte = hoje_meia_noite.strftime('%Y-%m-%dT%H:%M:%S')

    print(f"\nCliente: {cliente} (partner_id={partner_id})")
    print(f"Intervalo: {data_inicio_historico} ate {data_corte}")
    print("Ordem de processamento: MAIS RECENTE -> MAIS ANTIGO "
          "(cron incremental pode ligar sobre os meses recentes assim que terminarem, "
          "mesmo com meses antigos ainda em backfill).")
    print(f"limite_linhas_por_janela: {args.limite_linhas_por_janela}")

    if not args.confirmo:
        print("\nModo simulacao (sem --confirmo) -- NADA foi gravado.")
        print("Rode de novo adicionando --confirmo para executar de verdade.")
        sys.exit(0)

    print("\n" + "=" * 70)
    print(f"Backfill de Bet -- {cliente} (por BetTime, ordem mais recente -> mais antiga)")
    print("=" * 70)
    api.processa_bet_backfill(
        cliente=cliente,
        data_inicio_historico=data_inicio_historico,
        data_corte=data_corte,
        limite_linhas_por_janela=args.limite_linhas_por_janela,
        partner_id=partner_id,
    )

    print("\n" + "=" * 70)
    print("Backfill de Bet concluido. Confira:")
    print(f"  - inplay.etl_execution_logs, operation 'ETL_BET', cliente '{cliente}' -> status de cada janela")
    print("  - SELECT count(*) FROM inplay.fact_bet -- volume total carregado")
    print("=" * 70)


if __name__ == "__main__":
    main()
