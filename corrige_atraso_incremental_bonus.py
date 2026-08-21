"""
corrige_atraso_incremental_bonus.py
=====================================
Backfill de segurança para fechar o gap causado pelo cursor de
source_updated_at ter sido "empurrado" para frente pelo backfill
suplementar de AwardingTime NULL (ver conversa) -- o MAX(source_updated_at)
não refletia de fato até onde o incremental normal tinha processado.

Cobre uma janela EXPLÍCITA (AwardingTime), sem depender do cursor.
Depois de rodar isso, o processa_bonus('ZEROUM', modo='incremental')
volta a ser confiável, porque o cursor vai refletir uma carga real.
"""

import argparse
from datetime import datetime, timedelta
from consume_api import ConsumeAPI


def main():
    parser = argparse.ArgumentParser(description="Backfill de segurança - fecha atraso do incremental de bônus")
    parser.add_argument("--cliente", required=True, choices=["ZEROUM", "ENERGIABET"])
    parser.add_argument(
        "--dias-margem", type=int, default=30,
        help="Quantos dias para trás cobrir, a partir de hoje (default: 30, "
             "margem generosa para garantir que nada ficou de fora)"
    )
    args = parser.parse_args()

    hoje = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    data_final = hoje - timedelta(days=1)
    data_inicio = data_final - timedelta(days=args.dias_margem)

    data_inicio_str = data_inicio.strftime("%Y-%m-%dT00:00:00")
    data_final_str = data_final.strftime("%Y-%m-%dT%H:%M:%S")

    print(
        f"Rodando backfill de segurança (via AwardingTime, NÃO via cursor incremental)\n"
        f"  cliente: {args.cliente}\n"
        f"  janela:  {data_inicio_str} -> {data_final_str}\n"
    )

    # Usa o dispatch de backfill que JÁ existe (processa_bonus_backfill),
    # o mesmo usado na carga histórica original -- filtra por AwardingTime,
    # então não depende (e não é enganado) pelo source_updated_at.
    #
    # IMPORTANTE: não instanciar ConsumeAPI(cliente='ZEROUM') puro -- isso
    # dispara principal_zeroum() (pipeline horário inteiro) como efeito
    # colateral do __init__. Criamos o objeto sem rodar __init__ e montamos
    # só o necessário.
    etl = ConsumeAPI.__new__(ConsumeAPI)
    etl.engine_zro1bet = etl.create_engine_zro1bet()
    etl.engine_dw = etl.create_engine_dw()

    import logging
    from logging.handlers import RotatingFileHandler
    handler = RotatingFileHandler('etl.log', maxBytes=10 * 1024 * 1024, backupCount=5)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    etl.logger = logging.getLogger(__name__)
    etl.logger.setLevel(logging.INFO)
    etl.logger.addHandler(handler)
    logging.basicConfig(handlers=[handler], level=logging.INFO)

    from db_logger import DBLogger
    etl.db_logger = DBLogger(cliente=args.cliente)

    etl.processa_bonus_backfill(
        cliente=args.cliente,
        data_inicio_historico=data_inicio_str,
        data_corte=data_final_str,
    )

    print("Backfill de segurança concluído. Revalide:")
    print("  SELECT COUNT(DISTINCT id) FROM inplay.fact_user_bonus;")
    print("  SELECT MAX(source_updated_at) FROM inplay.fact_user_bonus;")


if __name__ == "__main__":
    main()