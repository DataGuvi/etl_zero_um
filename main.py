import argparse
from consume_api import ConsumeAPI

class Principal:
    def __init__(self):
        parser = argparse.ArgumentParser(description="ETL")
        parser.add_argument("--cliente", required=True, help="cliente a qual os dados pertencem")
        parser.add_argument("--data-inicial-backfill", dest="data_inicial_backfill", default=None,
                             help="Data inicial (YYYY-MM-DDTHH:MM:SS) para o backfill único de dim_usuario")
        parser.add_argument("--ids-backfill", dest="ids_backfill", default=None,
                             help="Lista de ids separados por vírgula para regularização pontual de dim_usuario (ex.: 123,456,789)")
        args = parser.parse_args()

        ids_backfill = None
        if args.ids_backfill:
            ids_backfill = [int(i.strip()) for i in args.ids_backfill.split(",") if i.strip()]

        self.consume_api = ConsumeAPI(
            cliente=args.cliente,
            data_inicial_backfill=args.data_inicial_backfill,
            ids_backfill=ids_backfill,
        )

if __name__ == "__main__":
    inicio = Principal()