import argparse
from consume_api import ConsumeAPI

class Principal:
    def __init__(self):
        parser = argparse.ArgumentParser(description="ETL")
        parser.add_argument("--cliente", required=True, help="cliente a qual os dados pertencem")
        args = parser.parse_args()
        self.consume_api = ConsumeAPI(cliente=args.cliente)

if __name__ == "__main__":
    inicio = Principal()