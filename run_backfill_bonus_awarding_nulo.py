"""
run_backfill_bonus_awarding_nulo.py
=====================================
Script standalone para rodar o backfill suplementar de inplay.fact_user_bonus,
cobrindo os ClientBonus com AwardingTime NULO (achado real: ~9,6M de
registros, majoritariamente Status=6 / BonusType 12,14,15 -- freebet,
freespin, riskfree bet, que não passam por uma etapa explícita de
"ativação").

Esses registros NUNCA são capturados pelo backfill principal
(processa_bonus_backfill), porque aquele filtra por AwardingTime -- uma
comparação >=/< contra NULL sempre avalia UNKNOWN em SQL, então a linha é
descartada silenciosamente, não importa quantas vezes o backfill rode.

Pré-requisito: os patches em consume_api.py descritos na conversa já
aplicados:
  - _sql_fact_user_bonus(..., filtro_extra="")
  - extrai_fact_user_bonus_por_periodo(..., filtro_extra="")
  - processa_bonus(..., campo_filtro_override=None, filtro_extra="")
  - _perfil_diario_bonus_generico(...)
  - processa_bonus_backfill_awarding_nulo(...)
  - _valida_bloco_bonus_awarding_nulo(...)

Uso:
    python run_backfill_bonus_awarding_nulo.py --cliente ZEROUM
    python run_backfill_bonus_awarding_nulo.py --cliente ZEROUM --teste
    python run_backfill_bonus_awarding_nulo.py --cliente ZEROUM \
        --data-inicio 2025-04-11T00:00:00 --data-corte 2026-08-13T23:59:59

Retomada após falha:
    O log de erro (arquivo de log do ETL + e-mail de alerta) traz a data
    exata para retomar, ex.:

    python run_backfill_bonus_awarding_nulo.py --cliente ZEROUM \
        --data-inicio 2025-06-03T00:00:00 --data-corte 2026-08-13T23:59:59
"""

import argparse
from datetime import datetime
from consume_api import ConsumeAPI


DATA_INICIO_PADRAO = "2025-04-11T00:00:00"   # MIN(CreationTime) confirmado onde AwardingTime IS NULL
DATA_CORTE_PADRAO = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")  # até agora


def main():
    parser = argparse.ArgumentParser(
        description="Backfill suplementar de fact_user_bonus (AwardingTime NULO)"
    )
    parser.add_argument(
        "--cliente", required=True, choices=["ZEROUM", "ENERGIABET"],
        help="Cliente a processar"
    )
    parser.add_argument(
        "--data-inicio", default=DATA_INICIO_PADRAO,
        help=f"Data ISO de início do backfill (default: {DATA_INICIO_PADRAO}, "
             f"MIN(CreationTime) já confirmado para AwardingTime IS NULL)"
    )
    parser.add_argument(
        "--data-corte", default=DATA_CORTE_PADRAO,
        help=f"Data ISO de corte do backfill (default: agora = {DATA_CORTE_PADRAO})"
    )
    parser.add_argument(
        "--teste", action="store_true",
        help="Roda só uma janela pequena (4 dias a partir de --data-inicio) "
             "para validar a mecânica antes de rodar o range completo"
    )
    parser.add_argument(
        "--sem-validacao", action="store_true",
        help="Desliga a validação por bloco (_valida_bloco_bonus_awarding_nulo). "
             "Não recomendado -- só use se a validação estiver com falso positivo "
             "conhecido e você já confirmou manualmente."
    )
    parser.add_argument(
        "--limite-linhas-janela", type=int, default=900000,
        help="Teto de linhas por janela (default 900000, com margem sob o "
             "teto real do Metabase de 1.048.575)"
    )
    args = parser.parse_args()

    if args.teste:
        inicio_dt = datetime.fromisoformat(args.data_inicio)
        data_corte_teste = (inicio_dt.replace(
            day=min(inicio_dt.day + 4, 28)
        )).strftime("%Y-%m-%dT%H:%M:%S")
        data_inicio = args.data_inicio
        data_corte = data_corte_teste
        print(f"[MODO TESTE] Rodando apenas {data_inicio} -> {data_corte}")
    else:
        data_inicio = args.data_inicio
        data_corte = args.data_corte

    cliente_dispatch = f"{args.cliente}_BACKFILL_BONUS_AWARDING_NULO"

    print(
        f"Iniciando backfill suplementar (AwardingTime NULO)\n"
        f"  cliente:     {args.cliente}\n"
        f"  cliente (dispatch __init__): {cliente_dispatch}\n"
        f"  data_inicio: {data_inicio}\n"
        f"  data_corte:  {data_corte}\n"
        f"  validar_blocos: {not args.sem_validacao}\n"
        f"  limite_linhas_por_janela: {args.limite_linhas_janela}\n"
    )

    # IMPORTANTE: NÃO instanciar ConsumeAPI(cliente='ZEROUM') aqui -- isso
    # dispara self.principal_zeroum() (o pipeline horário INTEIRO) como
    # efeito colateral do __init__. Usa o cliente de dispatch dedicado
    # ('..._BACKFILL_BONUS_AWARDING_NULO'), que chama só
    # processa_bonus_backfill_awarding_nulo() -- ver __init__ em consume_api.py.
    #
    # validar_blocos e limite_linhas_por_janela usam os defaults do método
    # (validar_blocos=True, limite_linhas_por_janela=900000) porque o
    # dispatch via __init__ não repassa esses parâmetros extras. Se precisar
    # de --sem-validacao ou --limite-linhas-janela diferente do default,
    # chame processa_bonus_backfill_awarding_nulo diretamente (ver bloco
    # comentado abaixo) em vez de instanciar por cliente.
    ConsumeAPI(
        cliente=cliente_dispatch,
        data_inicial_backfill=data_inicio,
        data_final=data_corte,
    )

    # --- Alternativa para customizar validar_blocos/limite_linhas_por_janela ---
    # etl = ConsumeAPI.__new__(ConsumeAPI)  # cria o objeto SEM rodar __init__
    # etl.engine_zro1bet = ConsumeAPI.create_engine_zro1bet()
    # etl.engine_dw = ConsumeAPI.create_engine_dw()
    # (... replicar setup de logger/db_logger do __init__ ...)
    # etl.processa_bonus_backfill_awarding_nulo(
    #     cliente=args.cliente,
    #     data_inicio_historico=data_inicio,
    #     data_corte=data_corte,
    #     limite_linhas_por_janela=args.limite_linhas_janela,
    #     validar_blocos=not args.sem_validacao,
    # )

    print("Backfill suplementar concluído. Rode a conferência final:")
    print("  SELECT COUNT(DISTINCT id) FROM inplay.fact_user_bonus;")
    print("  -- esperado: 54.717.693 (ou o total atual da origem, se mudou)")


if __name__ == "__main__":
    main()