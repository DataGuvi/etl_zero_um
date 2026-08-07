"""
backfill_historico_unificado.py
================================
Carga histórica ÚNICA para inplay.dim_usuario — substitui os antigos
backfill_frente_a.py e backfill_frente_b_protegido.py, que faziam duas
extrações e dois UPDATEs separados a partir do MESMO card.

Por que unificar (pedido explícito): as 11 colunas (8 da Frente A + 3
colunas protegidas da Frente B) vêm todas do mesmo SELECT do card. Extrair
uma vez só e gravar tudo junto na mesma stg evita duas idas ao Metabase e
duas passadas de UPDATE em dim_usuario — mais rápido e mais simples de
operar numa base de 9M+ usuários.

Por que em janelas de tempo (chunks): o Metabase trunca silenciosamente a
exportação de CSV em LIMITE_LINHAS_METABASE = 1.048.575 linhas (constante
já existente em consume_api.py, usada em extrai_dados_card_por_periodo).
Numa base de 9M+ usuários, uma extração única (data_inicial=0, data_final=0)
retornaria só uma fração do histórico, sem erro visível. Este script
subdivide o intervalo pedido em janelas de tempo por registration_date,
dobrando a divisão sempre que uma janela: (a) retornar >= o teto de linhas,
ou (b) falhar por timeout/conexão — mesmo padrão já usado em
extrai_dados_card_por_periodo, adaptado para GRAVAR cada janela na stg
imediatamente em vez de acumular tudo em memória (9M+ linhas em um único
DataFrame arriscaria estourar memória).

Não aplica decrypt em nenhum momento (Frente A nunca precisou; Frente B
grava propositalmente o valor cru/cifrado nas colunas "_protegido").

Uso:
    python backfill_historico_unificado.py --cliente ZEROUM \\
        --card-id card__21517 \\
        --data-inicio 2015-01-01 --data-fim 2026-08-01

    # Em produção, depois da Fase 7: --card-id card__14826
"""

import argparse
from datetime import datetime, timedelta

import pandas as pd

from database import ConnectionDB
from db_logger import DBLogger
from enums import MetabaseDatabase
from config import DB
from consume_api import ConsumeAPI


LIMITE_LINHAS_METABASE = 1048575  # mesmo teto usado em extrai_dados_card_por_periodo
JANELA_MINIMA = timedelta(hours=1)  # abaixo disso, para de subdividir e avisa

COLUNAS_FRENTE_A = ['LastName', 'TaxNumber', 'DocumentType', 'DocumentNumber',
                     'DocumentIssuedBy', 'IsDocumentVerified', 'KYCStatus', 'KYCDocsStatus']
COLUNAS_FRENTE_B_ORIGEM = ['first_name', 'birth_date', 'mobile_number']
COLUNAS_ORIGEM = ['id'] + COLUNAS_FRENTE_A + COLUNAS_FRENTE_B_ORIGEM

RENAME_FRENTE_B = {
    'first_name': 'first_name_protegido',
    'birth_date': 'birth_date_protegido',
    'mobile_number': 'mobile_number_protegido',
}


def _fmt(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S')


def processa_janela(api, auth_id, id_database, id_card, cliente,
                     data_inicial_dt, data_final_dt, logger, contador):
    """
    Extrai uma janela [data_inicial_dt, data_final_dt) do card. Se o
    resultado estourar o teto do Metabase, ou a chamada falhar por
    timeout/conexão, subdivide a janela ao meio e chama a si mesma
    recursivamente para cada metade -- mesma estratégia de
    extrai_dados_card_por_periodo (já validada em produção para outro
    fluxo), adaptada para gravar direto na stg em vez de devolver o
    DataFrame para quem chamou.
    """
    duracao = data_final_dt - data_inicial_dt
    ini_str, fim_str = _fmt(data_inicial_dt), _fmt(data_final_dt)

    try:
        df = api.extrai_dados_card(
            auth_id, id_database, id_card,
            ini_str, fim_str,
            campo_filtro="registration_date",
        )
    except Exception as e:
        if duracao <= JANELA_MINIMA:
            logger.error(f"[BACKFILL] Falha na janela mínima {ini_str}-{fim_str}: {e}")
            raise
        logger.warning(f"[BACKFILL] Falha na janela {ini_str}-{fim_str} ({e}). Dividindo ao meio.")
        meio = data_inicial_dt + duracao / 2
        processa_janela(api, auth_id, id_database, id_card, cliente, data_inicial_dt, meio, logger, contador)
        processa_janela(api, auth_id, id_database, id_card, cliente, meio, data_final_dt, logger, contador)
        return

    if len(df) >= LIMITE_LINHAS_METABASE and duracao > JANELA_MINIMA:
        logger.warning(
            f"[BACKFILL] Janela {ini_str}-{fim_str} atingiu o teto do Metabase "
            f"({len(df)} linhas). Dividindo ao meio."
        )
        meio = data_inicial_dt + duracao / 2
        processa_janela(api, auth_id, id_database, id_card, cliente, data_inicial_dt, meio, logger, contador)
        processa_janela(api, auth_id, id_database, id_card, cliente, meio, data_final_dt, logger, contador)
        return

    if len(df) >= LIMITE_LINHAS_METABASE:
        logger.warning(
            f"[BACKFILL] Janela mínima atingida ({ini_str}-{fim_str}) e ainda assim "
            f"{len(df)} linhas -- possível truncamento residual. Revisar manualmente "
            f"esse intervalo depois."
        )

    if len(df) == 0:
        logger.info(f"[BACKFILL] Janela {ini_str}-{fim_str}: 0 linhas, pulando.")
        return

    df_ajust = df[COLUNAS_ORIGEM].rename(columns=RENAME_FRENTE_B)
    df_ajust = df_ajust.where(pd.notnull(df_ajust), None)

    ConnectionDB.conecta(DB, cliente)
    ConnectionDB.insere_dados_bulk('inplay.stg_usuario_backfill', df_ajust, logger)

    contador['total'] += len(df_ajust)
    contador['janelas'] += 1
    logger.info(
        f"[BACKFILL] Janela {ini_str}-{fim_str}: {len(df_ajust)} linhas inseridas "
        f"(acumulado: {contador['total']} linhas em {contador['janelas']} janelas)"
    )


def executar_backfill(cliente: str, id_card: str, data_inicio_str: str, data_fim_str: str, logger):
    logger.info(f"[BACKFILL] Iniciando carga histórica unificada para {cliente}")

    api = ConsumeAPI(cliente=cliente)
    auth_id = api.conection(cliente)
    id_database = (MetabaseDatabase.ClickhousePartnerZeroum.value if cliente == 'ZEROUM'
                   else MetabaseDatabase.ClickhousePartnerEnergiabet.value)

    # Trunca a stg SÓ no início da execução inteira -- cada janela depois
    # disso é um INSERT (append), nunca um TRUNCATE por janela.
    ConnectionDB.conecta(DB, cliente)
    ConnectionDB.deleta_dados('inplay.stg_usuario_backfill', '', logger)

    data_inicio_dt = datetime.strptime(data_inicio_str, '%Y-%m-%d')
    data_fim_dt = datetime.strptime(data_fim_str, '%Y-%m-%d')

    contador = {'total': 0, 'janelas': 0}
    processa_janela(api, auth_id, id_database, id_card, cliente,
                     data_inicio_dt, data_fim_dt, logger, contador)

    logger.info(
        f"[BACKFILL] Extração concluída: {contador['total']} linhas em "
        f"{contador['janelas']} janelas gravadas em stg_usuario_backfill"
    )

    sql_update = """
        UPDATE inplay.dim_usuario d
        SET
            lastname                = s."LastName",
            taxnumber                = s."TaxNumber",
            documenttype             = s."DocumentType",
            documentnumber           = s."DocumentNumber",
            documentissuedby         = s."DocumentIssuedBy",
            isdocumentverified       = s."IsDocumentVerified",
            kycstatus                = s."KYCStatus",
            kycdocsstatus            = s."KYCDocsStatus",
            first_name_protegido     = s.first_name_protegido,
            mobile_number_protegido  = s.mobile_number_protegido,
            birth_date_protegido     = s.birth_date_protegido
        FROM inplay.stg_usuario_backfill s
        WHERE d.id = s.id
    """
    logger.info("[BACKFILL] Aplicando UPDATE único em dim_usuario (11 colunas)...")
    ConnectionDB.conecta(DB, cliente)
    ConnectionDB.executa_dml(sql_update, logger)
    logger.info(
        "[BACKFILL] UPDATE concluído. LEMBRETE: rodar VACUUM e ANALYZE em "
        "inplay.dim_usuario em seguida (fora de transação, janela de baixo "
        "tráfego) -- o Redshift implementa UPDATE como DELETE+INSERT por trás "
        "dos panos, e um UPDATE de milhões de linhas deixa muito espaço morto "
        "e blocos fora de ordem até a tabela ser compactada."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cliente", required=True, choices=["ZEROUM", "ENERGIABET"])
    parser.add_argument("--card-id", required=True,
                         help="card de teste (card__21517/card__21518) em validação; "
                              "card de produção (card__14826/card__15850) depois da Fase 7")
    parser.add_argument("--data-inicio", required=True, help="YYYY-MM-DD")
    parser.add_argument("--data-fim", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    logger = DBLogger(cliente=args.cliente).logger
    executar_backfill(args.cliente, args.card_id, args.data_inicio, args.data_fim, logger)
