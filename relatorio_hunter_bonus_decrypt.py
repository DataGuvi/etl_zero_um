"""
relatorio_hunter_bonus_decrypt.py
==================================
Complementa relatorio_hunter_bonus.sql.

A query SQL já identifica duplicidade de CPF/telefone/identidade comparando
os valores (o telefone/nome/nascimento continuam cifrados -- a comparação
funciona porque a chave/IV são fixos por parceiro, então mesmo valor em
claro = mesmo texto cifrado, ver util.py).

Este script SÓ descriptografa nome/telefone/nascimento da lista curta de
suspeitos (client_id com risco Alto/Médio) que já saiu da query -- não
roda decrypt na base inteira. Usa a função Util.descriptografar já
existente no projeto (util.py), sem duplicar lógica de criptografia.

Uso:
    1) Rode relatorio_hunter_bonus.sql no Redshift.
    2) Exporte o resultado (client_id, classificacao_risco, ...) para CSV
       OU busque direto do banco os campos cifrados desses client_id em
       inplay.dim_usuario (id, first_name_protegido, mobile_number_protegido,
       birth_date_protegido, taxnumber).
    3) Rode este script apontando pro cliente (ZEROUM ou ENERGIABET) --
       cada parceiro tem chave/IV diferentes (config.py).
"""

import pandas as pd
from util import Util
from database import ConnectionDB


def buscar_suspeitos_cifrados(client_ids: list[int], db: str, cliente: str, logger):
    """
    Busca em inplay.dim_usuario os campos cifrados só para os client_ids
    já sinalizados pela query de score (lista curta -- não a base inteira).
    """
    if not client_ids:
        return pd.DataFrame()

    ids_str = ",".join(str(i) for i in client_ids)

    sql = f"""
        SELECT
            id AS client_id,
            taxnumber,
            first_name_protegido,
            mobile_number_protegido,
            birth_date_protegido,
            isdocumentverified,
            kycstatus
        FROM inplay.dim_usuario
        WHERE id IN ({ids_str})
    """

    ConnectionDB.conecta(db, cliente)
    resultado = ConnectionDB.recupera_dados(
        'inplay.dim_usuario',
        'id, taxnumber, first_name_protegido, mobile_number_protegido, '
        'birth_date_protegido, isdocumentverified, kycstatus',
        f"WHERE id IN ({ids_str})"
    )

    colunas = [
        'client_id', 'taxnumber', 'first_name_protegido',
        'mobile_number_protegido', 'birth_date_protegido',
        'isdocumentverified', 'kycstatus'
    ]
    return pd.DataFrame(resultado, columns=colunas)


def descriptografar_suspeitos(df_suspeitos: pd.DataFrame, cliente: str, debug=False) -> pd.DataFrame:
    """
    Aplica Util.descriptografar linha a linha nos campos cifrados.
    Só roda em cima do dataframe já filtrado (suspeitos), nunca na base toda.
    """
    util = Util(debug=debug)

    df = df_suspeitos.copy()
    df['first_name'] = df['first_name_protegido'].apply(
        lambda v: util.descriptografar(cliente, v)
    )
    df['mobile_number'] = df['mobile_number_protegido'].apply(
        lambda v: util.descriptografar(cliente, v)
    )
    df['birth_date'] = df['birth_date_protegido'].apply(
        lambda v: util.descriptografar(cliente, v)
    )

    return df[[
        'client_id', 'taxnumber', 'first_name', 'mobile_number',
        'birth_date', 'isdocumentverified', 'kycstatus'
    ]]


def gerar_relatorio_legivel(client_ids: list[int], db: str, cliente: str, logger,
                             caminho_saida: str = 'hunters_identificados.csv'):
    """
    Pipeline completo: busca os cifrados só dos suspeitos, descriptografa,
    salva CSV legível para o time de risco/compliance.

    client_ids: lista de client_id com classificacao_risco IN ('Alto','Médio')
                vinda do resultado de relatorio_hunter_bonus.sql.
    cliente: 'ZEROUM' ou 'ENERGIABET' -- define qual AES_KEY/AES_IV usar
             (ver config.py) e qual banco consultar (database.py já
             seleciona o DB_NAME certo por cliente).
    """
    logger.info(f"[HUNTER-BONUS] Buscando {len(client_ids)} suspeitos cifrados ({cliente})")
    df_cifrado = buscar_suspeitos_cifrados(client_ids, db, cliente, logger)

    logger.info(f"[HUNTER-BONUS] Descriptografando {len(df_cifrado)} registros")
    df_legivel = descriptografar_suspeitos(df_cifrado, cliente)

    df_legivel.to_csv(caminho_saida, index=False, encoding='utf-8')
    logger.info(f"[HUNTER-BONUS] Relatório legível salvo em {caminho_saida}")

    return df_legivel


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("hunter_bonus")

    # Exemplo: substitua pela lista real de client_id (risco Alto/Médio)
    # vinda do resultado de relatorio_hunter_bonus.sql
    client_ids_suspeitos = []  # ex.: [123456, 789012, ...]

    gerar_relatorio_legivel(
        client_ids=client_ids_suspeitos,
        db='redshift',       # ou 'postgres', conforme seu ambiente
        cliente='ZEROUM',    # ou 'ENERGIABET'
        logger=logger,
    )
