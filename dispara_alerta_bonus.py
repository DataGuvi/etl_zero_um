"""
dispara_alerta_bonus.py
=========================
Versão de PRODUÇÃO de teste_dispara_alertas_bonus.py -- mesma lógica de
dedup/escalada, mas lendo e gravando nas tabelas reais do Redshift em vez
de um CSV local. Não envia nada -- só decide o que deve ser enviado e
grava em inplay.fila_disparo_alerta_bonus, que a plataforma de disparo
(Twilio/Meta, já usada para métricas de jogadores) consulta por polling.

OTIMIZAÇÃO PARCIAL (26/08/2026): só o relatório #7 (rajada) usa
df_fact_bonus (o lote incremental já extraído por processa_bonus(), em
memória) em vez de consultar o banco -- a janela de 50min do #7 cabe
folgada dentro do lote incremental (~5h de cobertura, ver processa_bonus()
em consume_api.py). #2 (reuso 24h) e #3 (rollover hoje) CONTINUAM
consultando fact_user_bonus diretamente -- df_fact_bonus só tem o que
mudou NESTA execução, não as últimas 24h/o dia inteiro. Usar só o
incremental nesses dois perderia casos (ex.: cliente usou o bônus 3x há 6h,
já processado num run anterior, e mais 3x agora -- só veríamos 3, não 6).

NOVO (26/08/2026): relatório #8 -- cadastro recente + bônus sem depósito.
Sinal descrito pelo próprio cliente na reunião ("cadastro às 10h, bônus às
10h01, ele não depositou, ele não sacou"). Roda ANTES de qualquer saque
ser possível -- é verificado no mesmo ciclo horário em que o bônus foi
concedido, e o filtro exige explicitamente que o cliente ainda não tenha
sacado (first_withdraw_date nulo), não é só "rápido o suficiente para
provavelmente vir antes".

PRÉ-REQUISITOS (rodar ddl_producao_bonus_antifraude.sql antes, uma vez):
    - inplay.log_disparo_alerta_bonus
    - inplay.stg_log_disparo_alerta_bonus
    - inplay.fila_disparo_alerta_bonus
    - inplay.stg_clientes_bonus_hora (novo, usado só pelo relatório #8)

CHAMAR dentro de processa_bonus() em consume_api.py, logo após
executar_agregacao_bonus_concessoes(df_fact_bonus, ...) -- mesmo df_fact_bonus
já extraído nessa função, sem extração adicional.

USO
---
    from dispara_alerta_bonus import executar_disparo_alertas_bonus
    executar_disparo_alertas_bonus(df_fact_bonus, cliente='ZEROUM',
                                    db='redshift', partner_id=180,
                                    logger=self.logger)


=============================================================================
COMO ESSE PIPELINE FUNCIONA -- GUIA DE LEITURA
=============================================================================

CADÊNCIA (de hora em hora)
---------------------------
Este módulo não tem agendador próprio -- ele é CHAMADO de dentro de
processa_bonus() (consume_api.py), que roda a cada execução do job
principal (main.py), tipicamente de hora em hora. Não existe estado "ao
vivo" entre uma chamada e outra: cada chamada de
executar_disparo_alertas_bonus() é UMA RODADA isolada, que:
  1. lê o histórico de quem já foi alertado (sql_leitura_log);
  2. roda a detecção dos 4 relatórios contra os dados atuais;
  3. decide, comparando com o histórico, quem é candidato NOVO nesta
     rodada e quem é reincidência (e de que tipo);
  4. consolida em poucas mensagens e grava a fila;
  5. atualiza o histórico com o que foi decidido nesta rodada.
Não há "job separado" rodando de hora em hora -- é literalmente essa
função sendo chamada de novo a cada ciclo do job principal.

OS 4 RELATÓRIOS, E POR QUE CADA UM LÊ OS DADOS DE UM JEITO DIFERENTE
-----------------------------------------------------------------------
    #2 reuso     -- sql_reuso()       -- cliente usou o MESMO bônus 5+
                                          vezes nas últimas 24h.
    #3 rollover  -- sql_rollover()    -- bônus com rollover (giro)
                                          pendente, status parado,
                                          atualizado hoje.
    #7 rajada    -- detecta_rajada()  -- 5+ bônus DIFERENTES pro mesmo
                                          cliente em 50min, valor
                                          zerado (indício de
                                          automação/script).
    #8 cadastro  -- detecta_cadastro_recente_bonus() -- cadastro seguido
                     de bônus em até 50min, sem depósito antes, sem
                     saque ainda (perfil clássico de hunter).

#2 e #3 consultam o banco DIRETO (fact_user_bonus) porque a janela deles
(24h / "hoje") é maior que o intervalo entre rodadas -- usar só o lote
incremental desta hora perderia casos de horas anteriores (ex.: cliente
reusou o bônus 3x há 6h, já visto numa rodada passada, e mais 3x agora --
usando só o incremental veríamos 3, não 6, e classificaríamos errado).
#7 e #8 usam df_fact_bonus (o lote incremental já extraído por
processa_bonus(), em memória) porque a janela deles (50min) cabe dentro do
intervalo típico entre rodadas -- não precisa reconsultar o banco.

O QUE CONTA COMO "REINCIDENTE", RELATÓRIO POR RELATÓRIO
-----------------------------------------------------------------------
Cada relatório tem sua própria regra de reincidência, controlada pelo
parâmetro escalada_por_valor de processa_relatorio() (ver essa função pro
mecanismo genérico completo -- chave, "já disparado hoje", escalada):

  #2 REUSO (escalada_por_valor=True):
      Chave = client_id + bonus_id. Se esse par JÁ foi alertado HOJE, só
      dispara de novo se o número de reusos AUMENTOU desde o último
      alerta (ex.: alertou com 5x, subiu pra 8x -- dispara de novo com o
      valor atualizado). Se não aumentou, fica em silêncio até virar o
      dia -- reincidência aqui é "piorou", não "aconteceu de novo".

  #3 ROLLOVER (escalada_por_valor=False):
      Chave = client_id + bonus_id + source_updated_at (o timestamp da
      própria atualização do rollover na origem). Como o timestamp já é
      parte da chave, toda atualização nova JÁ é uma chave nova por
      definição -- não existe "supressão de reincidência" pra este
      relatório: cada source_updated_at diferente é um caso novo, mesmo
      que seja o mesmo client_id/bonus_id de um alerta de ontem.

  #7 RAJADA (escalada_por_valor=True):
      Chave = client_id. Mesma lógica do reuso: só dispara de novo no
      mesmo dia se a NOVA rajada tiver mais bônus distintos que a última
      já alertada.

  #8 CADASTRO RECENTE (lógica própria -- processa_relatorio_cadastro_recente):
      É o único com 3 categorias de resultado, cada uma com mensagem
      própria (ver msg_cadastro_recente* mais abaixo):
      a) Mesmo client_id, já alertado HOJE -> SUPRIME (dedup simples,
         não repete no mesmo dia).
      b) Mesmo client_id, alertado em dia ANTERIOR, mas o padrão
         disparou de novo -> REINCIDENTE DO PRÓPRIO CLIENT_ID (cita a
         data do alerta anterior e a contagem de vezes).
      c) client_id NOVO (nunca alertado), mas CPF/telefone/nome+
         sobrenome+nascimento batem com outro client_id JÁ alertado
         antes -> REINCIDENTE POR IDENTIDADE -- é o caso de "conta nova,
         mesma pessoa", o sinal mais forte de fraude organizada que este
         relatório carrega (ver marca_reincidencia_identidade).
      d) Nenhum dos casos acima -> primeira ocorrência (mensagem padrão).

COMO A MENSAGEM CHEGA NO DESTINO
-----------------------------------------------------------------------
Este módulo NUNCA envia nada -- ele só decide e grava em
inplay.fila_disparo_alerta_bonus. Uma plataforma externa (Twilio/Meta) faz
polling (SELECT) nessa tabela e é quem realmente manda a mensagem. Por
isso existe a marcação status='enviado' logo antes de cada novo insert
(ver comentário dentro de executar_disparo_alertas_bonus): como a
plataforma nunca atualiza o status ela mesma, esse módulo marca como
"enviado" tudo que já estava na fila da rodada anterior antes de inserir
os casos novos -- senão a mesma leva antiga ficaria pendente pra sempre.
=============================================================================
"""

import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from database import ConnectionDB
from util import Util

load_dotenv()


FUSO_SP = "America/Sao_Paulo"

# CSVs de conferência (comparativo e-mail x WhatsApp) -- só para uso LOCAL.
# Em produção a variável não existe (ou é "false") e nenhum arquivo é gravado.
# Para gerar localmente: EXPORTA_CSV_CONFERENCIA=true no .env da sua máquina.
EXPORTA_CSV_CONFERENCIA = os.getenv("EXPORTA_CSV_CONFERENCIA", "false").lower() == "true"


def salva_csv_conferencia(df, nome_arquivo):
    """Grava CSV de conferência só se EXPORTA_CSV_CONFERENCIA=true (uso local)."""
    if EXPORTA_CSV_CONFERENCIA:
        df.to_csv(nome_arquivo, index=False)


# Horário local de São Paulo, só pro texto "Gerado às HH:MM" das mensagens (variavel1).
def horario_sp() -> str:
    """Servidor roda em UTC (Redshift/infra), então pd.Timestamp.now() puro
    devolve hora UTC -- errado pro time de análise, que lê em horário de
    Brasília. Esse helper converte só pra exibição (variavel1 de cada
    compila_*_unico). América/São_Paulo está fixo em UTC-3 desde o fim do
    horário de verão em 2019, mas usar o nome IANA (em vez de um offset
    fixo "-3h") deixa a conversão correta automaticamente se isso mudar.

    NÃO mexe em nenhum outro timestamp do pipeline -- ultimo_disparo,
    source_updated_at, a comparação "hoje" de processa_relatorio/
    processa_relatorio_cadastro_recente, tudo isso continua em UTC (fuso
    do servidor), sem alteração, pra não mudar a lógica de dedup/escalada
    já validada. Só o texto que o time lê é que precisa do fuso certo."""
    return pd.Timestamp.now(tz=FUSO_SP).strftime("%H:%M")


def _linha_sem_dados(nome_relatorio: str, titulo: str) -> pd.DataFrame:
    """Linha padrão pra quando um relatório NÃO encontrou nenhum caso novo
    nesta execução. Antes (até 04/set/2026), esse cenário resultava em
    NADA sendo inserido na fila pra esse relatório -- mudança pedida:
    sempre inserir 1 linha, com as variáveis numéricas em "0" e as de
    texto/lista em "Sem informação", pra a plataforma de disparo sempre
    ter uma linha por relatório por execução (visibilidade de que o
    relatório rodou e não achou nada, não apenas silêncio).

    Usado pelos 5 compila_*_unico (reuso, rollover, rajada, cadastro,
    pix_conta_nova) -- centralizado aqui pra não duplicar o mesmo padrão
    5 vezes de forma inconsistente."""
    horario_geracao = horario_sp()
    mensagem = f"{titulo}\nGerado às {horario_geracao}\n\nNenhum caso identificado nesta execução."
    return pd.DataFrame([{
        "relatorio": nome_relatorio,
        "client_id": None,
        "bonus_id": None,
        "mensagem": mensagem,

        "variavel1": horario_geracao,
        "variavel2": "0",
        "variavel3": "0",
        "variavel4": "0",
        "variavel5": "0",
        "variavel6": "Sem informação",
        "variavel7": "Sem informação",
        "variavel8": "Sem informação",
        "variavel9": None,
        "variavel10": None,
    }])


# =============================================================================
# QUERIES DE DETECÇÃO -- #2 e #3 seguem no banco (precisam de janela maior
# que o incremental). #7 virou função em pandas (ver detecta_rajada).
# =============================================================================

# Monta a consulta que procura clientes que reutilizaram o mesmo bônus 5 vezes ou mais nas últimas 24h.

def sql_reuso(partner_id):
    return f"""
        SELECT client_id, bonus_id, COUNT(*) AS valor
        FROM inplay.fact_user_bonus
        WHERE partner_id = {partner_id}
          AND awarding_time >= GETDATE() - INTERVAL '24 hours'
        GROUP BY client_id, bonus_id
        HAVING COUNT(*) >= 5
    """

# Monta a consulta que procura bônus com rollover ainda pendente, considerando os registros do dia.
def sql_rollover(partner_id):
    return f"""
        SELECT
            client_id,
            bonus_id,
            status,
            turnover_amount_left AS valor,
            source_updated_at
        FROM inplay.fact_user_bonus
        WHERE partner_id = {partner_id}
          AND status IN (6, 8)
          AND source_updated_at::date = CURRENT_DATE
          AND turnover_amount_left > 0
          AND bonus_id IS NOT NULL
    """

# Procura no lote atual clientes que receberam 5 ou mais tipos diferentes de bônus nos últimos 50 minutos, com bonus_prize = 0.
def detecta_rajada(df_fact_bonus: pd.DataFrame) -> pd.DataFrame:
    """Relatório #7, em pandas, sobre o lote incremental já em memória --
    substitui a consulta SQL antiga (sql_rajada) que reconsultava o banco
    a cada execução. Mesma regra: 5+ bonus_id distintos para o mesmo
    client_id, com bonus_prize somando zero, nos últimos 50 minutos
    (alterado de 15 para 50min, 02/set/2026, a pedido do cliente)."""
    if df_fact_bonus is None or df_fact_bonus.empty:
        return pd.DataFrame(columns=["client_id", "qtd_bonus_distintos", "valor"])

    limite = pd.Timestamp.now() - pd.Timedelta(minutes=50)
    df = df_fact_bonus.copy()
    # format="ISO8601" -- necessário porque awarding_time vem do df_fact_bonus
    # (extração bruta da API) com formatos ISO inconsistentes na mesma coluna
    # (algumas linhas com microssegundos+offset, outras só com "Z" no final).
    # Sem isso, pd.to_datetime infere o formato pela 1ª linha e quebra ao
    # encontrar uma linha com formato diferente (achado em produção, 01/set/2026).
    #
    # .dt.tz_localize(None) -- o parse acima produz datetime COM fuso (UTC),
    # porque a origem tem "Z"/offset explícito em algumas linhas. O resto do
    # projeto trabalha com TIMESTAMP WITHOUT TIME ZONE (ver DDLs) -- comparar
    # um datetime com fuso com um sem fuso (ex.: pd.Timestamp.now(), que é
    # naive) gera TypeError. Removemos o fuso aqui pra ficar consistente com
    # o padrão do projeto, em vez de espalhar tz='UTC' em todo lugar que usa
    # pd.Timestamp.now() (mesmo achado em produção, 01/set/2026).
    df["awarding_time"] = pd.to_datetime(df["awarding_time"], format="ISO8601").dt.tz_localize(None)
    df = df[df["awarding_time"] >= limite]

    if df.empty:
        return pd.DataFrame(columns=["client_id", "qtd_bonus_distintos", "valor"])

    resumo = (
        df.groupby("client_id")
        .agg(qtd_bonus_distintos=("bonus_id", "nunique"),
             valor=("bonus_id", "count"),
             soma_bonus_prize=("bonus_prize", "sum"))
        .reset_index()
    )
    return resumo[(resumo["qtd_bonus_distintos"] >= 5) & (resumo["soma_bonus_prize"] == 0)]


# Procura contas cadastradas recentemente que já vinculam uma chave PIX usada por outra conta existente.
def sql_pix_conta_nova():
    """Relatório #9 -- conta nova (registration_date nas últimas 24h)
    vinculada a uma chave PIX que outra conta (qualquer idade, ativa)
    também usa. Gera 1 linha por PAR conta-nova x conta-anterior -- se
    uma chave tiver mais de 1 outra conta vinculada, fana em várias
    linhas (não colapsa) -- ver relatorio_pix_conta_nova.sql pro
    histórico completo da decisão de design (por que não dá pra usar
    updated_at/import_date de agg_pix_cliente pra saber "quem vinculou
    primeiro" -- os dois são gravados como now() a cada recálculo do
    grupo, não preservam a data real do vínculo).
 
    valor = 1 sempre (evento binário, sem escalada por valor -- ver
    escalada_por_valor=False na chamada de processa_relatorio)."""
    return """
        WITH pix_com_multiplas_contas AS (
            SELECT pix_key
            FROM inplay.agg_pix_cliente
            WHERE is_active = true
              AND pix_key IS NOT NULL AND TRIM(pix_key) <> ''
            GROUP BY pix_key
            HAVING COUNT(DISTINCT client_id) > 1
        ),
        contas_novas AS (
            SELECT
                a.client_id,
                a.pix_key,
                a.pix_type,
                a.qtd_transacoes,
                u.registration_date,
                u.status_usuario
            FROM inplay.agg_pix_cliente a
            INNER JOIN pix_com_multiplas_contas p ON p.pix_key = a.pix_key
            LEFT JOIN inplay.dim_usuario u ON u.id = a.client_id
            WHERE a.is_active = true
              AND u.registration_date >= GETDATE() - INTERVAL '24 hours'
        )
        SELECT
            n.client_id                AS client_id,
            n.pix_key                  AS pix_key,
            n.pix_type                 AS pix_type,
            n.qtd_transacoes           AS qtd_transacoes,
            n.registration_date        AS registration_date,
            n.status_usuario           AS status_usuario,
            outra.client_id            AS client_id_anterior,
            outra.qtd_transacoes       AS qtd_transacoes_anterior,
            u_outra.registration_date  AS registration_date_anterior,
            u_outra.status_usuario     AS status_usuario_anterior,
            1                          AS valor
        FROM contas_novas n
        INNER JOIN inplay.agg_pix_cliente outra
            ON outra.pix_key = n.pix_key
           AND outra.client_id <> n.client_id
           AND outra.is_active = true
        LEFT JOIN inplay.dim_usuario u_outra ON u_outra.id = outra.client_id
    """
    """Relatório #7, em pandas, sobre o lote incremental já em memória --
    substitui a consulta SQL antiga (sql_rajada) que reconsultava o banco
    a cada execução. Mesma regra: 5+ bonus_id distintos para o mesmo
    client_id, com bonus_prize somando zero, nos últimos 50 minutos
    (alterado de 15 para 50min, 02/set/2026, a pedido do cliente)."""
    if df_fact_bonus is None or df_fact_bonus.empty:
        return pd.DataFrame(columns=["client_id", "qtd_bonus_distintos", "valor"])
 
    limite = pd.Timestamp.now() - pd.Timedelta(minutes=50)
    df = df_fact_bonus.copy()
    # format="ISO8601" -- necessário porque awarding_time vem do df_fact_bonus
    # (extração bruta da API) com formatos ISO inconsistentes na mesma coluna
    # (algumas linhas com microssegundos+offset, outras só com "Z" no final).
    # Sem isso, pd.to_datetime infere o formato pela 1ª linha e quebra ao
    # encontrar uma linha com formato diferente (achado em produção, 01/set/2026).
    #
    # .dt.tz_localize(None) -- o parse acima produz datetime COM fuso (UTC),
    # porque a origem tem "Z"/offset explícito em algumas linhas. O resto do
    # projeto trabalha com TIMESTAMP WITHOUT TIME ZONE (ver DDLs) -- comparar
    # um datetime com fuso com um sem fuso (ex.: pd.Timestamp.now(), que é
    # naive) gera TypeError. Removemos o fuso aqui pra ficar consistente com
    # o padrão do projeto, em vez de espalhar tz='UTC' em todo lugar que usa
    # pd.Timestamp.now() (mesmo achado em produção, 01/set/2026).
    df["awarding_time"] = pd.to_datetime(df["awarding_time"], format="ISO8601").dt.tz_localize(None)
    df = df[df["awarding_time"] >= limite]
 
    if df.empty:
        return pd.DataFrame(columns=["client_id", "qtd_bonus_distintos", "valor"])
 
    resumo = (
        df.groupby("client_id")
        .agg(qtd_bonus_distintos=("bonus_id", "nunique"),
             valor=("bonus_id", "count"),
             soma_bonus_prize=("bonus_prize", "sum"))
        .reset_index()
    )
    return resumo[(resumo["qtd_bonus_distintos"] >= 5) & (resumo["soma_bonus_prize"] == 0)]

# Procura clientes que acabaram de se cadastrar e receberam bônus rapidamente, sem depósito prévio e ainda sem saque.
def detecta_cadastro_recente_bonus(df_fact_bonus: pd.DataFrame, cliente: str, db: str, logger,
                                    corte_minutos: int = 50) -> pd.DataFrame:
    """
    Relatório #8: cliente reivindicou bônus poucos minutos após o cadastro,
    sem depósito prévio e sem nenhum saque ainda -- perfil clássico de
    bônus hunter descrito na reunião de 26/08.

    Roda no mesmo lote incremental (df_fact_bonus), mas precisa cruzar com
    inplay.dim_usuario (registration_date, first_deposit_date,
    first_withdraw_date) -- que não vem no df_fact_bonus. Por isso sobe os
    client_id impactados pra uma staging pequena e faz um JOIN escopado
    (mesmo padrão de stg_pares_bonus_impactados em agregacao_bonus.py),
    em vez de escanear dim_usuario inteira.

    corte_minutos: default inicial de trabalho era 15min (marba deu o
    exemplo de 1 minuto na reunião -- 15min era uma margem mais
    conservadora pra não gerar alerta demais antes de calibrar com volume
    real). Alterado para 50min em 02/set/2026 a pedido do cliente, mesma
    janela usada no relatório #7 (rajada). Ajustar depois de observar
    quantos casos isso gera por dia.
    """
    if df_fact_bonus is None or df_fact_bonus.empty:
        return pd.DataFrame(columns=["client_id", "valor", "taxnumber", "mobile_number_protegido", "first_name_protegido", "lastname", "birth_date_protegido"])

    client_ids = df_fact_bonus["client_id"].dropna().unique().tolist()
    if not client_ids:
        return pd.DataFrame(columns=["client_id", "valor", "taxnumber", "mobile_number_protegido", "first_name_protegido", "lastname", "birth_date_protegido"])

    df_ids = pd.DataFrame({"client_id": client_ids})
    ConnectionDB.conecta(db, cliente)
    ConnectionDB.deleta_dados('inplay.stg_clientes_bonus_hora', '', logger)
    ConnectionDB.conecta(db, cliente)
    ConnectionDB.insere_dados_bulk('inplay.stg_clientes_bonus_hora', df_ids, logger, normalize_numpy=True)

    sql = """
        SELECT
            u.id AS client_id,
            u.registration_date,
            CASE WHEN u.first_deposit_date = '1970-01-01' THEN NULL
                 ELSE u.first_deposit_date END AS first_deposit_date,
            CASE WHEN u.first_withdraw_date = '1970-01-01' THEN NULL
                 ELSE u.first_withdraw_date END AS first_withdraw_date,
            u.taxnumber,
            u.mobile_number_protegido,
            u.first_name_protegido,
            u.lastname,
            u.birth_date_protegido
        FROM inplay.dim_usuario u
        INNER JOIN inplay.stg_clientes_bonus_hora s ON s.client_id = u.id
    """
    ConnectionDB.conecta(db, cliente)
    df_usuario = ConnectionDB.executa_script(sql, logger)

    if df_usuario.empty:
        return pd.DataFrame(columns=["client_id", "valor", "taxnumber", "mobile_number_protegido", "first_name_protegido", "lastname", "birth_date_protegido"])

    # primeira concessão de bônus de cada cliente NESTE lote
    df_bonus_min = df_fact_bonus.groupby("client_id", as_index=False)["awarding_time"].min()
    # Mesmo tratamento de fuso de detecta_rajada (ver comentário lá) --
    # registration_date vem sem fuso do banco, awarding_time chegaria com
    # fuso do parse ISO8601 -- a subtração das duas quebraria sem isso.
    df_bonus_min["awarding_time"] = pd.to_datetime(df_bonus_min["awarding_time"], format="ISO8601").dt.tz_localize(None)

    df = df_bonus_min.merge(df_usuario, on="client_id", how="inner")
    df["registration_date"] = pd.to_datetime(df["registration_date"], format="ISO8601")
    df["first_deposit_date"] = pd.to_datetime(df["first_deposit_date"], format="ISO8601")

    df["minutos_ate_bonus"] = (
        (df["awarding_time"] - df["registration_date"]).dt.total_seconds() / 60
    )

    sem_deposito_previo = df["first_deposit_date"].isna() | (df["first_deposit_date"] > df["awarding_time"])
    ainda_nao_sacou = df["first_withdraw_date"].isna()
    cadastro_recente = (df["minutos_ate_bonus"] >= 0) & (df["minutos_ate_bonus"] <= corte_minutos)

    candidatos = df[cadastro_recente & sem_deposito_previo & ainda_nao_sacou].copy()
    candidatos = candidatos.rename(columns={"minutos_ate_bonus": "valor"})
    return candidatos[[
        "client_id", "valor", "taxnumber", "mobile_number_protegido",
        "first_name_protegido", "lastname", "birth_date_protegido",
    ]]

# Lê o histórico dos alertas já disparados para saber o que já foi comunicado.
def sql_leitura_log():
    return "SELECT relatorio, chave, valor_referencia, ultimo_disparo, qtd_disparos FROM inplay.log_disparo_alerta_bonus"

# Busca CPF, telefone e dados de identidade de determinados clientes para comparação.
def sql_identidade_client_ids(client_ids):
    """Busca CPF/telefone/nome+sobrenome+nascimento de uma lista específica
    de client_id -- usado pra pegar a identidade de quem já foi alertado
    antes pelo relatório 08, e cruzar com os candidatos desta execução.
    Esta função só monta a QUERY -- ainda traz telefone/nome/nascimento
    CIFRADOS (mobile_number_protegido, first_name_protegido,
    birth_date_protegido). O decrypt de verdade (Util.descriptografar)
    acontece depois, em marca_reincidencia_identidade, não aqui -- taxnumber
    e lastname não precisam de decrypt, já vêm em texto puro."""
    ids_str = ",".join(str(int(cid)) for cid in client_ids)
    return f"""
        SELECT
            id AS client_id,
            NULLIF(taxnumber, '') AS taxnumber,
            mobile_number_protegido,
            first_name_protegido,
            NULLIF(lastname, '') AS lastname,
            birth_date_protegido
        FROM inplay.dim_usuario
        WHERE id IN ({ids_str})
    """

# Verifica se um cliente novo parece ser a mesma pessoa de uma conta que já havia sido alertada.
def marca_reincidencia_identidade(candidatos: pd.DataFrame, cliente: str, db: str, logger) -> pd.DataFrame:
    """
    Pra cada candidato do relatório 08 desta execução, verifica se
    CPF/telefone/identidade (nome+sobrenome+nascimento) batem com algum
    client_id JÁ alertado antes pelo PRÓPRIO relatório 08
    (log_disparo_alerta_bonus, relatorio='08_cadastro_recente').

    Se bater, preenche client_id_anterior_mesma_identidade -- é o caso de
    "conta nova, mesma pessoa de uma conta já alertada", que hoje passa
    batido porque o dedup só olha client_id.

    ALTERADO (03/set/2026, a pedido do cliente): antes comparava direto o
    valor CIFRADO de telefone/nome/nascimento -- funcionava porque a
    chave/IV são fixas por parceiro (mesmo valor em claro sempre vira o
    mesmo cifrado), mas é uma comparação que só é válida "por acidente" de
    como o algoritmo funciona, não uma comparação de identidade de
    verdade. Agora descriptografa (Util.descriptografar, util.py) antes de
    comparar. Mais lento -- decrypt é feito linha a linha em Python, sem
    equivalente em SQL -- mas correto por definição, e abre espaço pra
    normalizar formato (ex.: telefone com/sem 9º dígito, nome com espaço a
    mais) numa próxima iteração, o que a comparação cifrada nunca
    permitiria. taxnumber e lastname NÃO passam por decrypt -- já vêm em
    texto puro (ver relatorio_hunter_bonus.sql, achado da Frente A).
    """
    candidatos = candidatos.copy()
    candidatos["client_id_anterior_mesma_identidade"] = np.nan

    ConnectionDB.conecta(db, cliente)
    df_log_08 = ConnectionDB.executa_script(
        "SELECT DISTINCT chave FROM inplay.log_disparo_alerta_bonus "
        "WHERE relatorio = '08_cadastro_recente'",
        logger,
    )
    if df_log_08.empty:
        return candidatos

    ids_anteriores = df_log_08["chave"].astype("int64").tolist()
    # não faz sentido comparar um candidato de hoje contra ele mesmo caso
    # já tenha log de outro dia -- esse caso já é tratado à parte (mesmo
    # client_id reincidente), não precisa entrar aqui.
    ids_anteriores = [i for i in ids_anteriores if i not in set(candidatos["client_id"])]
    if not ids_anteriores:
        return candidatos

    ConnectionDB.conecta(db, cliente)
    df_ident_anteriores = ConnectionDB.executa_script(
        sql_identidade_client_ids(ids_anteriores), logger
    )
    if df_ident_anteriores.empty:
        return candidatos

    # Descriptografia real -- escopo é pequeno de propósito (só os
    # candidatos desta hora + quem já foi alertado antes pelo #8), então o
    # custo de decrypt linha a linha em Python é desprezível aqui. NÃO
    # fazer isso na base inteira (ver decripta_identidade_bonus.py, que
    # trata o caso de escopo maior usado por relatorio_hunter_bonus.sql).
    util = Util()

    def _decripta_coluna(df: pd.DataFrame, coluna_cifrada: str, coluna_decrypt: str):
        df[coluna_decrypt] = df[coluna_cifrada].apply(
            lambda v: util.descriptografar(cliente, v) if pd.notna(v) else None
        )

    for df in (candidatos, df_ident_anteriores):
        _decripta_coluna(df, "mobile_number_protegido", "mobile_number_decrypt")
        _decripta_coluna(df, "first_name_protegido", "first_name_decrypt")
        _decripta_coluna(df, "birth_date_protegido", "birth_date_decrypt")

    # ordem = prioridade do critério -- CPF é o mais confiável, identidade
    # (nome+sobrenome+nascimento) é o mais fraco, só usado se os outros dois
    # não bateram. taxnumber e lastname já vêm em texto puro, sem decrypt.
    criterios = [
        ["taxnumber"],
        ["mobile_number_decrypt"],
        ["first_name_decrypt", "lastname", "birth_date_decrypt"],
    ]
    for cols in criterios:
        base = df_ident_anteriores.dropna(subset=cols).drop_duplicates(subset=cols)
        if base.empty:
            continue
        merged = candidatos.merge(
            base[cols + ["client_id"]].rename(columns={"client_id": "_match"}),
            on=cols, how="left",
        )
        candidatos["client_id_anterior_mesma_identidade"] = (
            candidatos["client_id_anterior_mesma_identidade"].fillna(merged["_match"])
        )

    return candidatos


# =============================================================================
# MENSAGENS -- mesmo texto validado com o cliente
# =============================================================================
# Cria a mensagem individual do alerta de reuso.
def msg_reuso(row):
    return (f"🟡 Reuso de bônus fora do padrão. Cliente {row['client_id']} usou o bônus "
            f"{row['bonus_id']} {int(row['valor'])}x nas últimas 24h. Análise manual recomendada.")

# Cria a mensagem individual do alerta de rollover.
def msg_rollover(row):
    return (f"🟡 Rollover não cumprido. Cliente {row['client_id']}, bônus {row['bonus_id']}, "
            f"R$ {row['valor']:.2f} pendente, status {row['status']}.")

# Cria a mensagem individual do alerta de rajada/automação.
def msg_rajada(row):
    return (f"🔴 Possível automação detectada. Cliente {row['client_id']} reivindicou "
            f"{int(row['valor'])} bônus em {int(row['qtd_bonus_distintos'])} tipos diferentes, "
            f"valor zerado, intervalos de milissegundos. Verificar.")

# Cria a mensagem de primeira ocorrência de cadastro recente + bônus.
def msg_cadastro_recente(row):
    return (f"🔴 Possível bônus hunter. Cliente {row['client_id']} usou bônus "
            f"{row['valor']:.0f} min após o cadastro, sem depósito prévio e sem saque ainda. "
            f"Verificar antes de liberar saque.")

# Cria mensagem especial quando o mesmo cliente já foi alertado antes.
def msg_cadastro_recente_reincidente_client_id(row, log_existente):
    data_anterior = pd.Timestamp(log_existente["ultimo_disparo"]).date()
    qtd_vezes = int(log_existente["qtd_disparos"]) + 1
    return (f"🔴 Possível bônus hunter (REINCIDENTE). Cliente {row['client_id']} voltou a "
            f"disparar o padrão de cadastro+bônus -- já alertado antes em {data_anterior} "
            f"({qtd_vezes}ª vez). Verificar antes de liberar saque.")

# Cria mensagem especial quando uma conta nova pertence aparentemente à mesma pessoa de uma conta já alertada.
def msg_cadastro_recente_reincidente_identidade(row):
    client_id_anterior = int(row["client_id_anterior_mesma_identidade"])
    return (f"🔴 Possível bônus hunter (CONTA NOVA, MESMA PESSOA). Cliente {row['client_id']} "
            f"usou bônus {row['valor']:.0f} min após o cadastro, sem depósito prévio e sem "
            f"saque ainda -- CPF/telefone/identidade batem com o cliente {client_id_anterior}, "
            f"já alertado antes pelo mesmo relatório. Provável conta-laranja/multi-conta. "
            f"Verificar antes de liberar saque.")

# Cria a mensagem individual do alerta de conta nova com PIX já vinculado a outra conta.
_PIX_TYPE_NOME = {1: "documento", 2: "e-mail", 3: "telefone", 4: "chave aleatória"}
_STATUS_NOME = {
    1: "Active", 5: "Locked", 7: "Suspended", 8: "Disabled", 9: "SelfExcluded",
    10: "RegulationRestricted", 11: "SystemExcluded", 12: "CentralizedExcluded",
    13: "BrazilProgram", 14: "BeneficioContinuado", 15: "RenegociacaoFies", 16: "FiesEmpreendedor",
}
 
def msg_pix_conta_nova(row):
    tipo_nome = _PIX_TYPE_NOME.get(int(row["pix_type"]), f"tipo {row['pix_type']}")
    status_anterior_nome = _STATUS_NOME.get(int(row["status_usuario_anterior"]), "desconhecido") \
        if pd.notna(row.get("status_usuario_anterior")) else "desconhecido"
    return (f"🔴 Conta nova com chave PIX já usada. Cliente {row['client_id']} "
            f"(cadastrado em {row['registration_date']}) vinculou uma chave PIX ({tipo_nome}) "
            f"que já pertence à conta {int(row['client_id_anterior'])} "
            f"(status {status_anterior_nome}). Possível conta vinculada/laranja -- "
            f"verificar antes de liberar saque.")
 
# =============================================================================
# CONSOLIDAÇÃO POR CLIENTE -- #2 e #3 podem gerar várias linhas pro mesmo
# client_id (um por bonus_id diferente flagado). Em vez de disparar uma
# mensagem por linha, agrupa por cliente e manda 1 mensagem só, listando
# todos os bônus. #7 e #8 já são naturalmente 1 linha por cliente (chave é
# só client_id), não precisam passar por isso.
# =============================================================================
# Junta vários registros de reuso do mesmo cliente em uma única ocorrência, classificando em ALTA, MEDIA ou ATENCAO.
def consolida_reuso_por_cliente(fila_reuso: pd.DataFrame) -> pd.DataFrame:

    if fila_reuso.empty:
        return fila_reuso

    linhas = []

    for client_id, grupo in fila_reuso.groupby("client_id"):

        # Ordena os bônus do cliente pelo maior número de reutilizações 
        
        grupo = grupo.sort_values( 
            ["valor", "bonus_id"], 
            ascending=[False, True] 
        )

        # Lista dos bônus e respectivas quantidades de reutilização
        itens = " | ".join(
            f"{int(r['bonus_id'])} {int(r['valor'])}x"
            for _, r in grupo.iterrows()
        )

        # Quantidade de bônus diferentes utilizados pelo cliente
        qtd_bonus = len(grupo)

        # Maior quantidade de reutilizações em um único bônus
        maior_reuso = int(grupo["valor"].max())

        # Soma das reutilizações de todos os bônus do cliente
        total_reusos = int(grupo["valor"].sum())

        # ---------------------------------------------------------
        # Classificação baseada no maior reuso de um único bônus
        # ---------------------------------------------------------

        if maior_reuso >= 10:
            prioridade = "ALTA"

        elif maior_reuso >= 7:
            prioridade = "MEDIA"

        else:
            prioridade = "ATENCAO"

        linhas.append({
            "relatorio": "02_reuso",
            "client_id": int(client_id),

            # Mantém o bonus_id somente quando existe um único bônus.
            # Quando há vários, a linha representa o cliente como um todo.
            "bonus_id": (
                int(grupo.iloc[0]["bonus_id"])
                if qtd_bonus == 1
                else None
            ),

            # Indicadores de concentração do cliente

            "qtd_bonus": qtd_bonus,
            "maior_reuso": maior_reuso,
            "total_reusos": total_reusos,
            # Prioridade baseada no maior reuso individual
            "prioridade": prioridade,

            # Detalhamento dos bônus utilizados 
            "itens": itens,

            # Não precisamos mais da mensagem antiga aqui.
            "mensagem": None,
        })

    return pd.DataFrame(linhas)



def consolida_reuso_por_bonus(fila_reuso: pd.DataFrame) -> pd.DataFrame:

    if fila_reuso.empty:
        return pd.DataFrame()

    linhas = []

    for bonus_id, grupo in fila_reuso.groupby("bonus_id"):

        qtd_clientes = grupo["client_id"].nunique()

        total_reusos = int(grupo["valor"].sum())

        maior_reuso = int(grupo["valor"].max())

        linhas.append({
            "bonus_id": int(bonus_id),
            "qtd_clientes": int(qtd_clientes),
            "total_reusos": total_reusos,
            "maior_reuso": maior_reuso,
        })

    resultado = pd.DataFrame(linhas)

    # Primeiro os bônus presentes em mais clientes.
    # Em empate, prioriza maior volume total de reutilizações.
    resultado = resultado.sort_values(
        ["qtd_clientes", "total_reusos", "maior_reuso"],
        ascending=False
    ).reset_index(drop=True)

    return resultado
    
def prepara_variaveis_reuso(
    fila_cliente: pd.DataFrame,
    fila_bonus: pd.DataFrame
    ) -> dict:

     
    if fila_cliente.empty:
        return {
            "variavel2": "0",
            "variavel3": "0",
            "variavel4": "0",
            "variavel5": "0",
            "variavel6": "",
            "variavel7": "",
            "variavel8": "",
            "variavel9": "0",
            "variavel10": "últimas 24h",
        }

    # ---------------------------------------------------------
    # Indicadores gerais
    # ---------------------------------------------------------

    total_clientes = len(fila_cliente)

    clientes_multiplos = int(
        (fila_cliente["qtd_bonus"] > 1).sum()
    )

    maior_reuso = int(
        fila_cliente["maior_reuso"].max()
    )

    maior_total_reusos = int(
        fila_cliente["total_reusos"].max()
    )

    # ---------------------------------------------------------
    # Bônus envolvidos
    # ---------------------------------------------------------

    bonus_ids = (
        fila_bonus["bonus_id"]
        .astype(int)
        .astype(str)
        .tolist()
    )

    bonus_ids_texto = ", ".join(bonus_ids)

    # ---------------------------------------------------------
    # Clientes com maior concentração
    # ---------------------------------------------------------

    clientes_destaque = fila_cliente.sort_values(
        ["qtd_bonus", "total_reusos", "maior_reuso"],
        ascending=False
    ).head(5)

    lista_clientes = []

    for _, row in clientes_destaque.iterrows():

        lista_clientes.append(
            f"• Cliente *{int(row['client_id'])}* → "
            f"{int(row['qtd_bonus'])} bônus, "
            f"{int(row['total_reusos'])}x total, "
            f"máx. {int(row['maior_reuso'])}x"
        )

    clientes_texto = " | ".join(lista_clientes)

    # ---------------------------------------------------------
    # Distribuição por prioridade
    # ---------------------------------------------------------

    qtd_alta = int(
        (fila_cliente["prioridade"] == "ALTA").sum()
    )

    qtd_media = int(
        (fila_cliente["prioridade"] == "MEDIA").sum()
    )

    qtd_atencao = int(
        (fila_cliente["prioridade"] == "ATENCAO").sum()
    )

    prioridades = []

    if qtd_alta:
        prioridades.append(f"ALTA: {qtd_alta}")

    if qtd_media:
        prioridades.append(f"MEDIA: {qtd_media}")

    if qtd_atencao:
        prioridades.append(f"ATENCAO: {qtd_atencao}")

    prioridades_texto = " | ".join(prioridades)

    # ---------------------------------------------------------
    # Bônus com maior concentração entre clientes
    # ---------------------------------------------------------

    bonus_destaque = fila_bonus[
        fila_bonus["qtd_clientes"] >= 2
    ].head(5)

    lista_bonus = []

    for _, row in bonus_destaque.iterrows():

        lista_bonus.append(
            f"• Bônus *{int(row['bonus_id'])}* → "
            f"{int(row['qtd_clientes'])} clientes, "
            f"{int(row['total_reusos'])}x total"
        )

    # variavel9 nunca pode ir vazia (a plataforma de disparo/template exige
    # valor). Quando nenhum bônus é compartilhado por 2+ clientes -- caso
    # comum, ex.: reuso isolado de 1 cliente -- a lista fica vazia e o
    # join devolvia "". Na ausência, retorna "0".
    bonus_texto = " | ".join(lista_bonus) if lista_bonus else "0"

    return {
        "variavel2": str(total_clientes),
        "variavel3": str(clientes_multiplos),
        "variavel4": str(maior_reuso),
        "variavel5": str(maior_total_reusos),
        "variavel6": bonus_ids_texto,
        "variavel7": clientes_texto,
        "variavel8": prioridades_texto,
        "variavel9": bonus_texto,
        "variavel10": "últimas 24h",
    }
    


# Transforma todos os casos de reuso em uma única mensagem-resumo para envio.
def monta_mensagem_reuso(fila_reuso: pd.DataFrame) -> str:

    if fila_reuso.empty:
        return ""

    total_casos = len(fila_reuso)

    casos_multiplos = int(
        (fila_reuso["qtd_bonus"] > 1).sum()
    )

    maior_total_reusos  = int(
        fila_reuso["total_reusos"].max()
    )

    # ---------------------------------------------------------
    # Classificação
    # ---------------------------------------------------------

    alta = fila_reuso[
        fila_reuso["prioridade"] == "ALTA"
    ].sort_values(
        ["maior_reuso", "total_reusos"],
        ascending=False
    )

    media = fila_reuso[
        fila_reuso["prioridade"] == "MEDIA"
    ].sort_values(
        ["maior_reuso", "total_reusos"],
        ascending=False
    )

    atencao = fila_reuso[
        fila_reuso["prioridade"] == "ATENCAO"
    ]

    linhas = []

    # ---------------------------------------------------------
    # Cabeçalho
    # ---------------------------------------------------------

    linhas.append(
        f"🟡 *Alerta de Reuso de Bônus — {total_casos} casos*"
    )

    # ---------------------------------------------------------
    # Alta prioridade
    # ---------------------------------------------------------

    if not alta.empty:

        linhas.append("")
        linhas.append("🚨 *Alta prioridade*")

        for _, row in alta.iterrows():

            linhas.append(
                f"• Cliente *{int(row['client_id'])}* → "
                f"{row['itens']}"
            )

    # ---------------------------------------------------------
    # Prioridade
    # ---------------------------------------------------------

    if not media.empty:

        linhas.append("")
        linhas.append("🟠 *Prioridade*")

        for _, row in media.iterrows():

            linhas.append(
                f"• Cliente *{int(row['client_id'])}* → "
                f"{row['itens']}"
            )

    # ---------------------------------------------------------
    # Atenção
    # ---------------------------------------------------------

    if not atencao.empty:

        linhas.append("")
        linhas.append("🟡 *Atenção*")

        linhas.append(
            f"• {len(atencao)} casos com reutilização entre *5–6x*"
        )

    # ---------------------------------------------------------
    # Resumo
    # ---------------------------------------------------------

    linhas.append("")
    linhas.append("📊 *Resumo da execução*")

    linhas.append(
        f"• Total de casos: *{total_casos}*"
    )

    linhas.append(
        f"• Casos com múltiplos bônus: *{casos_multiplos}*"
    )

    linhas.append(
        f"• Maior recorrência: *{maior_total_reusos} utilizações*"
    )

    linhas.append(
        "• Janela analisada: *últimas 24h*"
    )

    # ---------------------------------------------------------
    # Ação
    # ---------------------------------------------------------

    linhas.append("")
    linhas.append(
        "⚠️ *Ação recomendada:* priorizar os casos "
        "🚨/🟠 para análise manual."
    )

    return "\n".join(linhas)

#Junta os rollovers do mesmo cliente e cria uma mensagem única mostrando os bônus e valores pendentes.
def consolida_rollover_por_cliente(fila_rollover: pd.DataFrame) -> pd.DataFrame:
    if fila_rollover.empty:
        return fila_rollover

    # Segurança: remove registros sem bonus_id.
    # Rollover sem identificação do bônus não deve gerar alerta.
    fila_rollover = fila_rollover[
        fila_rollover["bonus_id"].notna()
    ].copy()

    if fila_rollover.empty:
        return pd.DataFrame()

    linhas = []

    for client_id, grupo in fila_rollover.groupby("client_id"):

        # Garante que bonus_id seja tratado como inteiro
        grupo = grupo[grupo["bonus_id"].notna()].copy()

        if grupo.empty:
            continue

        itens = "; ".join(
            f"bônus {int(r['bonus_id'])} "
            f"(R$ {r['valor']:.2f}, status {r['status']})"
            for _, r in grupo.iterrows()
            if pd.notna(r["bonus_id"])
        )

        qtd_bonus = grupo["bonus_id"].nunique()
        valor_total = grupo["valor"].sum()

        if qtd_bonus == 1:
            mensagem = (
                f"🟡 Rollover não cumprido. "
                f"Cliente {int(client_id)}, "
                f"bônus {int(grupo.iloc[0]['bonus_id'])}, "
                f"R$ {valor_total:.2f} pendente, "
                f"status {grupo.iloc[0]['status']}."
            )
        else:
            mensagem = (
                f"🟡 Rollover não cumprido em {qtd_bonus} bônus "
                f"(total R$ {valor_total:.2f} pendente). "
                f"Cliente {int(client_id)}: {itens}."
            )

        linhas.append({
            "relatorio": "03_rollover",
            "client_id": int(client_id),
            "bonus_id": (
                int(grupo.iloc[0]["bonus_id"])
                if qtd_bonus == 1
                else None
            ),
            "valor": float(valor_total),
            "status": grupo.iloc[0]["status"],
            "mensagem": mensagem,
        })

    return pd.DataFrame(linhas)

# Junta todos os casos de rollover da execução em uma mensagem geral, com resumo dos bônus, valores e clientes mais relevantes.
def compila_rollover_unico(
        fila_rollover: pd.DataFrame,
        qtd_ocorrencias: int,
        fila_original: pd.DataFrame
    ) -> pd.DataFrame:

    if fila_rollover.empty:
        return _linha_sem_dados("03_rollover", "🟡 Relatório de Rollover Não Cumprido")

    # ---------------------------------------------------------
    # Quantidades principais
    # ---------------------------------------------------------

    qtd_clientes = len(fila_rollover)

    qtd_bonus = fila_original["bonus_id"].nunique()

    valor_total = fila_original["valor"].sum()

    # ---------------------------------------------------------
    # IDs dos bônus
    # ---------------------------------------------------------

    bonus_ids = (
        fila_original["bonus_id"]
        .dropna()
        .drop_duplicates()
        .astype(int)
        .astype(str)
        .tolist()
    )

    bonus_ids_texto = ", ".join(bonus_ids)

    # ---------------------------------------------------------
    # Maiores valores por cliente
    # ---------------------------------------------------------

    maiores = (
        fila_rollover
        .sort_values("valor", ascending=False)
        .head(5)
    )

    clientes_maiores = []

    for _, row in maiores.iterrows():

        clientes_maiores.append(
            f"{int(row['client_id'])} "
            f"(R$ {row['valor']:.2f})"
        )

    maiores_clientes_texto = ", ".join(clientes_maiores)

    # ---------------------------------------------------------
    # Status predominante
    # ---------------------------------------------------------

    status_mode = fila_original["status"].mode()

    status_principal = (
        int(status_mode.iloc[0])
        if not status_mode.empty
        else None
    )

    # ---------------------------------------------------------
    # Horário de geração
    # ---------------------------------------------------------

    horario_geracao = horario_sp()  # horário de SP (UTC-3), servidor roda em UTC

    # ---------------------------------------------------------
    # Mensagem de PREVIEW
    #
    # Continua existindo para auditoria/conferência,
    # mas não será usada como payload do template.
    # ---------------------------------------------------------

    mensagem_preview = (
        "📊 Relatório operacional — Rollover não cumprido\n"
        f"Gerado às {horario_geracao}\n\n"
        f"📊 {qtd_ocorrencias} ocorrências identificadas\n"
        f"👥 {qtd_clientes} clientes afetados\n"
        f"🎁 {qtd_bonus} bônus envolvidos\n"
        f"💰 R$ {valor_total:.2f} pendentes\n\n"
        f"🎯 Concentração por bônus:\n"
        f"Bônus: {bonus_ids_texto}\n\n"
        f"🚨 Maiores valores pendentes:\n"
        f"{maiores_clientes_texto}\n\n"
        f"📌 Status predominante: {status_principal}\n\n"
        "Relatórios enviados para análise pelas áreas responsáveis."
    )

    # ---------------------------------------------------------
    # Retorno para a fila
    # ---------------------------------------------------------

    return pd.DataFrame([{
        "relatorio": "03_rollover",

        "client_id": None,

        "bonus_id": None,

        "mensagem": mensagem_preview,

        # Variáveis do template
        "variavel1": horario_geracao,
        "variavel2": str(qtd_ocorrencias),
        "variavel3": str(qtd_clientes),
        "variavel4": str(qtd_bonus),
        "variavel5": f"{valor_total:.2f}",
        "variavel6": bonus_ids_texto,
        "variavel7": maiores_clientes_texto,
        "variavel8": (
            str(status_principal)
            if status_principal is not None
            else ""
        ),

        # Reservadas para futuras necessidades
        "variavel9": None,
        "variavel10": None,
    }])


# Junta todos os casos de reuso da execução em uma mensagem geral segmentada em variaveis (mesmo padrão do rollover).
def compila_reuso_unico(
        fila_reuso: pd.DataFrame,
        qtd_ocorrencias: int,
        fila_original: pd.DataFrame,
        variaveis: dict
    ) -> pd.DataFrame:
    """
    Compila o relatório #2 (reuso) em uma única linha para o template
    de disparo.

    fila_reuso:
        Resultado consolidado por cliente.

    fila_original:
        Resultado granular de processa_relatorio, antes da consolidação.
        Mantido para auditoria/contagem das ocorrências originais.

    qtd_ocorrencias:
        Quantidade de pares client_id + bonus_id identificados na rodada.

    variaveis:
        Dicionário com variavel1..variavel10 preparado a partir das
        concentrações por cliente e por bônus.
    """

    if fila_reuso.empty:
        linha_sem_dados = _linha_sem_dados("02_reuso", "🟡 Alerta de Reuso de Bônus")
        # variavel9 do reuso nunca vai vazia/None: na ausência, "0".
        linha_sem_dados["variavel9"] = "0"
        return linha_sem_dados

    horario_geracao = variaveis.get(
        "variavel1",
        horario_sp()
    )

    mensagem_preview = monta_mensagem_reuso(fila_reuso)

    return pd.DataFrame([{
        "relatorio": "02_reuso",
        "client_id": None,
        "bonus_id": None,
        "mensagem": mensagem_preview,

        "variavel1": horario_geracao,
        "variavel2": variaveis.get("variavel2", "0"),
        "variavel3": variaveis.get("variavel3", "0"),
        "variavel4": variaveis.get("variavel4", "0"),
        "variavel5": variaveis.get("variavel5", "0"),
        "variavel6": variaveis.get("variavel6", ""),
        "variavel7": variaveis.get("variavel7", ""),
        "variavel8": variaveis.get("variavel8", ""),
        # "or" cobre ausente, None e string vazia -> sempre "0" na ausência
        "variavel9": variaveis.get("variavel9") or "0",
        "variavel10": variaveis.get("variavel10", "últimas 24h"),
    }])


# Junta todos os casos de rajada/automação da execução em uma mensagem geral segmentada em variaveis (mesmo padrão do rollover).
def compila_rajada_unico(fila_rajada: pd.DataFrame, qtd_ocorrencias: int) -> pd.DataFrame:
    """
    Mesmo padrão de compila_rollover_unico/compila_reuso_unico, pro
    relatório #7 (rajada). Diferente dos outros dois, aqui já chega 1
    linha por cliente (a chave do #7 é só client_id -- não existe
    "consolida_rajada_por_cliente" porque não há o que consolidar).

    Depende de processa_relatorio ter carregado qtd_bonus_distintos
    junto (ver comentário em processa_relatorio) -- sem isso não dá pra
    calcular variavel4/variavel5 aqui.
    """
    if fila_rajada.empty:
        return _linha_sem_dados("07_rajada", "🔴 Relatório de Rajada/Automação")

    qtd_clientes = len(fila_rajada)  # == qtd_ocorrencias, chave é só client_id
    total_bonus_distintos = int(fila_rajada["qtd_bonus_distintos"].sum())
    maior_rajada = int(fila_rajada["qtd_bonus_distintos"].max())

    client_ids_texto = ", ".join(str(int(c)) for c in fila_rajada["client_id"])

    top = fila_rajada.sort_values(["qtd_bonus_distintos", "valor"], ascending=False).head(5)
    maiores_clientes_texto = ", ".join(
        f"{int(r['client_id'])} ({int(r['valor'])} bônus em {int(r['qtd_bonus_distintos'])} tipos)"
        for _, r in top.iterrows()
    )

    horario_geracao = horario_sp()  # horário de SP (UTC-3), servidor roda em UTC

    linhas_texto = "\n".join(f"• {m}" for m in fila_rajada["mensagem"])
    mensagem_preview = _trunca_por_bytes(
        "🔴 Relatório operacional — Rajada/Automação\n"
        f"Gerado às {horario_geracao}\n\n"
        f"📊 {qtd_ocorrencias} clientes com possível automação\n"
        f"🎯 {total_bonus_distintos} bônus distintos somados\n"
        f"🚨 Maior rajada individual: {maior_rajada} tipos diferentes\n\n"
        f"Clientes: {client_ids_texto}\n\n"
        f"{linhas_texto}",
        3500,
    )

    return pd.DataFrame([{
        "relatorio": "07_rajada",
        "client_id": None,
        "bonus_id": None,
        "mensagem": mensagem_preview,

        "variavel1": horario_geracao,
        "variavel2": str(qtd_ocorrencias),
        "variavel3": str(qtd_clientes),
        "variavel4": str(total_bonus_distintos),
        "variavel5": str(maior_rajada),
        "variavel6": client_ids_texto,
        "variavel7": maiores_clientes_texto,
        "variavel8": "",

        "variavel9": None,
        "variavel10": None,
    }])


# Junta todos os casos de cadastro recente + bônus da execução em uma mensagem geral segmentada em variaveis (mesmo padrão do rollover).
def compila_cadastro_unico(fila_cadastro: pd.DataFrame, qtd_ocorrencias: int) -> pd.DataFrame:
    """
    Mesmo padrão dos demais compila_*_unico, pro relatório #8 (cadastro
    recente + bônus sem depósito). Também já vem 1 linha por cliente
    (chave = só client_id).

    Depende de processa_relatorio_cadastro_recente ter carregado
    tipo_caso e client_id_anterior_mesma_identidade junto (ver comentário
    lá) -- é o que permite separar, nas variaveis, quantos casos são
    primeira ocorrência, reincidência do mesmo client_id, e reincidência
    por identidade (conta nova, mesma pessoa).
    """
    if fila_cadastro.empty:
        return _linha_sem_dados("08_cadastro_recente", "🔴 Relatório de Cadastro Recente + Saque")

    qtd_clientes = len(fila_cadastro)  # == qtd_ocorrencias, chave é só client_id
    qtd_reincidentes_client_id = int((fila_cadastro["tipo_caso"] == "reincidente_client_id").sum())
    qtd_reincidentes_identidade = int((fila_cadastro["tipo_caso"] == "reincidente_identidade").sum())

    client_ids_texto = ", ".join(str(int(c)) for c in fila_cadastro["client_id"])

    reincidentes_identidade = fila_cadastro[fila_cadastro["tipo_caso"] == "reincidente_identidade"]
    pares_identidade_texto = ", ".join(
        f"{int(r['client_id'])}→{int(r['client_id_anterior_mesma_identidade'])}"
        for _, r in reincidentes_identidade.iterrows()
    )

    tempo_medio_minutos = fila_cadastro["valor"].astype(float).mean()

    horario_geracao = horario_sp()  # horário de SP (UTC-3), servidor roda em UTC

    linhas_texto = "\n".join(f"• {m}" for m in fila_cadastro["mensagem"])
    mensagem_preview = _trunca_por_bytes(
        "🔴 Relatório operacional — Cadastro Recente + Bônus\n"
        f"Gerado às {horario_geracao}\n\n"
        f"📊 {qtd_ocorrencias} casos identificados\n"
        f"🔁 {qtd_reincidentes_client_id} reincidentes (mesmo client_id)\n"
        f"🕵️ {qtd_reincidentes_identidade} reincidentes por identidade (conta nova, mesma pessoa)\n"
        f"⏱️ Tempo médio até o bônus: {tempo_medio_minutos:.1f} min\n\n"
        f"Clientes: {client_ids_texto}\n\n"
        f"{linhas_texto}",
        3500,
    )

    return pd.DataFrame([{
        "relatorio": "08_cadastro_recente",
        "client_id": None,
        "bonus_id": None,
        "mensagem": mensagem_preview,

        "variavel1": horario_geracao,
        "variavel2": str(qtd_ocorrencias),
        "variavel3": str(qtd_clientes),
        "variavel4": str(qtd_reincidentes_client_id),
        "variavel5": str(qtd_reincidentes_identidade),
        "variavel6": client_ids_texto,
        "variavel7": pares_identidade_texto,
        "variavel8": f"{tempo_medio_minutos:.1f}",

        "variavel9": None,
        "variavel10": None,
    }])

# Junta os casos de conta-nova-com-pix-ja-usado por CONTA ANTERIOR, pra achar concentração (1 conta antiga recebendo várias contas novas).
def consolida_pix_conta_nova_por_anterior(fila_pix: pd.DataFrame) -> pd.DataFrame:
    """Agrupa por client_id_anterior -- se a MESMA conta antiga aparece
    como 'conta anterior' de várias contas novas diferentes nesta
    execução, é sinal ainda mais forte que um caso isolado (ex.: uma
    conta virando ponto de recebimento de vários laranjas). Mesmo
    espírito de consolida_reuso_por_bonus (concentração), aplicado aqui
    pro lado 'conta anterior' do relatório #9."""
    if fila_pix.empty:
        return fila_pix
 
    linhas = []
    for client_id_anterior, grupo in fila_pix.groupby("client_id_anterior"):
        linhas.append({
            "client_id_anterior": int(client_id_anterior),
            "qtd_contas_novas": grupo["client_id"].nunique(),
            "status_anterior": grupo.iloc[0].get("status_usuario_anterior"),
        })
    return pd.DataFrame(linhas).sort_values("qtd_contas_novas", ascending=False)
 
 
# Junta todos os casos de conta-nova-com-pix-ja-usado da execução em uma mensagem geral segmentada em variaveis (mesmo padrão do rollover/reuso).
def compila_pix_conta_nova_unico(fila_pix: pd.DataFrame, qtd_ocorrencias: int) -> pd.DataFrame:
    """
    Mesmo padrão dos demais compila_*_unico, pro relatório #9 (conta nova
    com PIX já vinculado a outra conta). fila_pix já vem 1 linha por par
    conta-nova x conta-anterior (saída direta de processa_relatorio, sem
    consolidação por cliente antes -- volume esperado baixo o suficiente
    pra não precisar).
 
    Acrescenta a visão de concentração por conta ANTERIOR
    (consolida_pix_conta_nova_por_anterior) -- se uma mesma conta antiga
    está recebendo várias contas novas, é o sinal mais forte que esse
    relatório pode carregar (mesmo raciocínio da concentração por bônus
    no #2).
    """
    if fila_pix.empty:
        return _linha_sem_dados("09_pix_conta_nova", "🔴 Relatório operacional — Conta Nova com PIX Já Vinculado")
 
    qtd_contas_novas = fila_pix["client_id"].nunique()
    qtd_contas_anteriores = fila_pix["client_id_anterior"].nunique()
 
    por_anterior = consolida_pix_conta_nova_por_anterior(fila_pix)
    concentracao = por_anterior[por_anterior["qtd_contas_novas"] >= 2]
 
    status_novas_suspeitos = int(
        fila_pix.drop_duplicates("client_id")["status_usuario"].apply(
            lambda s: int(s) != 1 if pd.notna(s) else False
        ).sum()
    )
 
    client_ids_texto = ", ".join(str(int(c)) for c in fila_pix["client_id"].drop_duplicates())

    # SEMPRE preenchido (diferente da concentração abaixo) -- pares
    # conta-nova -> conta-anterior, 1 por caso. Antes so existia a
    # variavel de concentracao, que so populava quando a MESMA conta
    # anterior recebia 2+ contas novas -- no caso comum (cada conta nova
    # ligada a uma anterior diferente), nenhuma variavel trazia quem eram
    # as contas anteriores (achado em producao, 04/set/2026).
    pares_texto = ", ".join(
        f"{int(r['client_id'])}\u2192{int(r['client_id_anterior'])}"
        for _, r in fila_pix.drop_duplicates(["client_id", "client_id_anterior"]).iterrows()
    )
 
    concentracao_texto = ", ".join(
        f"{int(r['client_id_anterior'])} ({int(r['qtd_contas_novas'])} contas novas)"
        for _, r in concentracao.iterrows()
    )
 
    horario_geracao = horario_sp()  # horário de SP (UTC-3), servidor roda em UTC
 
    linhas_texto = "\n".join(f"• {m}" for m in fila_pix.drop_duplicates("client_id")["mensagem"])
    mensagem_preview = _trunca_por_bytes(
        "🔴 Relatório operacional — Conta Nova com PIX Já Vinculado\n"
        f"Gerado às {horario_geracao}\n\n"
        f"📊 {qtd_contas_novas} conta(s) nova(s) vinculada(s) a chave já usada\n"
        f"🔗 {qtd_contas_anteriores} conta(s) anterior(es) envolvida(s)\n"
        f"⚠️ {status_novas_suspeitos} conta(s) nova(s) já com status != Active\n"
        + (f"🎯 Concentração (mesma conta anterior, várias contas novas): {concentracao_texto}\n"
           if concentracao_texto else "")
        + f"\n{linhas_texto}",
        3500,
    )
 
    return pd.DataFrame([{
        "relatorio": "09_pix_conta_nova",
        "client_id": None,
        "bonus_id": None,
        "mensagem": mensagem_preview,
 
        "variavel1": horario_geracao,
        "variavel2": str(qtd_ocorrencias),
        "variavel3": str(qtd_contas_novas),
        "variavel4": str(qtd_contas_anteriores),
        "variavel5": str(status_novas_suspeitos),
        "variavel6": client_ids_texto,
        "variavel7": pares_texto,
        "variavel8": concentracao_texto,
 
        "variavel9": None,
        "variavel10": None,
    }])
 
# =============================================================================
# COMPILAÇÃO EM 1 MENSAGEM POR RELATÓRIO (LEGADO) -- compila_relatorio_unico
# junta todos os casos de um relatório numa única mensagem, mas só com o
# campo "mensagem" (sem variavel1..10). #2/#3/#7/#8 agora usam suas
# próprias funções compila_*_unico (acima), que já preenchem as variaveis
# segmentadas. Esta função fica mantida como utilitário genérico, caso um
# relatório futuro precise só da compilação simples.
# =============================================================================

def _trunca_por_bytes(texto: str, max_bytes: int) -> str:
    """Trunca por BYTES (UTF-8), não por caracteres. Necessário porque
    VARCHAR(n) no Redshift é definido em bytes, e emoji (4 bytes cada) e
    acentos/cedilha do português (2 bytes cada) fazem o tamanho em bytes
    passar longe do tamanho em caracteres do Python -- um corte por
    len(str) parecia seguro (3900 "caracteres") mas na prática virava bem
    mais que 4000 bytes reais (achado em produção, 01/set/2026:
    "value too long for type character varying(4000)" mesmo cortando a
    string por caracteres antes)."""
    codificado = texto.encode("utf-8")
    if len(codificado) <= max_bytes:
        return texto
    cortado = codificado[:max_bytes]
    # decode com errors="ignore" descarta bytes finais que sobraram no
    # meio de um caractere multibyte cortado pela metade
    return cortado.decode("utf-8", errors="ignore")

# Junta vários alertas do mesmo relatório em uma única mensagem e limita seu tamanho.
def compila_relatorio_unico(fila: pd.DataFrame, nome_relatorio: str, titulo: str) -> pd.DataFrame:
    """Se houver mais de 1 caso no relatório nesta execução, substitui as
    N linhas por 1 única linha com todos os casos listados. Se houver só 1
    caso, mantém como está -- mas SEMPRE normaliza pras 4 colunas que a
    tabela fila_disparo_alerta_bonus realmente tem (relatorio, client_id,
    bonus_id, mensagem). Sem isso, colunas extras usadas só internamente
    pela consolidação (valor, status) vazam pro INSERT quando o relatório
    tem 1 caso só (não passa pela consolidação por cliente, que é quem
    normalmente descarta essas colunas) -- achado em produção, 01/set/2026:
    "column 'valor' of relation 'fila_disparo_alerta_bonus' does not exist"."""
    colunas_tabela = ["relatorio", "client_id", "bonus_id", "mensagem"]
    LIMITE_BYTES_COLUNA = 4000
    MARGEM_SEGURANCA = 3500  # fica bem abaixo do limite real da coluna, de propósito

    if fila.empty:
        return fila[colunas_tabela] if all(c in fila.columns for c in colunas_tabela) else fila

    if len(fila) <= 1:
        return fila[colunas_tabela]

    qtd_casos = len(fila)
    linhas_texto = "\n".join(f"• {m}" for m in fila["mensagem"])
    mensagem = f"{titulo} — {qtd_casos} casos nesta execução:\n\n{linhas_texto}"

    if len(mensagem.encode("utf-8")) > MARGEM_SEGURANCA:
        sufixo = f"\n\n[...lista cortada, {qtd_casos} casos no total -- ver fila completa no banco]"
        orcamento_corpo = MARGEM_SEGURANCA - len(sufixo.encode("utf-8"))
        cabecalho = f"{titulo} — {qtd_casos} casos nesta execução:\n\n"
        orcamento_linhas = orcamento_corpo - len(cabecalho.encode("utf-8"))
        corpo_cortado = _trunca_por_bytes(linhas_texto, max(orcamento_linhas, 0))
        mensagem = cabecalho + corpo_cortado + sufixo

    return pd.DataFrame([{
        "relatorio": nome_relatorio,
        "client_id": None,  # mensagem cobre vários clientes -- não faz sentido 1 client_id só
        "bonus_id": None,
        "mensagem": mensagem,
    }])


# =============================================================================
# LÓGICA DE DEDUP / ESCALADA -- idêntica à validada em teste local
# =============================================================================
# É o coração da deduplicação: verifica o histórico e decide se cada caso deve gerar novo alerta, ser ignorado ou gerar uma escalada.
def processa_relatorio(nome_relatorio, df_deteccao, colunas_chave, gera_mensagem,
                        df_log, escalada_por_valor):
    """
    Motor genérico de dedup/reincidência usado pelos relatórios #2, #3 e
    #7 (o #8 tem regra própria demais pra caber aqui -- ver
    processa_relatorio_cadastro_recente). Resumo do mecanismo (detalhe
    completo no cabeçalho do arquivo, seção "O QUE CONTA COMO
    REINCIDENTE"):

    1. Monta uma "chave" por linha detectada, concatenando as colunas de
       colunas_chave (ex.: client_id+bonus_id pro reuso). É essa chave que
       identifica "o mesmo caso" entre rodadas diferentes.
    2. Pra cada linha, busca se essa chave já existe no log (df_log) e se
       o ÚLTIMO disparo dela foi HOJE (ja_disparado_hoje).
    3. Decide se dispara:
         - Nunca visto, ou visto num dia anterior -> dispara (é a
           primeira vez que aparece HOJE).
         - Já visto hoje E escalada_por_valor=True -> só dispara se o
           valor desta rodada for MAIOR que o valor_referencia salvo no
           log (ex.: reuso subiu de 5x pra 8x) -- é a "reincidência que
           piorou".
         - Já visto hoje E escalada_por_valor=False -> NUNCA dispara de
           novo no mesmo dia (usado pelo #3, onde a chave já inclui o
           timestamp do evento, então repetir a mesma chave no mesmo dia
           não deveria nem acontecer na prática).
    4. Cada disparo grava uma linha nova de log (log_novo), incrementando
       qtd_disparos e atualizando valor_referencia/ultimo_disparo -- é
       esse log que a PRÓXIMA rodada vai ler pra repetir o processo.
    """
    if df_deteccao.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = df_deteccao.copy()
    df["chave"] = df[colunas_chave].astype(str).agg(":".join, axis=1)

    log_relatorio = df_log[df_log["relatorio"] == nome_relatorio]
    hoje = pd.Timestamp.now().normalize()

    linhas_fila, log_novo = [], []

    for _, row in df.iterrows():
        existente = log_relatorio[log_relatorio["chave"] == row["chave"]]
        ja_disparado_hoje = (
            not existente.empty
            and pd.notna(existente.iloc[0]["ultimo_disparo"])
            and pd.Timestamp(existente.iloc[0]["ultimo_disparo"]).normalize() == hoje
        )

        if not ja_disparado_hoje:
            dispara = True
        elif escalada_por_valor:
            dispara = row["valor"] > existente.iloc[0]["valor_referencia"]
        else:
            dispara = False  # chave já inclui timestamp do evento -- nunca repete

        if dispara:
            # Carrega TODAS as colunas da detecção (não só client_id/bonus_id/
            # valor/status) -- necessário pra #7 (rajada) levar
            # qtd_bonus_distintos adiante até a compilação de variaveis
            # (compila_rajada_unico), e pra #3 (rollover) levar
            # source_updated_at. Colunas extras que um relatório não usa
            # simplesmente ficam sem uso mais adiante -- não atrapalham.
            linha = row.drop(labels=["chave"]).to_dict()
            linha.update({
                "relatorio": nome_relatorio,
                "client_id": int(row["client_id"]),
                "bonus_id": int(row["bonus_id"]) if pd.notna(row.get("bonus_id")) else None,
                "mensagem": gera_mensagem(row),
            })
            linhas_fila.append(linha)
            qtd_disparos_anterior = int(existente.iloc[0]["qtd_disparos"]) if not existente.empty else 0
            log_novo.append({
                "relatorio": nome_relatorio,
                "chave": row["chave"],
                "valor_referencia": float(row["valor"]),
                "ultimo_disparo": pd.Timestamp.now(),
                "qtd_disparos": qtd_disparos_anterior + 1,
            })

    return pd.DataFrame(linhas_fila), pd.DataFrame(log_novo)

# Faz a mesma função de deduplicação, mas com regras especiais para o relatório #8, incluindo reincidência por identidade.
def processa_relatorio_cadastro_recente(df_deteccao: pd.DataFrame, df_log: pd.DataFrame):
    """
    Versão especializada de processa_relatorio() só pro relatório 08.
    A genérica não distingue "mesmo client_id repetindo" de "conta nova,
    mesma pessoa de uma conta já alertada" (reincidência por identidade) --
    aqui os dois casos têm mensagem própria, como pedido na revisão do
    relatório 08 (estudo de hunter de bônus, 02/set/2026):
      - Mesmo client_id já alertado HOJE -> suprime (não repete no mesmo dia).
      - Mesmo client_id já alertado em dia ANTERIOR -> dispara, mensagem de
        reincidência do PRÓPRIO client_id.
      - client_id NOVO, mas identidade (CPF/telefone/nome+sobrenome+
        nascimento) bate com um client_id JÁ alertado antes -> dispara,
        mensagem de reincidência por IDENTIDADE (conta nova, mesma pessoa).
      - Nenhum dos casos acima -> dispara, mensagem padrão de 1a ocorrência.
    """
    if df_deteccao.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = df_deteccao.copy()
    df["chave"] = df["client_id"].astype(str)

    log_relatorio = df_log[df_log["relatorio"] == "08_cadastro_recente"]
    hoje = pd.Timestamp.now().normalize()

    linhas_fila, log_novo = [], []

    for _, row in df.iterrows():
        existente = log_relatorio[log_relatorio["chave"] == row["chave"]]
        ja_disparado_hoje = (
            not existente.empty
            and pd.notna(existente.iloc[0]["ultimo_disparo"])
            and pd.Timestamp(existente.iloc[0]["ultimo_disparo"]).normalize() == hoje
        )

        if ja_disparado_hoje:
            continue  # já informado hoje -- não repete

        if not existente.empty:
            mensagem = msg_cadastro_recente_reincidente_client_id(row, existente.iloc[0])
            tipo_caso = "reincidente_client_id"
        elif pd.notna(row.get("client_id_anterior_mesma_identidade")):
            mensagem = msg_cadastro_recente_reincidente_identidade(row)
            tipo_caso = "reincidente_identidade"
        else:
            mensagem = msg_cadastro_recente(row)
            tipo_caso = "novo"

        linhas_fila.append({
            "relatorio": "08_cadastro_recente",
            "client_id": int(row["client_id"]),
            "bonus_id": None,
            "valor": row.get("valor"),
            "status": None,
            # tipo_caso e client_id_anterior_mesma_identidade não vão pra
            # tabela final (normaliza_fila_disparo descarta o que não está
            # em COLUNAS_FILA) -- servem só de insumo pra
            # compila_cadastro_unico montar variavel4/5/7 (contagem de
            # reincidência e pares identidade→conta original).
            "tipo_caso": tipo_caso,
            "client_id_anterior_mesma_identidade": row.get("client_id_anterior_mesma_identidade"),
            "mensagem": mensagem,
        })
        qtd_disparos_anterior = int(existente.iloc[0]["qtd_disparos"]) if not existente.empty else 0
        log_novo.append({
            "relatorio": "08_cadastro_recente",
            "chave": row["chave"],
            "valor_referencia": float(row["valor"]),
            "ultimo_disparo": pd.Timestamp.now(),
            "qtd_disparos": qtd_disparos_anterior + 1,
        })

    return pd.DataFrame(linhas_fila), pd.DataFrame(log_novo)


# =============================================================================
# NORMALIZAÇÃO DA FILA FINAL -- colunas reais de inplay.fila_disparo_alerta_bonus
# (mensagem única + variavel1..variavel10, usadas pelo template de disparo
# segmentado). Por enquanto só o relatório #3 (rollover) preenche as
# variaveis -- ver compila_rollover_unico -- os demais relatórios continuam
# só com "mensagem" e ficam com as variaveis em branco (None) até serem
# migrados pro mesmo padrão.
# =============================================================================

COLUNAS_FILA = [
    "relatorio",
    "client_id",
    "bonus_id",
    "mensagem",
    "variavel1",
    "variavel2",
    "variavel3",
    "variavel4",
    "variavel5",
    "variavel6",
    "variavel7",
    "variavel8",
    "variavel9",
    "variavel10",
]

VARIAVEIS_TEMPLATE = [
    "variavel1",
    "variavel2",
    "variavel3",
    "variavel4",
    "variavel5",
    "variavel6",
    "variavel7",
    "variavel8",
    "variavel9",
    "variavel10",
]


# Garante que a fila final tenha exatamente as colunas da tabela de destino, preenchendo com None o que faltar.
def normaliza_fila_disparo(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty and len(df.columns) == 0:
        return pd.DataFrame(columns=COLUNAS_FILA)

    df = df.copy()
    for coluna in COLUNAS_FILA:
        if coluna not in df.columns:
            df[coluna] = None

    return df[COLUNAS_FILA]


# Valida que nenhuma variável de template carrega quebra de linha/TAB, que quebrariam o envio pela plataforma de disparo.
def valida_variaveis_template(df: pd.DataFrame, logger):
    if df.empty:
        return df

    for coluna in VARIAVEIS_TEMPLATE:
        if coluna not in df.columns:
            continue

        valores = df[coluna].dropna().astype(str)
        if valores.empty:
            continue

        problemas = valores[valores.str.contains(r"[\r\n\t]", regex=True)]
        if not problemas.empty:
            logger.error(f"[DISPARO_BONUS] Variável {coluna} contém quebra de linha/TAB.")
            raise ValueError(f"Variável {coluna} contém caracteres inválidos para template.")

    return df


# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================
# É a função principal/orquestradora: chama todas as outras funções na ordem correta e grava os resultados no banco.
def executar_disparo_alertas_bonus(df_fact_bonus: pd.DataFrame, cliente: str, db: str,
                                    partner_id: int, logger):
    """
    Parâmetros
    ----------
    df_fact_bonus : DataFrame já extraído por processa_bonus() nesta rodada
                    (mesmo usado por executar_agregacao_bonus_concessoes) --
                    usado só pelo relatório #7 (rajada). #2 e #3 continuam
                    consultando o banco (ver nota no topo do arquivo).
    cliente, db, partner_id, logger : mesmo padrão dos demais módulos.

    Sequência de UMA rodada (chamada a cada execução de processa_bonus(),
    tipicamente de hora em hora -- ver "CADÊNCIA" no topo do arquivo):

      1. Lê inplay.log_disparo_alerta_bonus inteiro (df_log) -- é contra
         esse histórico que cada relatório decide o que é novo vs.
         reincidência.
      2. Roda a detecção dos 4 relatórios (sql_reuso, sql_rollover,
         detecta_rajada, detecta_cadastro_recente_bonus) -- cada um
         retorna os CANDIDATOS desta rodada, sem ainda saber se são novos
         ou reincidência.
      3. Pro #8 especificamente, roda também marca_reincidencia_identidade
         -- decrypta e cruza CPF/telefone/identidade dos candidatos contra
         quem já foi alertado antes, pra distinguir reincidência por
         identidade de primeira ocorrência.
      4. processa_relatorio / processa_relatorio_cadastro_recente cruzam
         cada candidato com df_log e decidem: dispara ou não, e com qual
         mensagem (ver "O QUE CONTA COMO REINCIDENTE" no topo do arquivo).
         Aqui já sai tanto a fila de mensagens quanto o log_novo que vai
         alimentar a PRÓXIMA rodada.
      5. Consolida múltiplos casos do mesmo relatório numa única mensagem
         (consolida_reuso_por_cliente, compila_rollover_unico,
         compila_relatorio_unico) -- e normaliza tudo pras colunas reais
         da tabela de destino (relatorio, client_id, bonus_id, mensagem,
         variavel1..variavel10 -- ver normaliza_fila_disparo).
      6. Marca como 'enviado' as linhas que já estavam na fila desde a
         rodada anterior (a plataforma de disparo só faz SELECT, nunca
         marca ela mesma) e insere as mensagens novas desta rodada.
      7. Grava o log_novo em inplay.log_disparo_alerta_bonus -- é esse
         MERGE que fecha o ciclo e vira o histórico que a rodada seguinte
         vai ler no passo 1.
    """
    try:
        logger.info("[DISPARO_BONUS] Lendo log de disparo")
        ConnectionDB.conecta(db, cliente)
        df_log = ConnectionDB.executa_script(sql_leitura_log(), logger)

        logger.info("[DISPARO_BONUS] Rodando detecção dos 4 relatórios")
        ConnectionDB.conecta(db, cliente)
        df_reuso = ConnectionDB.executa_script(sql_reuso(partner_id), logger)
        ConnectionDB.conecta(db, cliente)
        df_rollover = ConnectionDB.executa_script(sql_rollover(partner_id), logger)
        df_rajada = detecta_rajada(df_fact_bonus)  # pandas, sem consulta ao banco

        logger.info("[DISPARO_BONUS] Checando cadastro recente + bônus (relatório #8)")
        df_cadastro_recente = detecta_cadastro_recente_bonus(df_fact_bonus, cliente, db, logger)
        if not df_cadastro_recente.empty:
            df_cadastro_recente = marca_reincidencia_identidade(df_cadastro_recente, cliente, db, logger)

        logger.info("[DISPARO_BONUS] Checando conta nova com PIX já vinculado (relatório #9)")
        ConnectionDB.conecta(db, cliente)
        df_pix_conta_nova = ConnectionDB.executa_script(sql_pix_conta_nova(), logger)

        fila_reuso, log_reuso = processa_relatorio(
            "02_reuso", df_reuso, ["client_id", "bonus_id"], msg_reuso, df_log, escalada_por_valor=True)
        fila_rollover, log_rollover = processa_relatorio(
            "03_rollover", df_rollover, ["client_id", "bonus_id", "source_updated_at"], msg_rollover,
            df_log, escalada_por_valor=False)
        fila_rajada, log_rajada = processa_relatorio(
            "07_rajada", df_rajada, ["client_id"], msg_rajada, df_log, escalada_por_valor=True)
        fila_cadastro, log_cadastro = processa_relatorio_cadastro_recente(df_cadastro_recente, df_log)
        # chave inclui client_id_anterior -- sem isso, 2 contas-anteriores
        # diferentes vinculadas à MESMA conta-nova+pix_key colidiriam na
        # mesma chave, e só a primeira dispararia (ver comentário em
        # sql_pix_conta_nova sobre o fan-out de 1 linha por par).
        fila_pix_conta_nova, log_pix_conta_nova = processa_relatorio(
            "09_pix_conta_nova", df_pix_conta_nova, ["client_id", "pix_key", "client_id_anterior"],
            msg_pix_conta_nova, df_log, escalada_por_valor=False)

        # TRATAMENTO (pedido 02/set/2026): logar explicitamente quando um
        # relatório não localizou nenhum caso NOVO nesta execução -- os
        # passos de consolidação/mensagem abaixo (monta_mensagem_reuso,
        # compila_rollover_unico, compila_relatorio_unico) já retornam
        # DataFrame vazio quando a entrada é vazia, então nada é inserido
        # -- isso aqui é só visibilidade em log de QUAL relatório ficou sem
        # caso, pra não descobrir só quando (ou se) uma mensagem chegasse
        # sem informação de verdade no destino.
        for nome, fila in [("02_reuso", fila_reuso), ("03_rollover", fila_rollover),
                            ("07_rajada", fila_rajada), ("08_cadastro_recente", fila_cadastro),
                            ("09_pix_conta_nova", fila_pix_conta_nova)]:
            if fila.empty:
                logger.info(f"[DISPARO_BONUS] {nome}: nenhum caso novo localizado -- nada será inserido")

        # Consolidação por cliente -- SÓ na fila final (o que vira mensagem).
        # O log de dedup (log_reuso/log_rollover) continua granular por
        # bonus_id, sem alteração -- é o que garante que a escalada
        # continua detectada corretamente por bônus individual, mesmo a
        # mensagem final sendo agrupada.
        #
        # Os 4 relatórios agora seguem o MESMO padrão de compilação
        # (mesma lógica validada com o rollover): guarda a contagem de
        # ocorrências ANTES de qualquer agrupamento, consolida por
        # cliente quando aplicável, e chama compila_<relatorio>_unico pra
        # gerar 1 linha final já com variavel1..variavel10 preenchidas.
        # ---------------------------------------------------------
        # REUSO (#2)
        # ---------------------------------------------------------
        
        # ------------------------------------------------------------------
        # TEMPORÁRIO -- exporta as DUAS opções de conteúdo pro e-mail, lado
        # a lado, pra decisão com dado real (ver conversa sobre conciliar
        # e-mail x WhatsApp). Remover depois que a decisão for tomada.
        #
        #   OPÇÃO A (_so_whatsapp): só os casos que dispararam mensagem
        #   NESTA execução (mesma fonte que virou WhatsApp) -- garante
        #   consistência total entre e-mail e mensagem.
        #
        #   OPÇÃO B (_panorama_completo): TUDO que bate no critério hoje,
        #   incluindo casos já avisados antes que não mudaram -- mais
        #   abrangente, mas os números não batem necessariamente com o
        #   que foi mandado no WhatsApp desta execução.
        # ------------------------------------------------------------------
        timestamp_execucao =  pd.Timestamp.now(tz=FUSO_SP).strftime("%Y%m%d_%H%M%S")
 
        if not fila_reuso.empty:
            salva_csv_conferencia(
                fila_reuso[["client_id", "bonus_id", "valor"]],
                f"comparativo_reuso_so_whatsapp_{timestamp_execucao}.csv")
        if not df_reuso.empty:
            salva_csv_conferencia(
                df_reuso,
                f"comparativo_reuso_panorama_completo_{timestamp_execucao}.csv")
 
        if not fila_rollover.empty:
            salva_csv_conferencia(
                fila_rollover[["client_id", "bonus_id", "valor", "status"]],
                f"comparativo_rollover_so_whatsapp_{timestamp_execucao}.csv")
        if not df_rollover.empty:
            salva_csv_conferencia(
                df_rollover,
                f"comparativo_rollover_panorama_completo_{timestamp_execucao}.csv")
 
        if EXPORTA_CSV_CONFERENCIA:
            logger.info(
                f"[DISPARO_BONUS] Exportado comparativo e-mail x WhatsApp "
                f"(sufixo {timestamp_execucao}) -- reuso: {len(fila_reuso)} só-whatsapp / "
                f"{len(df_reuso)} panorama completo | rollover: {len(fila_rollover)} "
                f"só-whatsapp / {len(df_rollover)} panorama completo"
            )
        # ------------------------------------------------------------------
        # FIM DO TRECHO TEMPORÁRIO
        # ------------------------------------------------------------------
        
        # ---------------------------------------------------------
        # REUSO (#2)
        # ---------------------------------------------------------

        qtd_ocorrencias_reuso = len(fila_reuso)
        fila_reuso_original = fila_reuso.copy()

        # Consolidação por cliente
        fila_reuso_cliente = consolida_reuso_por_cliente(
            fila_reuso_original
        )

        # Consolidação por bônus
        fila_reuso_bonus = consolida_reuso_por_bonus(
            fila_reuso_original
        )

        # Variáveis que serão utilizadas pelo template WhatsApp
        variaveis_reuso = prepara_variaveis_reuso(
            fila_reuso_cliente,
            fila_reuso_bonus
        )

        # Compila uma única mensagem/template para o relatório #2
        fila_reuso = compila_reuso_unico(
            fila_reuso_cliente,
            qtd_ocorrencias_reuso,
            fila_reuso_original,
            variaveis_reuso,
        )

        # ---------------------------------------------------------
        # ROLLOVER (#3) -- relatório onde esse formato de variaveis foi
        # validado primeiro (ver compila_rollover_unico).
        # ---------------------------------------------------------
        qtd_ocorrencias_rollover = len(fila_rollover)

        fila_rollover_original = fila_rollover.copy()

        fila_rollover = consolida_rollover_por_cliente(fila_rollover)

        fila_rollover = compila_rollover_unico(
            fila_rollover,
            qtd_ocorrencias_rollover,
            fila_rollover_original
        )

        # ---------------------------------------------------------
        # RAJADA (#7) -- chave já é só client_id, então já chega 1 linha
        # por cliente (sem consolidação prévia necessária).
        # ---------------------------------------------------------
        qtd_ocorrencias_rajada = len(fila_rajada)

        fila_rajada = compila_rajada_unico(fila_rajada, qtd_ocorrencias_rajada)

        # ---------------------------------------------------------
        # CADASTRO RECENTE (#8) -- idem, chave já é só client_id.
        # ---------------------------------------------------------
        qtd_ocorrencias_cadastro = len(fila_cadastro)

        fila_cadastro = compila_cadastro_unico(fila_cadastro, qtd_ocorrencias_cadastro)

        # ---------------------------------------------------------
        # PIX CONTA NOVA (#9) -- chave já inclui client_id_anterior, então
        # já chega 1 linha por par conta-nova x conta-anterior.
        # ---------------------------------------------------------
        qtd_ocorrencias_pix_conta_nova = len(fila_pix_conta_nova)

        fila_pix_conta_nova = compila_pix_conta_nova_unico(fila_pix_conta_nova, qtd_ocorrencias_pix_conta_nova)

        df_fila = pd.concat(
            [fila_reuso, fila_rollover, fila_rajada, fila_cadastro, fila_pix_conta_nova],
            ignore_index=True,
        )
        df_fila = normaliza_fila_disparo(df_fila)
        df_fila = valida_variaveis_template(df_fila, logger)

        df_log_novo = pd.concat(
            [log_reuso, log_rollover, log_rajada, log_cadastro, log_pix_conta_nova],
            ignore_index=True,
        )

        # TRATAMENTO (pedido 02/set/2026): marca como 'enviado' as linhas
        # que já estavam na fila ANTES desta execução -- a plataforma de
        # disparo (Twilio/Meta) só faz SELECT na tabela, nunca atualiza o
        # status ela mesma. Sem isso, o status fica NULL pra sempre e (se
        # o polling filtrar por status pendente) as mesmas linhas antigas
        # nunca saem da leva "a enviar", ou (se não filtrar) a plataforma
        # re-lê a fila inteira em todo ciclo de polling.
        # RESSALVA: isso marca "já teve ~1h de janela de polling desde que
        # entrou na fila" (intervalo entre execuções deste job), não
        # "confirmadamente entregue" -- não temos retorno da plataforma de
        # disparo neste fluxo. Só atualiza quem ainda está NULL, então não
        # sobrescreve nenhum status que a própria plataforma vier a setar
        # no futuro.
        logger.info("[DISPARO_BONUS] Marcando linhas pendentes como 'enviado' antes do novo insert")
        ConnectionDB.conecta(db, cliente)
        ConnectionDB.executa_dml(
            "UPDATE inplay.fila_disparo_alerta_bonus SET status = 'ENVIADO' WHERE status ='PENDENTE'",
            logger,
        )

        if df_fila.empty:
            logger.info("[DISPARO_BONUS] Nada novo para disparar nesta execução")
        else:
            logger.info(f"[DISPARO_BONUS] {len(df_fila)} mensagens novas -- gravando na fila")
            ConnectionDB.conecta(db, cliente)
            ConnectionDB.insere_dados_bulk('inplay.fila_disparo_alerta_bonus', df_fila, logger)

        if not df_log_novo.empty:
            logger.info(f"[DISPARO_BONUS] Atualizando log de disparo ({len(df_log_novo)} chaves)")
            ConnectionDB.conecta(db, cliente)
            ConnectionDB.deleta_dados('inplay.stg_log_disparo_alerta_bonus', '', logger)
            ConnectionDB.conecta(db, cliente)
            ConnectionDB.insere_dados_bulk('inplay.stg_log_disparo_alerta_bonus', df_log_novo, logger)
            ConnectionDB.conecta(db, cliente)
            ConnectionDB.mergeia_dados('inplay.stg_log_disparo_alerta_bonus',
                                        'inplay.log_disparo_alerta_bonus',
                                        df_log_novo, ['relatorio', 'chave'], logger)

        if not fila_pix_conta_nova.empty:
            salva_csv_conferencia(
                fila_pix_conta_nova,
                f"comparativo_pix_conta_nova_so_whatsapp_{timestamp_execucao}.csv")
        if not df_pix_conta_nova.empty:
            salva_csv_conferencia(
                df_pix_conta_nova,
                f"comparativo_pix_conta_nova_panorama_completo_{timestamp_execucao}.csv")

        logger.info("[DISPARO_BONUS] Execução concluída com sucesso")

    except Exception as e:
        logger.error(f"[DISPARO_BONUS] Erro: {e}")
        raise