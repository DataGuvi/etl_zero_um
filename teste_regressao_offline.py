"""
teste_regressao_offline.py
===========================
Teste de REGRESSÃO totalmente offline para o bug encontrado nesta
conversa: bisseccionar por tempo no modo "updated_at" podia gerar
`usuario+data_referencia` duplicado com valores de saldo incompletos
(cada metade da bisseção via só uma parte dos TypeId de um cliente e
agregava separadamente — ver o erro real do Redshift: "Found multiple
matches to update the same tuple", 757.087 chaves duplicadas).

Não bate em NENHUM serviço real (nem Metabase, nem Redshift, nem
precisa de config.py com credenciais de verdade) — simula um
"ClickHouse" mínimo em pandas e substitui só o método que fala com a
API do Metabase (`extrai_csv_nativo`). Toda a lógica real de
recursão/particionamento em `consume_api.py`
(`extrai_dados_card_por_periodo`, `_particiona_por_client_id`,
`_ponto_de_corte`, `_intervalo_dias_referencia`) roda SEM alteração
nenhuma — é exatamente o código de produção sendo exercitado.

Objetivo: se esse padrão de bug for reintroduzido no futuro (ex.:
alguém mexendo na bisseção sem perceber a armadilha), este teste
falha imediatamente, sem precisar esperar um erro de MERGE em
produção ou depender de volume real de dados.

Como rodar:
    python teste_regressao_offline.py

Sai com código 0 se passar, 1 se falhar — dá pra plugar em CI/pre-commit.
"""

import sys
import types
import re
import logging
from datetime import datetime, timedelta

import pandas as pd

# ------------------------------------------------------------------
# Stubs dos módulos externos — só o suficiente para importar
# consume_api.py sem precisar de config.py real, banco, etc.
# ------------------------------------------------------------------
for _modname in ['config', 'enums', 'database', 'send_email', 'util',
                  'agregacao_dim_usuario', 'db_logger',
                  'agregacao_cohort_retencao', 'sqlalchemy', 'psycopg2']:
    sys.modules.setdefault(_modname, types.ModuleType(_modname))

_cfg = sys.modules['config']
for _name in ['API_AUTH', 'API_USER_ZEROUM', 'API_PASS_ZEROUM',
              'API_USER_ENERGIABET', 'API_PASS_ENERGIABET',
              'API_ROTA_CSV', 'DB']:
    setattr(_cfg, _name, 'x')


class _FakeEnum:
    def __init__(self, v):
        self.value = v


_enumsmod = sys.modules['enums']
_enumsmod.MetabaseTable = _FakeEnum('t')
_enumsmod.MetabaseDatabase = _FakeEnum('d')
_enumsmod.MetabaseCard = _FakeEnum('c')


class _FakeConnectionDB:
    @staticmethod
    def conecta(*a, **k):
        pass


sys.modules['database'].ConnectionDB = _FakeConnectionDB
sys.modules['send_email'].send_email = lambda subject, body: None
sys.modules['util'].Util = type('Util', (), {})
sys.modules['agregacao_dim_usuario'].executar_agregacao_dim_usuario = lambda *a, **k: None
sys.modules['db_logger'].DBLogger = type(
    'DBLogger', (), {'__init__': lambda self, *a, **k: None}
)
sys.modules['agregacao_cohort_retencao'].executar_agregacao_cohort_retencao = lambda *a, **k: None
sys.modules['agregacao_cohort_retencao'].executar_kpi_diario_datatalk = lambda *a, **k: None
sys.modules['sqlalchemy'].create_engine = lambda *a, **k: None
sys.modules['sqlalchemy'].text = lambda x: x

import consume_api  # noqa: E402  (import depois dos stubs, de propósito)
ConsumeAPI = consume_api.ConsumeAPI

# silencia o log verboso de consume_api durante o teste (deixa só WARNING+)
logging.basicConfig(level=logging.WARNING, format="%(levelname)s - %(message)s")
log = logging.getLogger("teste_regressao_offline")


# ------------------------------------------------------------------
# "ClickHouse" fake: só o suficiente para simular ClientDailyBalance
# ------------------------------------------------------------------
def _monta_fixture():
    """
    Cenário desenhado para reproduzir o bug real: um cliente
    (ClientId=13, o "cliente-armadilha") com dois TypeId diferentes
    sincronizados em pontas OPOSTAS da janela incremental — um logo
    no início, outro quase no fim. Se o código bisseccionar por
    tempo, cada TypeId cai numa metade diferente da recursão -> duas
    linhas parciais para o mesmo usuario+dia -> duplicidade e soma
    incompleta (exatamente o padrão visto no log real: usuario
    14326507 com saldo_em_uso=1.02 numa linha e saldo_disponivel=0.24
    em outra, quando deveria ser uma linha só com os dois somados).

    Os outros 12 clientes (ClientId 1 a 12) só servem para dar volume
    e forçar alguma subdivisão por ClientId a acontecer — não expõem
    o bug sozinhos, porque cada um só tem 1 TypeId.
    """
    janela_inicio = datetime(2026, 7, 15, 0, 0, 0)
    janela_fim = datetime(2026, 7, 15, 10, 0, 0)

    linhas = []
    for client_id in range(1, 13):
        linhas.append({
            "ClientId": client_id, "TypeId": 2, "Balance": 10.0,
            "CreateDate": "2026-07-14 15:00:00",
            "_peerdb_synced_at": janela_inicio + timedelta(minutes=3),
        })

    # cliente-armadilha
    linhas.append({
        "ClientId": 13, "TypeId": 1, "Balance": 1.02,
        "CreateDate": "2026-07-14 08:00:00",
        "_peerdb_synced_at": janela_inicio + timedelta(minutes=2),
    })
    linhas.append({
        "ClientId": 13, "TypeId": 2, "Balance": 0.24,
        "CreateDate": "2026-07-14 08:00:00",
        "_peerdb_synced_at": janela_fim - timedelta(minutes=2),
    })

    return pd.DataFrame(linhas), janela_inicio, janela_fim


def _fake_extrai_csv_nativo(fixture):
    """
    Substitui ConsumeAPI.extrai_csv_nativo: interpreta a query SQL
    gerada por _sql_saldo_diario (lê as bordas de data/hora e a faixa
    de ClientId direto do TEXTO da query — sem tentar "entender"
    ClickHouse de verdade) e aplica a MESMA lógica de dedup +
    agregação em pandas que a query real faz no ClickHouse,
    devolvendo um CSV — como se fosse a resposta da API do Metabase.
    """
    def _fn(self, auth_id, id_database, sql_query, max_tentativas=3):
        m = re.search(r"BETWEEN '([^']+)' AND '([^']+)'", sql_query)
        ini_str, fim_str = m.group(1), m.group(2)
        cid_m = re.search(r"ClientId BETWEEN (\d+) AND (\d+)", sql_query)
        cid_min, cid_max = (int(cid_m.group(1)), int(cid_m.group(2))) if cid_m else (None, None)

        df = fixture.copy()
        df["_peerdb_synced_at"] = pd.to_datetime(df["_peerdb_synced_at"])
        ini_dt = datetime.strptime(ini_str, "%Y-%m-%dT%H:%M:%S")
        fim_dt = datetime.strptime(fim_str, "%Y-%m-%dT%H:%M:%S")
        df = df[(df["_peerdb_synced_at"] >= ini_dt) & (df["_peerdb_synced_at"] <= fim_dt)]

        if cid_min is not None:
            df = df[(df["ClientId"] >= cid_min) & (df["ClientId"] <= cid_max)]

        if df.empty:
            csv = ("usuario,data_referencia,saldo_em_uso,saldo_disponivel,"
                   "saldo_booking,saldo_bonus,saldo_total_dia,updated_at\n")
            return csv.encode()

        df["dia"] = pd.to_datetime(df["CreateDate"]).dt.date
        # dedup por ClientId+TypeId+dia (equivalente ao QUALIFY real)
        df = (df.sort_values("_peerdb_synced_at", ascending=False)
                .drop_duplicates(subset=["ClientId", "TypeId", "dia"]))

        agregado = df.pivot_table(
            index=["ClientId", "dia"], columns="TypeId", values="Balance",
            aggfunc="sum", fill_value=0.0,
        ).reset_index()
        for tipo in (1, 2, 3, 12):
            if tipo not in agregado.columns:
                agregado[tipo] = 0.0
        agregado["saldo_total_dia"] = (
            agregado[1] + agregado[2] + agregado[3] + agregado[12]
        )
        agregado["updated_at"] = (
            df.groupby(["ClientId", "dia"])["_peerdb_synced_at"].max().values
        )

        saida = pd.DataFrame({
            "usuario": agregado["ClientId"],
            "data_referencia": agregado["dia"],
            "saldo_em_uso": agregado[1],
            "saldo_disponivel": agregado[2],
            "saldo_booking": agregado[3],
            "saldo_bonus": agregado[12],
            "saldo_total_dia": agregado["saldo_total_dia"],
            "updated_at": agregado["updated_at"],
        })
        return saida.to_csv(index=False).encode()

    return _fn


def roda_teste():
    fixture, janela_inicio, janela_fim = _monta_fixture()

    api = ConsumeAPI.__new__(ConsumeAPI)  # instancia sem rodar __init__ (sem conexões reais)
    api.logger = logging.getLogger("teste_regressao_offline.api")
    api.LIMITE_LINHAS_METABASE = 5   # força subdivisão mesmo com poucas linhas sintéticas
    api.TETO_CLIENT_ID_METABASE = 50  # teto pequeno pra recursão de ClientId ficar rápida/legível

    consume_api.ConsumeAPI.extrai_csv_nativo = _fake_extrai_csv_nativo(fixture)

    chamadas = []
    original_extrai_csv = consume_api.ConsumeAPI.extrai_csv_nativo

    def _extrai_csv_com_log(self, auth_id, id_database, sql_query, max_tentativas=3):
        m = re.search(r"BETWEEN '([^']+)' AND '([^']+)'", sql_query)
        chamadas.append((m.group(1), m.group(2)))
        return original_extrai_csv(self, auth_id, id_database, sql_query, max_tentativas)

    consume_api.ConsumeAPI.extrai_csv_nativo = _extrai_csv_com_log

    df_resultado = api.extrai_dados_card_por_periodo(
        auth_id="fake", id_database="fake", id_card="card__TESTE",
        data_inicial_dt=janela_inicio, data_final_dt=janela_fim,
        campo_filtro="updated_at",
    )

    erros = []

    # 1) NENHUMA chamada pode ter estreitado a janela de tempo — é
    #    exatamente isso que o bug fazia, e é o que a correção proíbe
    #    no modo updated_at.
    janela_original = (
        janela_inicio.strftime('%Y-%m-%dT%H:%M:%S'),
        janela_fim.strftime('%Y-%m-%dT%H:%M:%S'),
    )
    chamadas_com_janela_estreitada = [c for c in chamadas if c != janela_original]
    if chamadas_com_janela_estreitada:
        erros.append(
            "REGRESSÃO DETECTADA: alguma chamada estreitou a janela de tempo "
            f"no modo updated_at (deveria SEMPRE usar {janela_original}). "
            f"Chamadas com janela diferente: {chamadas_com_janela_estreitada}"
        )

    # 2) nenhuma duplicidade de usuario+data_referencia no resultado final
    duplicados = df_resultado[
        df_resultado.duplicated(subset=["usuario", "data_referencia"], keep=False)
    ]
    if not duplicados.empty:
        erros.append(
            f"Duplicidade de usuario+data_referencia encontrada:\n{duplicados}"
        )

    # 3) o cliente-armadilha (13) tem que aparecer como UMA linha só,
    #    com os dois TypeId somados (1.02 + 0.24 = 1.26) — não como
    #    duas linhas parciais
    linha_13 = df_resultado[df_resultado["usuario"] == 13]
    if len(linha_13) != 1:
        erros.append(
            f"Esperava exatamente 1 linha para usuario=13, vieram {len(linha_13)}:\n{linha_13}"
        )
    else:
        total = float(linha_13.iloc[0]["saldo_total_dia"])
        if abs(total - 1.26) > 1e-6:
            erros.append(
                f"saldo_total_dia do usuario=13 deveria ser 1.26 (1.02+0.24), veio {total}"
            )

    if erros:
        print("FALHOU:")
        for e in erros:
            print(f" - {e}")
        return False

    print(
        f"PASS: {len(chamadas)} chamada(s) à API, todas com a janela de tempo "
        f"completa (nenhuma bisseção por tempo no modo updated_at). "
        f"{len(df_resultado)} linha(s) no resultado final, sem duplicidade, "
        "usuario=13 com saldo_total_dia=1.26 (1.02+0.24) correto."
    )
    return True


if __name__ == "__main__":
    ok = roda_teste()
    sys.exit(0 if ok else 1)
