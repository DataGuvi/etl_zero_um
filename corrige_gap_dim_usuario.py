"""
corrige_gap_dim_usuario.py (v2)
=================================
Script AVULSO (não faz parte do ETL incremental de consume_api.py) para:

  FASE 1 - DIAGNÓSTICO DO GAP
      Descobre quais 'id' existem no Metabase e NÃO existem em
      inplay.dim_usuario. Gera um CSV de auditoria antes de gravar
      qualquer coisa no banco.

      Extração via SQL NATIVO contra a tabela Client (não mais o card de
      Usuários): achado desta conversa -- o card tem 30+ colunas
      (incluindo campos cifrados pesados) e extrair só o `id` por ele
      fazia o Metabase materializar a linha inteira à toa. A mesma
      contagem (9.244.470 linhas) via SQL nativo saiu em ~500ms. Ver
      ExtratorMetabase._sql_ids.

  FASE 2 - CARGA DOS FALTANTES
      Usa os ids da Fase 1 e delega a carga para
      ConsumeAPI.backfill_dim_usuario_por_ids(cliente, ids) -- o método
      que já existe em produção em consume_api.py, em vez de reimplementar
      extração/merge aqui (evita duplicar lógica que muda com o tempo).

  FASE 3 - DIAGNÓSTICO DE INCOMPLETOS (colunas novas do ETL)
      Procura em inplay.dim_usuario os ids que já existem mas estão com as
      colunas NOVAS (ver abaixo) nulas. Gera CSV de auditoria.

  FASE 4 - CORREÇÃO DOS INCOMPLETOS
      Mesmo mecanismo da Fase 2 (backfill_dim_usuario_por_ids), agora para
      os ids da Fase 3.

MUDANÇA EM RELAÇÃO À v1 (importante)
-------------------------------------
consume_api.py deixou de aplicar Util().descriptografar() em
birth_date/first_name/mobile_number -- esses 3 campos ficaram CONGELADOS
em dim_usuario (não recebem mais atualização da incremental) e não são
mais lidos do card de Usuários. Por isso:

  - Este script NÃO importa/usa mais util.Util nem faz nenhum decrypt.
  - O diagnóstico de "incompletos" (Fase 3) parou de checar
    birth_date/first_name/mobile_number (não faz mais sentido -- eles
    sempre serão nulos/antigos daqui pra frente para quem nunca teve
    Frente B aplicada, e isso é esperado, não um gap de carga).
  - Passou a checar as colunas NOVAS incluídas no ETL (proteção de dados
    pessoais):
        Frente A (sem par em claro):
            lastname, taxnumber, documenttype, documentnumber,
            documentissuedby, isdocumentverified, kycstatus, kycdocsstatus
        Frente B (cifradas, vêm prontas do card -- sem decrypt em Python):
            first_name_protegido, mobile_number_protegido,
            birth_date_protegido

ATENÇÃO -- backfill pontual (por ids) x backfill histórico completo
---------------------------------------------------------------------
`backfill_dim_usuario_por_ids` foi feito para regularizar um número
PEQUENO de ids (ex.: algumas dezenas/centenas que ficaram incompletos por
falha pontual) -- cada chamada bate no Metabase com um filtro "IN (ids)".
Não é o mecanismo certo para popular as colunas novas em TODA a base
(9,2M+ linhas): para isso, consume_api.py já tem
`backfill_historico_protecao_dados_pessoais(cliente, data_inicio, data_fim)`,
que pagina por período e é a via recomendada para o rollout inicial.

Por segurança, este script AVISA (e não executa automaticamente) se a
Fase 3 encontrar um volume de incompletos acima de LIMITE_ALERTA_INCOMPLETOS
-- nesse caso, o rollout inicial provavelmente ainda não rodou, e o caminho
certo é o backfill histórico, não este script.

Reexecução segura
------------------
Idempotente: pode ser rodado de novo a qualquer momento.
  - Se o cache de ids do Metabase já existir, o download é pulado
    (use --forcar-download para refazer).
  - backfill_dim_usuario_por_ids faz upsert (MERGE por id), então rodar de
    novo não duplica nem quebra nada.

Uso
---
    python corrige_gap_dim_usuario.py --cliente ZEROUM
    python corrige_gap_dim_usuario.py --cliente ZEROUM --somente-diagnostico
    python corrige_gap_dim_usuario.py --cliente ENERGIABET --forcar-download
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests

from config import (
    API_AUTH, API_ROTA_CSV,
    API_USER_ZEROUM, API_PASS_ZEROUM,
    API_USER_ENERGIABET, API_PASS_ENERGIABET,
    DB,
)
from enums import MetabaseDatabase
from database import ConnectionDB
from consume_api import ConsumeAPI

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================

LIMITE_LINHAS_METABASE = 1048575  # teto de exportação CSV do Metabase (2^20 - 1)

# Ajuste se souber a data de registro do 1º usuário da base (evita janelas
# vazias no início da recursão). Um valor antigo "de sobra" não quebra nada,
# só gera 1-2 chamadas extras que voltam vazias.
DATA_INICIO_HISTORICO = "2018-01-01T00:00:00"

# Acima disso, a Fase 3 só avisa e NÃO chama backfill_dim_usuario_por_ids
# automaticamente -- ver nota "backfill pontual x histórico completo" no
# docstring do módulo.
LIMITE_ALERTA_INCOMPLETOS = 50_000

# -----------------------------------------------------------------------
# NOTA (pendência) -- platô em 1.048.575 linhas no PASSO 0
# -----------------------------------------------------------------------
# Numa execução real, a recursão do PASSO 0 chegou a janelas de ~6 dias
# (ex.: 2025-03-30 -> 2025-04-11) e AINDA retornava exatamente 1.048.575
# linhas -- ou é um pico real de cadastros nesse período (plausível: bases
# grandes costumam ter picos de importação/campanha), ou o filtro de
# CreationTime não está reduzindo a página por algum motivo (formatação,
# fuso, etc.). Não foi confirmado qual dos dois -- por ora, contornamos
# rodando a Fase 2 direto do CSV já gerado (--somente-fase-2-csv).
#
# Pra confirmar antes de confiar de novo no PASSO 0, rode no Metabase (SQL
# nativo) a contagem da MESMA janela que platô, isolada do CSV export
# (que trunca silenciosamente em 1.048.575 sem avisar):
#
#   SELECT count(*) FROM (
#       SELECT Id FROM Client
#       WHERE _peerdb_is_deleted = 0 AND PartnerId = 180
#         AND CreationTime >= '2025-03-30 06:11:02'
#         AND CreationTime <  '2025-04-11 14:50:54'
#       QUALIFY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY _peerdb_version DESC) = 1
#   )
#
# Se vier bem abaixo de 1.048.575 -> é bug no filtro (investigar
# formatação da data / fuso horário). Se vier próximo/acima -> é pico
# real, e o platô era esperado (a recursão eventualmente convergiria em
# janelas menores, só demorou mais do que o previsto).

# Tamanho de lote para as chamadas de correção (Fases 2 e 4). Cada chamada
# de backfill_dim_usuario_por_ids monta um filtro "IN (ids)" contra o
# Metabase (card) e uma query nativa (totalizador bet) -- lotes menores
# reduzem o tamanho da requisição e o raio de um eventual retry.
TAMANHO_LOTE_CORRECAO = 500

# Colunas novas do ETL (Frente A + Frente B) verificadas na Fase 3.
# Nomes exatamente como usados no UPDATE de
# backfill_historico_protecao_dados_pessoais em consume_api.py.
COLUNAS_NOVAS_ETL = [
    "lastname", "taxnumber", "documenttype", "documentnumber",
    "documentissuedby", "isdocumentverified", "kycstatus", "kycdocsstatus",
    "first_name_protegido", "mobile_number_protegido", "birth_date_protegido",
]

CLIENTE_CFG = {
    "ZEROUM": {
        "api_user": API_USER_ZEROUM,
        "api_pass": API_PASS_ZEROUM,
        "database": MetabaseDatabase.ClickhousePartnerZeroum.value,
        "partner_id": 180,  # confirmado em produção (ver consume_api.py: valida_aposta, etc.)
    },
    "ENERGIABET": {
        "api_user": API_USER_ENERGIABET,
        "api_pass": API_PASS_ENERGIABET,
        "database": MetabaseDatabase.ClickhousePartnerEnergiabet.value,
        "partner_id": 181,  # INFERIDO por analogia (Bônus/Pix) -- NÃO confirmado ainda
                            # especificamente para a tabela Client. Validar antes de
                            # rodar em produção (ver nota em processa_agregacao_pix).
    },
}


def get_logger():
    logger = logging.getLogger("corrige_gap_dim_usuario")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(h)
        fh = logging.FileHandler("corrige_gap_dim_usuario.log", encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(fh)
    return logger


# =============================================================================
# EXTRAÇÃO METABASE -- SQL NATIVO (achado desta conversa: o card de Usuários
# tem 30+ colunas, incluindo campos cifrados pesados -- extrair só o `id`
# via card fazia o Metabase materializar a linha inteira à toa. Rodando
# direto contra a tabela `Client` (ClickHouse), filtrando só o necessário,
# a mesma contagem (9.244.470 linhas) saiu em ~500ms.
#
# Auth/CSV isolados aqui (não instanciam ConsumeAPI) porque instanciar
# ConsumeAPI já dispara comportamento de __init__ (ver dispatch por
# `cliente`) -- usado só na hora de CORRIGIR (Fases 2/4), via
# backfill_dim_usuario_por_ids.
# =============================================================================

class ExtratorMetabase:
    def __init__(self, cliente: str, logger):
        self.cliente = cliente
        self.cfg = CLIENTE_CFG[cliente]
        self.logger = logger
        self.auth_id = self._autentica()

    def _autentica(self):
        resp = requests.post(
            API_AUTH,
            json={"username": self.cfg["api_user"], "password": self.cfg["api_pass"]},
        )
        resp.raise_for_status()
        auth_id = resp.json()["id"]
        self.logger.info(f"[AUTH] Autenticado no Metabase para {self.cliente}")
        return auth_id

    def _extrai_csv_nativo(self, sql_query, timeout=300, max_tentativas=3):
        """Mesmo padrão de ConsumeAPI.extrai_csv_nativo: envia SQL nativo
        (ClickHouse) direto pra API do Metabase, com retry simples em caso
        de falha transitória de conexão."""
        header = {
            "Content-Type": "application/json",
            "Cookie": f"metabase.DEVICE={self.auth_id}; metabase.SESSION={self.auth_id}; metabase.TIMEOUT=alive",
        }
        body = {
            "query": {
                "database": self.cfg["database"],
                "type": "native",
                "native": {"query": sql_query},
            }
        }
        ultimo_erro = None
        for tentativa in range(1, max_tentativas + 1):
            try:
                resp = requests.post(API_ROTA_CSV, headers=header, json=body, timeout=timeout)
                resp.raise_for_status()
                return resp.content
            except requests.exceptions.RequestException as e:
                ultimo_erro = e
                self.logger.warning(f"[SQL nativo] tentativa {tentativa}/{max_tentativas} falhou: {e}")
        raise ultimo_erro

    @staticmethod
    def _sql_ids(data_inicial: str, data_final: str, partner_id: int) -> str:
        """
        Filtra por CreationTime (campo real da tabela Client -- confirmado
        via DESCRIBE/SELECT *; o `registration_date` do card é um alias
        que não existe na tabela crua). Dedup por Id/_peerdb_version, mesmo
        padrão usado nas demais queries nativas do projeto.
        """
        return f"""
WITH client_dedup AS (
    SELECT Id, CreationTime
    FROM Client
    WHERE _peerdb_is_deleted = 0
      AND PartnerId = {partner_id}
      AND CreationTime >= '{data_inicial.replace("T", " ")}'
      AND CreationTime <  '{data_final.replace("T", " ")}'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY Id
        ORDER BY _peerdb_version DESC
    ) = 1
)
SELECT Id AS id
FROM client_dedup
"""

    def extrai_ids_periodo(self, data_inicial_str, data_final_str, partner_id):
        sql = self._sql_ids(data_inicial_str, data_final_str, partner_id)
        csv = self._extrai_csv_nativo(sql)
        return pd.read_csv(io_bytes(csv))

    def baixa_ids_paginado_cacheado(self, partner_id, data_inicial_dt, data_final_dt,
                                     cache_dir, profundidade=0):
        """
        Baixa recursivamente (dividindo a janela ao meio sempre que uma
        página >= LIMITE_LINHAS_METABASE) via SQL nativo contra Client,
        filtrando por CreationTime, e cacheia a coluna 'id' de cada folha
        em CSV.
        """
        os.makedirs(cache_dir, exist_ok=True)
        ini_str = data_inicial_dt.strftime("%Y-%m-%dT%H:%M:%S")
        fim_str = data_final_dt.strftime("%Y-%m-%dT%H:%M:%S")
        nome_arquivo = os.path.join(
            cache_dir, f"{ini_str.replace(':', '')}_{fim_str.replace(':', '')}.csv"
        )

        if os.path.exists(nome_arquivo):
            self.logger.info(f"[CACHE] Já existe, pulando: {nome_arquivo}")
            return

        self.logger.info(f"[DOWNLOAD] {ini_str} -> {fim_str} (profundidade={profundidade})")
        df = self.extrai_ids_periodo(ini_str, fim_str, partner_id)

        if len(df) >= LIMITE_LINHAS_METABASE and (data_final_dt - data_inicial_dt) > timedelta(minutes=1):
            self.logger.warning(
                f"[DOWNLOAD] {len(df)} linhas (teto do Metabase) em {ini_str}-{fim_str}. "
                f"Dividindo a janela ao meio."
            )
            meio = data_inicial_dt + (data_final_dt - data_inicial_dt) / 2
            self.baixa_ids_paginado_cacheado(partner_id, data_inicial_dt, meio, cache_dir, profundidade + 1)
            self.baixa_ids_paginado_cacheado(partner_id, meio, data_final_dt, cache_dir, profundidade + 1)
            return

        df[["id"]].to_csv(nome_arquivo, index=False)
        self.logger.info(f"[CACHE] {len(df)} ids salvos em {nome_arquivo}")


def io_bytes(csv_bytes):
    import io
    return io.BytesIO(csv_bytes)


def le_ids_cache_completo(cache_dir) -> np.ndarray:
    arquivos = [
        os.path.join(cache_dir, f) for f in os.listdir(cache_dir) if f.endswith(".csv")
    ]
    if not arquivos:
        raise FileNotFoundError(f"Cache vazio em {cache_dir}. Rode o download primeiro.")
    dfs = [pd.read_csv(a) for a in arquivos]
    ids = pd.concat(dfs, ignore_index=True)["id"].astype("int64").to_numpy()
    return np.unique(ids)


# =============================================================================
# ACESSO AO BANCO (DESTINO)
# =============================================================================

def busca_ids_existentes(cliente, logger) -> np.ndarray:
    logger.info("[DB] Lendo ids existentes em inplay.dim_usuario")
    ConnectionDB.conecta(DB, cliente)
    df = ConnectionDB.executa_script("SELECT id FROM inplay.dim_usuario", logger)
    ids = np.sort(df["id"].to_numpy(dtype="int64"))
    logger.info(f"[DB] {len(ids)} ids encontrados na base")
    return ids


def busca_ids_incompletos(cliente, logger) -> np.ndarray:
    logger.info("[DB] Procurando ids com colunas novas do ETL (proteção de dados) incompletas")
    condicao = " OR ".join(f"{col} IS NULL" for col in COLUNAS_NOVAS_ETL)
    sql = f"SELECT id FROM inplay.dim_usuario WHERE {condicao}"
    ConnectionDB.conecta(DB, cliente)
    df = ConnectionDB.executa_script(sql, logger)
    ids = np.sort(df["id"].to_numpy(dtype="int64"))
    logger.info(f"[DB] {len(ids)} ids com colunas novas incompletas")
    return ids


# =============================================================================
# CORREÇÃO -- delega para o método de produção já existente
# =============================================================================

def corrige_ids_em_lotes(cliente, ids_alvo, logger, rotulo):
    if len(ids_alvo) == 0:
        logger.info(f"[{rotulo}] Nenhum id para processar.")
        return

    lotes = [ids_alvo[i:i + TAMANHO_LOTE_CORRECAO] for i in range(0, len(ids_alvo), TAMANHO_LOTE_CORRECAO)]
    logger.info(f"[{rotulo}] {len(ids_alvo)} ids em {len(lotes)} lote(s) de até {TAMANHO_LOTE_CORRECAO}")

    for i, lote in enumerate(lotes, start=1):
        logger.info(f"[{rotulo}] Lote {i}/{len(lotes)} ({len(lote)} ids) via backfill_dim_usuario_por_ids")
        # Reaproveita a lógica de produção (consume_api.py): extrai
        # Usuarios/UsuariosTotalizador via card filtrado por id e
        # UsuariosTotalizadorBet via SQL nativo filtrado por id, sem
        # nenhum decrypt, e faz upsert (stg_usuario -> MERGE -> dim_usuario).
        ConsumeAPI(cliente=f"{cliente}_BACKFILL_USUARIO_POR_IDS",
                   ids_backfill=[int(x) for x in lote])
        logger.info(f"[{rotulo}] Lote {i}/{len(lotes)} concluído")


# =============================================================================
# ORQUESTRAÇÃO
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cliente", required=True, choices=["ZEROUM", "ENERGIABET"])
    parser.add_argument("--cache-dir", default=None,
                         help="Diretório de cache dos ids do Metabase (default: ./cache_ids_dim_usuario_<cliente>)")
    parser.add_argument("--forcar-download", action="store_true",
                         help="Ignora o cache existente e baixa os ids de novo do Metabase")
    parser.add_argument("--somente-diagnostico", action="store_true",
                         help="Só gera os CSVs de auditoria (Fases 1 e 3), não corrige nada no banco")
    parser.add_argument("--pular-fase3", action="store_true",
                         help="Pula o diagnóstico de incompletos (Fase 3) -- útil pra reprocessar só o "
                              "gap sem esperar a query de ~2min sobre dim_usuario. NA PRÁTICA, hoje esse "
                              "diagnóstico sempre vai sinalizar quase toda a base (colunas novas do "
                              "rollout ainda não populadas em massa -- ver LIMITE_ALERTA_INCOMPLETOS), "
                              "então a Fase 4 é pulada de qualquer forma até o backfill histórico rodar.")
    parser.add_argument(
        "--somente-fase-2-csv",
        type=str,
        metavar="CSV",
        help="Pula PASSO 0 / Fase 1 / Fase 3 inteiramente e carrega direto os ids de um CSV já "
             "gerado (coluna 'id') via backfill_dim_usuario_por_ids. Usado como workaround enquanto "
             "o PASSO 0 (paginação por CreationTime) não estiver confiável -- ver nota no topo do "
             "módulo sobre o platô em 1.048.575 linhas mesmo em janelas estreitas.",
    )
    args = parser.parse_args()

    logger = get_logger()
    cliente = args.cliente

    # -------------------------------------------------------------------
    # ATALHO - carrega só os ids de um CSV já pronto (bypassa PASSO 0/Fase 1/3)
    # -------------------------------------------------------------------
    if args.somente_fase_2_csv:
        logger.info("=" * 70)
        logger.info("[FASE 2 DIRETA] Executando somente a carga dos ids do CSV informado")
        logger.info(f"[FASE 2 DIRETA] CSV: {args.somente_fase_2_csv}")

        if not os.path.isfile(args.somente_fase_2_csv):
            raise FileNotFoundError(f"CSV não encontrado: {args.somente_fase_2_csv}")

        df_gap = pd.read_csv(args.somente_fase_2_csv)
        if "id" not in df_gap.columns:
            raise ValueError(f"O CSV precisa ter a coluna 'id'. Colunas encontradas: {list(df_gap.columns)}")

        ids_faltantes = (
            pd.to_numeric(df_gap["id"], errors="coerce").dropna().astype(np.int64).tolist()
        )
        logger.info(f"[FASE 2 DIRETA] {len(ids_faltantes)} ids carregados do CSV")

        corrige_ids_em_lotes(cliente, ids_faltantes, logger, rotulo="FASE 2 DIRETA")
        logger.info("[FASE 2 DIRETA] Processamento concluído.")
        return

    cfg = CLIENTE_CFG[cliente]
    cache_dir = args.cache_dir or f"./cache_ids_dim_usuario_{cliente.lower()}"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    if args.forcar_download and os.path.isdir(cache_dir):
        import shutil
        shutil.rmtree(cache_dir)

    extrator = ExtratorMetabase(cliente, logger)

    # -------------------------------------------------------------------
    # PASSO 0 - Download cacheado dos ids do Metabase (histórico completo)
    # -------------------------------------------------------------------
    logger.info("=" * 70)
    logger.info(f"[PASSO 0] Baixando/validando cache de ids ({cliente}), via SQL nativo "
                f"(Client, PartnerId={cfg['partner_id']})")
    extrator.baixa_ids_paginado_cacheado(
        partner_id=cfg["partner_id"],
        data_inicial_dt=datetime.strptime(DATA_INICIO_HISTORICO, "%Y-%m-%dT%H:%M:%S"),
        data_final_dt=datetime.now(),
        cache_dir=cache_dir,
    )
    ids_metabase = le_ids_cache_completo(cache_dir)
    logger.info(f"[PASSO 0] {len(ids_metabase)} ids distintos encontrados no Metabase ({cliente})")

    # -------------------------------------------------------------------
    # FASE 1 - Diagnóstico do gap (Metabase - nossa base)
    # -------------------------------------------------------------------
    logger.info("=" * 70)
    logger.info("[FASE 1] Diagnóstico do gap")
    ids_existentes = busca_ids_existentes(cliente, logger)
    ids_faltantes = np.setdiff1d(ids_metabase, ids_existentes, assume_unique=False)

    relatorio_gap = f"relatorio_gap_dim_usuario_{cliente}_{timestamp}.csv"
    pd.DataFrame({"id": ids_faltantes}).to_csv(relatorio_gap, index=False)
    logger.info(
        f"[FASE 1] Metabase={len(ids_metabase)} | Base={len(ids_existentes)} | "
        f"Faltantes={len(ids_faltantes)} -> relatório: {relatorio_gap}"
    )

    # -------------------------------------------------------------------
    # FASE 3 - Diagnóstico de incompletos (colunas novas do ETL)
    # -------------------------------------------------------------------
    ids_incompletos = np.array([], dtype="int64")
    if args.pular_fase3:
        logger.info("=" * 70)
        logger.info("[FASE 3] Pulada (--pular-fase3). Fase 4 também não vai rodar.")
    else:
        logger.info("=" * 70)
        logger.info("[FASE 3] Diagnóstico de registros com colunas novas incompletas")
        logger.info(f"[FASE 3] Colunas verificadas: {COLUNAS_NOVAS_ETL}")
        ids_incompletos = busca_ids_incompletos(cliente, logger)
        relatorio_incompletos = f"relatorio_incompletos_dim_usuario_{cliente}_{timestamp}.csv"
        pd.DataFrame({"id": ids_incompletos}).to_csv(relatorio_incompletos, index=False)
        logger.info(
            f"[FASE 3] {len(ids_incompletos)} ids incompletos -> relatório: {relatorio_incompletos}"
        )

        if len(ids_incompletos) > LIMITE_ALERTA_INCOMPLETOS:
            logger.warning(
                f"[FASE 3] {len(ids_incompletos)} ids incompletos ultrapassa "
                f"LIMITE_ALERTA_INCOMPLETOS={LIMITE_ALERTA_INCOMPLETOS}. Isso sugere que o "
                "rollout inicial das colunas novas ainda não rodou -- o caminho certo para "
                f"esse volume é ConsumeAPI(cliente='{cliente}_BACKFILL_HISTORICO_PROTECAO', "
                "data_inicial_backfill=<inicio>, data_final=<fim>) (backfill por período, já "
                "existente em consume_api.py), NÃO este script (que corrige por id, pensado "
                "para um volume residual pequeno). A Fase 4 abaixo NÃO vai rodar "
                "automaticamente neste caso -- use --somente-diagnostico e trate via backfill "
                "histórico."
            )

    if args.somente_diagnostico:
        logger.info("--somente-diagnostico ativo: nada será corrigido no banco. Encerrando.")
        return

    # -------------------------------------------------------------------
    # FASE 2 - Carga dos usuários faltantes (delega para backfill_dim_usuario_por_ids)
    # -------------------------------------------------------------------
    logger.info("=" * 70)
    logger.info("[FASE 2] Carga dos usuários faltantes")
    corrige_ids_em_lotes(cliente, ids_faltantes, logger, rotulo="FASE 2")

    # -------------------------------------------------------------------
    # FASE 4 - Correção dos incompletos (só roda se o volume for administrável)
    # -------------------------------------------------------------------
    logger.info("=" * 70)
    if len(ids_incompletos) == 0:
        logger.info("[FASE 4] Pulada -- nenhum id incompleto identificado (ou Fase 3 foi pulada).")
    elif len(ids_incompletos) > LIMITE_ALERTA_INCOMPLETOS:
        logger.info(
            "[FASE 4] Pulada -- volume acima de LIMITE_ALERTA_INCOMPLETOS "
            "(ver aviso da Fase 3). Use o backfill histórico por período."
        )
    else:
        logger.info("[FASE 4] Correção dos registros incompletos")
        corrige_ids_em_lotes(cliente, ids_incompletos, logger, rotulo="FASE 4")

    # -------------------------------------------------------------------
    # Conferência final (só do gap -- incompletos exigiriam reler todas as
    # 11 colunas novas, custo desnecessário para uma conferência rápida)
    # -------------------------------------------------------------------
    logger.info("=" * 70)
    ids_existentes_pos = busca_ids_existentes(cliente, logger)
    logger.info(
        f"[RESULTADO] Metabase={len(ids_metabase)} | Base antes={len(ids_existentes)} | "
        f"Base depois={len(ids_existentes_pos)} | Gap restante="
        f"{len(np.setdiff1d(ids_metabase, ids_existentes_pos))}"
    )


if __name__ == "__main__":
    main()