"""
db_logger.py
============
Padrão DataGuvi de log de execução de ETL persistido no banco.

Adaptado do padrão db_logger.py da empresa para o projeto etl_inplay:
- Usa psycopg2 direto (conexão própria, independente dos globals cur/conn
  de database.py — o logger não pode depender de uma conexão que pode já
  estar fechada quando um erro ocorre).
- Nunca levanta exceção: falha no logger nunca pode derrubar o ETL.
- Adiciona campo `cliente` e `duration_seconds` ao padrão original.

MAPEAMENTO DE BANCO POR CLIENTE
---------------------------------
ZEROUM     → DB_NAME_ZEROUM   (dlzeroum)
ZRO_1_BET  → DB_NAME_ZEROUM   (dlzeroum)
ENERGIABET → DB_NAME_ENERGIABET (dlenergiabet)

O cliente é passado no __init__ e resolve o banco automaticamente.
A tabela inplay.etl_execution_logs é criada em cada banco se não existir.
"""

import psycopg2
from datetime import datetime
from urllib.parse import quote_plus
from config import (
    DB_HOST,
    DB_PORT,
    DB_USER,
    DB_PASS,
    DB_NAME_ZEROUM,
    DB_NAME_ENERGIABET,
)


PROJECT_NAME = 'etl_inplay'
TABLE_NAME   = 'inplay.etl_execution_logs'

# Clientes que gravam no banco ZEROUM (dlzeroum)
CLIENTES_ZEROUM = {
    'ZEROUM', 'ZRO_1_BET', 'ZEROUM_VALIDACAO', 'ZEROUM_SALDO', 'ZEROUM_VALIDA_APOSTA',
    'ZEROUM_BACKFILL_USUARIO', 'ZEROUM_BACKFILL_USUARIO_POR_IDS',
    'ZEROUM_BACKFILL_HISTORICO_PROTECAO',
    'ZEROUM_BONUS', 'ZEROUM_BACKFILL_BONUS_AWARDING_NULO',
    'ZEROUM_PIX',
    # Adicionados nesta conversa (conferência da carga pós-parada de 8+ dias):
    'ZEROUM_VALIDACAO_PERIODO', 'ZEROUM_REPROCESSA_DIAS_PONTUAIS',
    'ZEROUM_REPROCESSA_FACT_USER_DAILY',
}

DDL_CREATE_TABLE = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id               BIGINT IDENTITY(1,1),
        project_name     VARCHAR(255)   ENCODE ZSTD,
        cliente          VARCHAR(100)   ENCODE ZSTD,
        operation        VARCHAR(255)   ENCODE ZSTD,
        status           VARCHAR(50)    ENCODE ZSTD,
        error_reason     VARCHAR(65535) ENCODE ZSTD,
        start_time       TIMESTAMP      ENCODE AZ64,
        end_time         TIMESTAMP      ENCODE AZ64,
        duration_seconds NUMERIC(12,2)  ENCODE AZ64
    )
    DISTSTYLE ALL
    SORTKEY (start_time)
"""
# Sem ponto-e-vírgula: cur.execute() do psycopg2 não precisa dele e
# em algumas combinações psycopg2 + Redshift o ';' causa
# "syntax error at end of input".

DML_INSERT = f"""
    INSERT INTO {TABLE_NAME}
        (project_name, cliente, operation, status,
         error_reason, start_time, end_time, duration_seconds)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


class DBLogger:
    """
    Persiste o resultado de cada execução de operação ETL no banco de dados.

    Parâmetros
    ----------
    cliente      : cliente ETL ('ZEROUM', 'ENERGIABET', 'ZRO_1_BET', etc.)
                   Determina em qual banco o log é gravado.
    project_name : identificador do projeto (padrão: 'etl_inplay')

    Uso:
        # No __init__ de ConsumeAPI, passar o cliente recebido:
        self.db_logger = DBLogger(cliente=cliente)

        start = datetime.now()
        try:
            ... lógica do ETL ...
            self.db_logger.log_operation(
                operation='ETL_ZEROUM',
                status='SUCCESS',
                start_time=start,
                end_time=datetime.now(),
                cliente='ZEROUM'
            )
        except Exception as e:
            self.db_logger.log_operation(
                operation='ETL_ZEROUM',
                status='FAILED',
                start_time=start,
                end_time=datetime.now(),
                error_reason=str(e),
                cliente='ZEROUM'
            )
            raise
    """

    def __init__(self, cliente: str = 'ZEROUM', project_name: str = PROJECT_NAME):
        self.project_name = project_name
        self.cliente      = cliente
        self.db_name      = self._resolver_banco(cliente)
        self._create_table_if_not_exists()

    # ------------------------------------------------------------------
    # RESOLUÇÃO DO BANCO POR CLIENTE
    # ------------------------------------------------------------------

    @staticmethod
    def _resolver_banco(cliente: str) -> str:
        """
        Retorna o nome do banco (dbname) correspondente ao cliente.

        ZEROUM e ZRO_1_BET → DB_NAME_ZEROUM   (dlzeroum)
        ENERGIABET          → DB_NAME_ENERGIABET (dlenergiabet)
        """
        if cliente in CLIENTES_ZEROUM:
            return DB_NAME_ZEROUM
        else:
            return DB_NAME_ENERGIABET

    # ------------------------------------------------------------------
    # CONEXÃO PRÓPRIA — não usa os globals de database.py
    # ------------------------------------------------------------------

    def _conectar(self):
        return psycopg2.connect(
            host=DB_HOST,
            dbname=self.db_name,
            user=quote_plus(DB_USER),
            password=DB_PASS,
            port=DB_PORT,
            connect_timeout=30,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
        )

    # ------------------------------------------------------------------
    # CRIAÇÃO DA TABELA (uma vez, no __init__, no banco correto)
    # ------------------------------------------------------------------

    def _create_table_if_not_exists(self):
        try:
            conn = self._conectar()
            conn.autocommit = False
            cur  = conn.cursor()
            cur.execute(DDL_CREATE_TABLE)
            conn.commit()
            cur.close()
            print(f"[DBLogger] Tabela garantida em '{self.db_name}'.")
        except Exception as e:
            print(f"[DBLogger] Aviso: não foi possível garantir a tabela de log "
                  f"em '{self.db_name}': {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # LOG DE OPERAÇÃO
    # ------------------------------------------------------------------

    def log_operation(
        self,
        operation:    str,
        status:       str,
        start_time:   datetime,
        end_time:     datetime,
        error_reason: str = None,
        cliente:      str = None,
    ):
        """
        Registra o resultado de uma operação ETL.

        ACHADO DESTA CONVERSA: se existir uma marca RUNNING (gravada por
        log_start(), com o MESMO cliente+operation+start_time — start_time
        funciona como correlação porque tem precisão de microssegundo,
        então é praticamente único por execução) esta chamada ATUALIZA
        essa linha em vez de inserir uma nova — vira 1 linha por execução
        (RUNNING → SUCCESS/FAILED), não 2 linhas soltas e sem end_time na
        marca de RUNNING. Se não achar nenhuma linha RUNNING correspondente
        (uso normal, sem lock — a grande maioria das chamadas no projeto),
        cai no comportamento de sempre: insere uma linha nova.

        Parâmetros
        ----------
        operation    : identificador da operação (ex: 'ETL_ZEROUM')
        status       : 'SUCCESS' ou 'FAILED'
        start_time   : datetime de início — se log_start() foi chamado
                       antes, PRECISA ser o mesmo objeto/valor passado lá,
                       senão a correlação não bate e uma linha nova é
                       inserida (a RUNNING fica órfã, como antes).
        end_time     : datetime de fim
        error_reason : mensagem de erro (None em caso de sucesso)
        cliente      : cliente ETL; se None usa o informado no __init__
        """
        try:
            cliente_log  = cliente or self.cliente
            db_name_log  = self._resolver_banco(cliente_log)
            duration     = round((end_time - start_time).total_seconds(), 2)

            if error_reason:
                error_reason = str(error_reason)[:60000]

            # conecta ao banco correto para este log
            conn = psycopg2.connect(
                host=DB_HOST,
                dbname=db_name_log,
                user=quote_plus(DB_USER),
                password=DB_PASS,
                port=DB_PORT
            )
            conn.autocommit = False
            cur  = conn.cursor()

            cur.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET status = %s, error_reason = %s, end_time = %s, duration_seconds = %s
                WHERE cliente = %s AND operation = %s AND status = 'RUNNING' AND start_time = %s
                """,
                (status, error_reason, end_time, duration, cliente_log, operation, start_time)
            )
            atualizou = cur.rowcount > 0

            if not atualizou:
                cur.execute(DML_INSERT, (
                    self.project_name,
                    cliente_log,
                    operation,
                    status,
                    error_reason,
                    start_time,
                    end_time,
                    duration,
                ))

            conn.commit()
            cur.close()

            print(f"[DBLogger] {operation} | {cliente_log} | "
                  f"{status} | {duration}s -> {db_name_log}"
                  f"{' (linha RUNNING atualizada)' if atualizou else ''}")

        except Exception as e:
            # Nunca levanta exceção — o logger não pode derrubar o ETL
            print(f"[DBLogger] Falha ao registrar log de '{operation}': {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # LOCK BEST-EFFORT CONTRA EXECUÇÃO CONCORRENTE
    # ------------------------------------------------------------------
    # ACHADO DA CONVERSA (conferência da carga ZEROUM, parada de 8+ dias):
    # os erros "Found multiple matches to update the same tuple" na
    # recuperação bateram com horários de execução praticamente idênticos
    # de mais de uma instância do mesmo pipeline (ex.: ETL_ZEROUM) — sinal
    # de que o cron disparou uma nova rodada antes da anterior (mais lenta
    # por causa do catch-up) terminar, e as duas disputaram a mesma tabela
    # de staging.
    #
    # LIMITAÇÃO CONHECIDA: isto NÃO é um lock atômico de verdade. O
    # Redshift não oferece advisory lock, e PRIMARY KEY/UNIQUE são só
    # dicas para o otimizador, não restrições aplicadas — então não dá
    # pra usar os mecanismos usuais de lock de banco. O que existe aqui é
    # um check-then-insert (esta_rodando() consulta, depois log_start()
    # grava): existe uma janela pequena de corrida entre os dois. Isso
    # reduz drasticamente a chance de colisão (de "quase garantida",
    # quando 2 crons disparam com o pipeline anterior ainda rodando, para
    # "só nesse instante específico de corrida"), mas não elimina 100%.
    # Suficiente para o cenário real do incidente (rodadas se sobrepondo
    # por minutos), não para uma race condition de milissegundos.

    def esta_rodando(self, operation: str, cliente: str = None, timeout_minutos: int = 30) -> bool:
        """
        Verifica se já existe uma execução da mesma operação+cliente
        marcada como RUNNING e ainda "fresca" (iniciada há menos de
        timeout_minutos). Usar como lock best-effort junto com
        log_start(), no começo do método que se quer proteger:

            if self.db_logger.esta_rodando('ETL_ZEROUM', 'ZEROUM'):
                self.logger.warning("Já existe uma execução em andamento — abortando.")
                return
            self.db_logger.log_start('ETL_ZEROUM', 'ZEROUM')

        timeout_minutos: depois desse tempo, uma marca RUNNING para de
        travar novas execuções — protege contra um processo que morreu
        sem nunca gravar SUCCESS/FAILED (ex.: máquina reiniciada no meio
        do ETL) deixar o lock preso pra sempre. Ajustar conforme a
        duração normal do pipeline (ETL_ZEROUM historicamente leva
        alguns minutos; 30 min dá folga confortável sem travar por muito
        tempo em caso de processo morto).
        """
        try:
            cliente_log = cliente or self.cliente
            db_name_log = self._resolver_banco(cliente_log)
            conn = psycopg2.connect(
                host=DB_HOST, dbname=db_name_log, user=quote_plus(DB_USER),
                password=DB_PASS, port=DB_PORT
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT COUNT(*) FROM {TABLE_NAME}
                WHERE cliente = %s AND operation = %s AND status = 'RUNNING'
                  AND start_time > (GETDATE() - INTERVAL '{int(timeout_minutos)} minutes')
                """,
                (cliente_log, operation)
            )
            (qtd,) = cur.fetchone()
            cur.close()
            return qtd > 0
        except Exception as e:
            # Se a checagem falhar (ex.: banco fora do ar), não bloqueia o
            # ETL por causa do lock — melhor rodar com o risco residual de
            # concorrência do que travar a operação inteira por uma falha
            # no mecanismo de proteção.
            print(f"[DBLogger] Falha ao checar lock de '{operation}': {e} — assumindo NÃO travado.")
            return False
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def log_start(self, operation: str, cliente: str = None, start_time: datetime = None):
        """
        Marca o início de uma operação como RUNNING (end_time/duration =
        NULL). Usar em conjunto com esta_rodando() — ver docstring acima.
        Nunca levanta exceção (mesmo princípio de log_operation): falha no
        lock não pode derrubar o ETL.

        start_time: IMPORTANTE — passe o MESMO objeto datetime que será
        usado depois na chamada de log_operation() pro mesmo run (ex.: a
        variável `start_time = datetime.now()` já capturada no começo do
        método). Se não passar, gera um novo aqui — mas aí ele NÃO vai
        bater com o start_time que o chamador eventualmente usar em
        log_operation(), e a correlação RUNNING→SUCCESS/FAILED se perde
        (log_operation cai no fallback de INSERT, deixando a linha
        RUNNING órfã, do jeito que era antes desta correção).

        Retorna o start_time efetivamente gravado, para o chamador guardar
        e reusar exatamente na chamada de log_operation() de encerramento.
        """
        if start_time is None:
            start_time = datetime.now()
        try:
            cliente_log = cliente or self.cliente
            db_name_log = self._resolver_banco(cliente_log)
            conn = psycopg2.connect(
                host=DB_HOST, dbname=db_name_log, user=quote_plus(DB_USER),
                password=DB_PASS, port=DB_PORT
            )
            conn.autocommit = False
            cur = conn.cursor()
            cur.execute(DML_INSERT, (
                self.project_name,
                cliente_log,
                operation,
                'RUNNING',
                None,
                start_time,
                None,
                None,
            ))
            conn.commit()
            cur.close()
            print(f"[DBLogger] {operation} | {cliente_log} | RUNNING (lock) -> {db_name_log}")
        except Exception as e:
            print(f"[DBLogger] Falha ao registrar início (lock) de '{operation}': {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return start_time