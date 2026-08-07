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
CLIENTES_ZEROUM = {'ZEROUM', 'ZRO_1_BET', 'ZEROUM_VALIDACAO', 'ZEROUM_SALDO', 'ZEROUM_VALIDA_APOSTA', 'ZEROUM_BACKFILL_USUARIO','ZEROUM_BACKFILL_USUARIO_POR_IDS','ZEROUM_BACKFILL_HISTORICO_PROTECAO' }

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

        Parâmetros
        ----------
        operation    : identificador da operação (ex: 'ETL_ZEROUM')
        status       : 'SUCCESS' ou 'FAILED'
        start_time   : datetime de início
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
                  f"{status} | {duration}s → {db_name_log}")

        except Exception as e:
            # Nunca levanta exceção — o logger não pode derrubar o ETL
            print(f"[DBLogger] Falha ao registrar log de '{operation}': {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass