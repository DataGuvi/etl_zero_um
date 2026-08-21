import psycopg2
from psycopg2.extras import execute_batch, execute_values
import redshift_connector
import pandas as pd
from urllib.parse import quote_plus
from datetime import datetime, timedelta, date
from config import (
    DB_HOST,
    DB_PORT,
    DB_NAME_ZEROUM,
    DB_NAME_ENERGIABET,
    DB_USER,
    DB_PASS,
    DB_SCHEMA,
    DB_NAME_ZRO_1_BET_ADTK,
    DB_HOST_ZRO_1_BET_ADTK,
    DB_PORT_ZRO_1_BET_ADTK,
    DB_USER_ZRO_1_BET_ADTK,
    DB_PASS_ZRO_1_BET_ADTK
)
import numpy as np


class ConnectionDB:
    def conecta(banco: str, cliente: str):
            global cur
            global conn
            

            DB_NAME = DB_NAME_ZEROUM if cliente == 'ZEROUM' else DB_NAME_ENERGIABET

            host = DB_HOST
            port = DB_PORT
            user = DB_USER
            password = DB_PASS

                     

             # ZRO_1_BET (novo PostgreSQL - informação de afiliados)
            if cliente == "ZRO_1_BET":
                DB_NAME = DB_NAME_ZRO_1_BET_ADTK
                host = DB_HOST_ZRO_1_BET_ADTK
                port = DB_PORT_ZRO_1_BET_ADTK
                user = DB_USER_ZRO_1_BET_ADTK
                password = DB_PASS_ZRO_1_BET_ADTK

            usuario = quote_plus(user)
            senha = quote_plus(password)


            if(banco == 'postgres'):
                #url = f"postgresql://{usuario}:{senha}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
                url = f"postgresql://{usuario}:{senha}@{host}:{port}/{DB_NAME}"
                print("url", url)
                print("Cliente:", cliente)
                print("Host:", host)
                print("Port:", port)
                print("Database:", DB_NAME)
                print("User:", user)
                conn = psycopg2.connect(url)
            elif(banco == 'redshift'):
                conn = psycopg2.connect(
                    host=DB_HOST,
                    dbname=DB_NAME,
                    user=usuario,
                    password=DB_PASS,
                    port=DB_PORT,
                    connect_timeout=30,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=3,
                )
            conn.autocommit = False
            cur = conn.cursor()
            print('conexao com o bd realizada')
            return conn

    def insere_dados(tabela: str, id: str, dados: dict) -> int:
        print("salva no banco")

        colunas = ', '.join(dados.keys())
        placeholders = ', '.join(['%s'] * len(dados))
        valores = list(dados.values())

        sql = f"INSERT INTO {tabela} ({colunas}) VALUES ({placeholders}) RETURNING {id}"
        cur.execute(sql, valores)
        id = cur.fetchone()[0]
        conn.commit()

        return id
    
    def insere_dados_bulk(tabela: str, df: pd.DataFrame, logger, normalize_numpy=False,
                           log_progresso=False, page_size=10000):
        try:
            print("salva no banco")
 
            colunas = ', '.join(df.columns)
            sql = f"INSERT INTO {tabela} ({colunas}) VALUES %s"
            if normalize_numpy:
                dados = [
                    tuple(x.item() if hasattr(x, "item") else x for x in row)
                    for row in df.to_numpy()
                ]
            else:
                dados = [tuple(row) for row in df.values]
 
            logger.info(f"Inserindo {len(dados)} linhas na tabela {tabela}")
            print(f"Inserindo {len(dados)} linhas... as {datetime.now().strftime('%Y%m%d_%H%M%S')}")
 
            if log_progresso and len(dados) > page_size:
                # OTIMIZAÇÃO: mesma técnica de sempre (execute_values em lotes),
                # só que logando o progresso a cada 10 lotes -- não muda a
                # performance, só dá visibilidade de que está avançando (em
                # vez de parecer travado durante inserts de milhões de linhas).
                # Fica desligado por padrão para não gerar log excessivo em
                # cargas pequenas do dia a dia (Saldo Diário, etc).
                total_paginas = -(-len(dados) // page_size)
                inicio_insert = time.monotonic()
                for i in range(0, len(dados), page_size):
                    lote = dados[i:i + page_size]
                    execute_values(cur, sql, lote, page_size=page_size)
                    pagina_atual = i // page_size + 1
                    if pagina_atual % 10 == 0 or pagina_atual == total_paginas:
                        decorrido = time.monotonic() - inicio_insert
                        logger.info(
                            f"Insert em {tabela}: lote {pagina_atual}/{total_paginas} "
                            f"(~{min(pagina_atual * page_size, len(dados)):,} linhas) — "
                            f"{decorrido:.0f}s decorridos"
                        )
            else:
                execute_values(cur, sql, dados, page_size=page_size)
 
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.info(f"Erro ao inserir os dados na tabela {tabela}: {e}")
            print(f"Erro ao inserir os dados: {e}")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
 

    def mergeia_dados(tabela_origem: str, tabela_destino: str, df: pd.DataFrame, campos_chave: list, logger):
        try:
            print("realiza o merge dos dados")

            # inclusão para visualiar os erros que eventualmente temos por dados duplicados
            duplicados = df[df.duplicated(subset=campos_chave, keep=False)]
            if not duplicados.empty:
                logger.warning(
                    f"[MERGE {tabela_destino}] {len(duplicados)} linhas duplicadas "
                    f"na chave {campos_chave} (origem: {tabela_origem})"
                )
                print(f"[MERGE {tabela_destino}] Linhas duplicadas detectadas na chave {campos_chave}:")
                print(duplicados.sort_values(campos_chave).to_string())

            chave = ''

            for campo in campos_chave:
                if (chave == ''):
                    chave = f"ON {tabela_destino}.{campo} = o.{campo}"
                else:
                    chave = chave + f" AND {tabela_destino}.{campo} = o.{campo}"
            
            insert_cols = ", ".join(df.columns)
            insert_vals = ", ".join([f"o.{c}" for c in df.columns])

            update_cols = ", ".join([f"{c} = o.{c}" for c in df.columns if c != chave])

            sql = f"""MERGE INTO {tabela_destino}
                    USING {tabela_origem} o
                    {chave}
                    -- Condição para atualizar os registros já existentes
                    WHEN MATCHED THEN
                    UPDATE SET {update_cols}
                    -- Condição para inserir registros novos
                    WHEN NOT MATCHED THEN
                    INSERT ({insert_cols})
                    VALUES ({insert_vals})"""
                
            logger.info(f"Realizando o merge na tabela {tabela_destino}")
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Erro ao mergear os dados na tabela {tabela_destino}: {e}")
            if not duplicados.empty:
                logger.error(
                    f"[MERGE {tabela_destino}] Provável causa: {len(duplicados)} "
                    f"chaves duplicadas em {campos_chave}. Amostra: "
                    f"{duplicados[campos_chave].drop_duplicates().head(5).to_dict('records')}"
                )
                print(f"Erro ao mergear os dados: {e}")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    def altera_dados(id: int, tabela: str, dados: dict):
        print("altera dados no banco")

        set_clause = ', '.join([f"{col} = %s" for col in dados])
        valores = list(dados.values())
        valores.append(id)

        sql = f"UPDATE {tabela} SET {set_clause} WHERE id = %s"
        cur.execute(sql, valores)
        conn.commit()

    def recupera_dados(tabela: str, campos_select: str, clausula_where: str) -> str:
        print("executa select")
        sql = f"SELECT {campos_select} FROM {tabela} {clausula_where}"
        cur.execute(sql)
        id = cur.fetchall()
        return id
    	
    def deleta_dados(tabela: str, clausula_where: str, logger):
        try:        
            # Usar parâmetros para evitar SQL injection
            query = f"""
                DELETE FROM {tabela}
                {clausula_where}
            """
        
            cur.execute(query)
            linhas_deletadas = cur.rowcount
        
            conn.commit()
            logger.info(f"{linhas_deletadas} registros deletados da tabela {tabela} com sucesso.")
            print(f"{linhas_deletadas} registros deletados da tabela {tabela} com sucesso.")
        
            return linhas_deletadas
        
        except Exception as e:
            if conn:
                conn.rollback()
            logger.info(f"Erro ao deletar os dados da tabela {tabela}: {e}")
            print(f"Erro ao deletar: {e}")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def executa_script(script: str, logger) -> pd.DataFrame:
        try:
            logger.info("Executando script passado.")
            print("Executando script passado.")

            df = pd.read_sql(script, conn)

            return df
        
        except Exception as e:
            if conn:
                conn.rollback()
            logger.info(f"Erro ao executar o script: {e}")
            print(f"Erro ao executar o script: {e}")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    def executa_dml(script: str, logger) -> int:
        """
        Executa um statement DML (INSERT, UPDATE, DELETE, MERGE, TRUNCATE).
        Diferente de executa_script (que usa pd.read_sql e é para SELECT),
        este método usa cur.execute diretamente e faz commit.
 
        Retorna o número de linhas afetadas (cur.rowcount).
        """
        try:
            logger.info("Executando DML.")
            print("Executando DML.")
 
            cur.execute(script)
            linhas = cur.rowcount
            conn.commit()
 
            logger.info(f"DML executado com sucesso. Linhas afetadas: {linhas}")
            print(f"DML executado. Linhas afetadas: {linhas}")
 
            return linhas
 
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Erro ao executar DML: {e}")
            print(f"Erro ao executar DML: {e}")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()