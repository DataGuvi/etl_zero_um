import psycopg2
from psycopg2.extras import execute_batch, execute_values
import redshift_connector
import pandas as pd
from urllib.parse import quote_plus
from datetime import datetime, timedelta, date
from config import DB_HOST, DB_PORT, DB_NAME_ZEROUM, DB_NAME_ENERGIABET, DB_USER, DB_PASS, DB_SCHEMA 


class ConnectionDB:
    def conecta(banco: str, cliente: str):
            global cur
            global conn
            usuario = quote_plus(DB_USER)
            senha = quote_plus(DB_PASS)

            DB_NAME = DB_NAME_ZEROUM if cliente == 'ZEROUM' else DB_NAME_ENERGIABET

            if(banco == 'postgres'):
                url = f"postgresql://{usuario}:{senha}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
                print("url", url)
                conn = psycopg2.connect(url)
            elif(banco == 'redshift'):
                conn = psycopg2.connect(
                    host=DB_HOST,
                    dbname=DB_NAME,
                    user=usuario,
                    password=DB_PASS,
                    port=DB_PORT
                )
            conn.autocommit = False
            cur = conn.cursor()
            print('conexao com o bd realizada')

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
    
    def insere_dados_bulk(tabela: str, df: pd.DataFrame, logger):
        try:
            print("salva no banco")

            colunas = ', '.join(df.columns)
            #placeholders = ', '.join(['%s'] * len(df.columns))
            #sql = f"INSERT INTO {tabela} ({colunas}) VALUES ({placeholders})"
            sql = f"INSERT INTO {tabela} ({colunas}) VALUES %s"
            dados = [tuple(row) for row in df.values]

            logger.info(f"Inserindo {len(dados)} linhas na tabela {tabela}")
            print(f"Inserindo {len(dados)} linhas... as {datetime.now().strftime('%Y%m%d_%H%M%S')}")
            #execute_batch(cur, sql, dados, page_size=10000)
            execute_values(cur, sql, dados, page_size=10000)
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
            logger.error(f"Erro ao mergear os dados na tabela {tabela_destino}")
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