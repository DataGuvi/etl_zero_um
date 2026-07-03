# ETL DataGuvi — Pipeline de Estruturação de Dados

## Visão Geral

Pipeline de **ETL (Extract, Transform, Load)** responsável por extrair dados de fontes externas (API Metabase e bancos PostgreSQL de parceiros), tratá-los e carregá-los no Data Warehouse Redshift, seguindo o padrão **stage → merge/fact**.

O pipeline atende três frentes de clientes:

- **ZEROUM** — extração via API Metabase (Clickhouse), carga de fatos de usuário, depósitos, saques, apostas, bônus, jogos de cassino e tabelas físicas de agregação para o BI.
- **ENERGIABET** — mesmo fluxo do ZEROUM, base separada.
- **ZRO_1_BET** — extração direta de PostgreSQL externo (`zro1_bet_adtk`), validação com log de rejeitados e carga incremental/histórica de vendas.

Em caso de qualquer falha, o pipeline envia alerta por e-mail e grava o resultado da execução na tabela `inplay.etl_execution_logs` no banco correspondente.


## Pré-requisitos

- Python 3.10+
- Acesso de rede ao Redshift (porta 5439) e ao PostgreSQL ZRO_1_BET (porta 5432 — requer VPN)
- Acesso à API Metabase (`inplaysoft.metabaseapp.com`)
- Credenciais fornecidas via `.env`


## Configuração do Ambiente

```bash
# Criar e ativar ambiente virtual
python -m venv venv
source venv/bin/activate      # Linux/Mac
venv\Scripts\activate         # Windows

# Instalar dependências
pip install -r requirements.txt
```

### Variáveis de Ambiente

Criar `.env` na raiz com as chaves abaixo. O arquivo não deve ser versionado (já está no `.gitignore`). Manter um `.env.example` sem valores reais para onboarding.

| Variável | Descrição |
|---|---|
| `DB` | Tipo de banco destino: `redshift` ou `postgres` |
| `DB_HOST`, `DB_PORT` | Host e porta do Data Warehouse |
| `DB_NAME_ZEROUM` | Database DW do cliente ZeroUm e ZRO_1_BET (ex: `dlzeroum`) |
| `DB_NAME_ENERGIABET` | Database DW do cliente Energiabet (ex: `dlenergiabet`) |
| `DB_USER`, `DB_PASS` | Credenciais do DW |
| `DB_SCHEMA` | Schema padrão (`inplay`) |
| `API_AUTH` | Endpoint de autenticação Metabase |
| `API_USER_ZEROUM`, `API_PASS_ZEROUM` | Credenciais API — ZeroUm |
| `API_USER_ENERGIABET`, `API_PASS_ENERGIABET` | Credenciais API — Energiabet |
| `API_ROTA_CSV` | Endpoint de extração CSV do Metabase |
| `EMAIL_SENDER` | Remetente dos alertas de erro |
| `SENDER_PASSWORD` | Senha/app password do remetente |
| `EMAIL_RECEIVER` | Destinatário dos alertas |
| `AES_KEY_ZEROUM`, `AES_IV_ZEROUM` | Chave/IV AES — descriptografia ZeroUm |
| `AES_KEY_ENERGIABET`, `AES_IV_ENERGIABET` | Chave/IV AES — descriptografia Energiabet |
| `DB_HOST_ZRO_1_BET_ADTK` | Host do PostgreSQL externo ZRO_1_BET |
| `DB_PORT_ZRO_1_BET_ADTK` | Porta do PostgreSQL externo ZRO_1_BET |
| `DB_NAME_ZRO_1_BET_ADTK` | Database do PostgreSQL externo ZRO_1_BET |
| `DB_USER_ZRO_1_BET_ADTK` | Usuário do PostgreSQL externo ZRO_1_BET |
| `DB_PASS_ZRO_1_BET_ADTK` | Senha do PostgreSQL externo ZRO_1_BET |


## Estrutura do Projeto

```
.
├── main.py                      # Ponto de entrada CLI
├── consume_api.py               # Orquestração do ETL (extração, transformação, carga)
├── database.py                  # Camada de acesso a banco (ConnectionDB)
├── config.py                    # Carregamento das variáveis de ambiente
├── enums.py                     # Mapeamento de IDs Metabase (databases, tabelas, cards)
├── util.py                      # Descriptografia AES de campos sensíveis
├── send_email.py                # Envio de e-mail de alerta (padrão DataGuvi)
├── db_logger.py                 # Log de execuções ETL no banco (padrão DataGuvi)
├── agregacao_dim_usuario.py     # Tabelas físicas de agregação para BI (ZEROUM)
├── requirements.txt
├── .env                         # Credenciais (não versionado)
├── .env.example                 # Modelo sem valores reais
└── .gitignore
```

### Descrição dos módulos novos

**`db_logger.py`** — persiste cada execução do ETL na tabela `inplay.etl_execution_logs` (banco determinado pelo cliente: ZEROUM/ZRO_1_BET → `dlzeroum`; ENERGIABET → `dlenergiabet`). Registra operação, status (`SUCCESS`/`FAILED`), horário de início/fim, duração e motivo do erro. Nunca lança exceção — uma falha no logger não interrompe o ETL.

**`agregacao_dim_usuario.py`** — alimenta as tabelas físicas que substituem o processamento pesado das views `vw_dim_usuario` e `vw_fato_usuarios_diario` no Redshift. Executado ao final de cada carga ZEROUM, escopado apenas aos usuários impactados na janela incremental. Não se aplica à Energiabet.


## Banco de Dados

### Grupos de conexão

| Grupo | Banco | Clientes |
|---|---|---|
| Data Warehouse destino | Redshift (`dlzeroum`) | ZEROUM, ZRO_1_BET |
| Data Warehouse destino | Redshift (`dlenergiabet`) | ENERGIABET |
| Origem ZRO_1_BET | PostgreSQL externo (`zro1_bet_adtk`) | ZRO_1_BET |
| Origem ZEROUM/ENERGIABET | API Metabase (Clickhouse) | ZEROUM, ENERGIABET |

### Tabelas no DW (schema `inplay`)

**Produção (alimentadas pelo ETL):**

| Tabela | Tipo | Cliente |
|---|---|---|
| `fact_user_daily` | Fato diário de usuário | ZEROUM, ENERGIABET |
| `fact_user_daily_sport` | Fato diário esportes | ZEROUM, ENERGIABET |
| `fact_deposits_withdraws_summarized` | Fato depósitos/saques | ZEROUM, ENERGIABET |
| `fact_casino_games_hourly` | Fato jogos por hora | ZEROUM, ENERGIABET |
| `dim_usuario` | Dimensão usuário | ZEROUM, ENERGIABET |
| `dim_game` | Dimensão jogo | ZEROUM, ENERGIABET |
| `fact_vendas_data` | Fato vendas | ZRO_1_BET |
| `fact_user_atividade_diaria` | Fato atividade diária física | ZEROUM |
| `agg_usuario_metricas` | Métricas acumuladas por usuário | ZEROUM |
| `agg_usuario_recorrencia_30d` | Recorrência 30 dias | ZEROUM |
| `agg_usuario_reativacao` | Estado de reativação | ZEROUM |
| `etl_execution_logs` | Log de execuções ETL | todos |

**Stage (temporárias, limpas a cada carga):**
`stg_fact_user_daily`, `stg_fact_user_daily_sport`, `stg_game`,
`stg_fact_deposits_withdraws_summarized`, `stg_fact_casino_games_hourly`,
`stg_usuario`, `stg_vendas_data`, `stg_usuarios_impactados`,
`stg_fact_user_atividade_diaria`, `stg_agg_usuario_metricas`,
`stg_agg_usuario_reativacao`

**Log/auditoria:**
`log_vendas_data_rejeitados`, `stg_vendas_data_rejeitados_tmp`,
`verificacao_zero_um`, `verificacao_energia_bet`

### Scripts de migração (rodar uma vez no banco)

| Script | Onde rodar | Finalidade |
|---|---|---|
| `migration_tabelas_fisicas.sql` | `dlzeroum` | Cria tabelas de agregação (ZEROUM) |
| `migration_etl_execution_logs.sql` | `dlzeroum` **e** `dlenergiabet` | Cria tabela de log de execuções |

> Os scripts de migração não fazem parte do deploy — são executados diretamente no banco antes da ativação.


## Como Executar o Pipeline

```bash
# Carga incremental ZeroUm
python main.py --cliente ZEROUM

# Carga incremental Energiabet
python main.py --cliente ENERGIABET

# Carga incremental ZRO_1_BET (requer VPN ativa)
python main.py --cliente ZRO_1_BET

# Validação de dados (compara origem x destino)
python main.py --cliente ZEROUM_VALIDACAO
python main.py --cliente ENERGIABET_VALIDACAO

# Carga manual de CSV auxiliar
python main.py --cliente sobe_dados
```

> Para carga histórica do ZRO_1_BET, alterar o parâmetro `modo` para `"historico"` na chamada de `principal_zro_1_bet()` em `consume_api.py`.

Logs de execução gravados em `etl.log` (rotação automática: 5 arquivos × 10 MB). Em caso de falha, e-mail enviado automaticamente e resultado gravado em `inplay.etl_execution_logs`.


## Alertas de Erro e Log de Execução

### E-mail (`send_email.py`)

Implementa o padrão DataGuvi. Lê credenciais diretamente do `.env` (`EMAIL_SENDER`, `SENDER_PASSWORD`, `EMAIL_RECEIVER`). Disparado em qualquer exceção nos métodos principais (`principal_zeroum`, `principal_energiabet`, `principal_zro_1_bet`, `valida_dados`) e nos métodos de comunicação com a API (`conection`, `extrai_csv`, `extrai_dados_card`).

### Log no banco (`db_logger.py`)

Implementa o padrão DataGuvi. Grava na tabela `inplay.etl_execution_logs` do banco correspondente ao cliente. Cobertura:

| Operação | Banco do log |
|---|---|
| `ETL_ZEROUM` | `dlzeroum` |
| `ETL_ENERGIABET` | `dlenergiabet` |
| `ETL_ZRO_1_BET` | `dlzeroum` |
| `VALIDACAO_ZEROUM` | `dlzeroum` |
| `VALIDACAO_ENERGIABET` | `dlenergiabet` |

Consultas úteis de monitoramento estão documentadas em `migration_etl_execution_logs.sql`.


## Fluxo do ETL

### ZEROUM e ENERGIABET (legado + agregação)

```
Autenticação Metabase
        ↓
Determina janela incremental (último updated_at em fact_user_daily)
        ↓
Limpa stages
        ↓
Extração paralela de cards Metabase
(stage, depósito, saque, bônus, apostas, jogos, usuários)
        ↓
Tratamento (NaN→None, descriptografia AES)
        ↓
Para cada subconjunto: insere_dados_bulk (stage) → mergeia_dados (fact/dim)
        ↓
[ZEROUM] Atualiza tabelas físicas de agregação
(fact_user_atividade_diaria, agg_usuario_metricas,
 agg_usuario_recorrencia_30d, agg_usuario_reativacao)
        ↓
Grava SUCCESS em etl_execution_logs
```

### ZRO_1_BET

```
Conexão PostgreSQL externo (requer VPN)
        ↓
Extração incremental (últimos 7 dias) ou histórica
        ↓
Validação: registros com data_criacao nula → log_vendas_data_rejeitados
        ↓
TRUNCATE stg_vendas_data → insere_dados_bulk
        ↓
UPDATE fact_vendas_data (registros existentes)
INSERT fact_vendas_data (registros novos)
        ↓
Grava SUCCESS em etl_execution_logs
```

### Novas integrações

Ao adicionar um novo cliente, seguir o padrão ZRO_1_BET: métodos separados por etapa, try/except com `self.db_logger.log_operation()` + `send_email()` no except, re-raise ao final. O `get_log_history()` já está disponível na classe para compor o body do e-mail.


