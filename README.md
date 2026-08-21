# ETL DataGuvi — Pipeline de Estruturação de Dados

## Visão Geral

Pipeline de **ETL (Extract, Transform, Load)** responsável por extrair dados de fontes externas (API Metabase e bancos PostgreSQL de parceiros), tratá-los e carregá-los no Data Warehouse Redshift, seguindo o padrão **stage → merge/fact**.

O pipeline atende três frentes de clientes:

- **ZEROUM** — extração via API Metabase (Clickhouse), carga de fatos de usuário, depósitos, saques, apostas, bônus, jogos de cassino e tabelas físicas de agregação para o BI.
- **ENERGIABET** — mesmo fluxo do ZEROUM, base separada.
- **ZRO_1_BET** — extração direta de PostgreSQL externo (`zro1_bet_adtk`), validação com log de rejeitados e carga incremental/histórica de vendas.

Além do fluxo principal (fact_user_daily e correlatas), dois recursos rodam como cargas **diárias isoladas**, fora do pipeline horário — ver seções **Agregação Pix** e **Bônus** abaixo:

- **Agregação Pix** — resumo de chaves Pix usadas por cliente em saques, consumido via endpoint por um parceiro externo.
- **Bônus** (`fact_user_bonus`/`dim_bonus`/`bridge_bonus_product` + `agg_bonus_concessoes`) — granular por concessão de bônus, com uma visão agregada por cliente+bônus equivalente em espírito à Agregação Pix.

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
| `API_ROTA_CSV` | Endpoint de extração CSV do Metabase (usado tanto para cards salvos quanto para queries nativas) |
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
| `METABASE_CARD_USUARIOS_ZEROUM` | *(opcional)* Sobrescreve qual card o pipeline usa para dados de usuário — ZeroUm. Fallback (se ausente do `.env`): `card__14826`, o card de produção. Usar só em ambiente local/de teste, apontando para um card clonado (ex.: `card__21517`) — nunca definir em produção |
| `METABASE_CARD_USUARIOS_ENERGIABET` | *(opcional)* Mesmo mecanismo acima, para Energiabet. Fallback: `card__15850` |


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
├── agregacao_bonus.py           # AGG_BONUS_CONCESSOES — agregado incremental de bônus por cliente
├── requirements.txt
├── .env                         # Credenciais (não versionado)
├── .env.example                 # Modelo sem valores reais
└── .gitignore
```

### Descrição dos módulos novos

**`db_logger.py`** — persiste cada execução do ETL na tabela `inplay.etl_execution_logs` (banco determinado pelo cliente: ZEROUM/ZRO_1_BET → `dlzeroum`; ENERGIABET → `dlenergiabet`, via lista explícita `CLIENTES_ZEROUM`). Registra operação, status (`SUCCESS`/`FAILED`), horário de início/fim, duração e motivo do erro. Nunca lança exceção — uma falha no logger não interrompe o ETL.

> ✅ **Corrigido:** `_resolver_banco` decide pelo cliente estar ou não em `CLIENTES_ZEROUM` (um `set` explícito), não por correspondência exata a `'ZEROUM'`. Ao adicionar `ZEROUM_VALIDA_APOSTA`/`ZEROUM_SALDO`/`ZEROUM_VALIDACAO`, os dois modos de backfill (`ZEROUM_BACKFILL_USUARIO`, `ZEROUM_BACKFILL_USUARIO_POR_IDS`) tinham ficado de fora da lista — a checagem inicial de "tabela garantida" ia para `dlenergiabet` por engano (chamadas explícitas de `log_operation(cliente='ZEROUM', ...)` já usavam o banco certo, então o log em si não corrompia, só a mensagem inicial). Os dois foram adicionados a `CLIENTES_ZEROUM`, junto com `ZEROUM_BACKFILL_HISTORICO_PROTECAO` (ver seção **Proteção de Dados Pessoais** abaixo) e, mais recentemente, `ZEROUM_PIX` (mesmo sintoma, ver seção **Agregação Pix**). Os modos `ENERGIABET_*` não precisam de entrada própria: caem no `else` (`dlenergiabet`) corretamente por padrão.
>
> ⚠️ **Cuidado com Windows/PowerShell:** o console do Windows usa `cp1252`, que não representa todo caractere Unicode — um `→` num `print()` de confirmação já causou `UnicodeEncodeError` (capturado pelo `except`, gerando uma mensagem de "falha ao registrar log" **enganosa**, mesmo com o `INSERT` já commitado no banco). Corrigido trocando por `->` (ASCII). Ao adicionar novo texto a um `print()` neste arquivo, evitar caracteres fora do cp1252 (setas, emojis) — acentos comuns do português (ã, ç, é etc.) são seguros, fazem parte do cp1252.

**`consume_api.py`** — ⚠️ até esta revisão, `extrai_csv_nativo` chamava `time.sleep(...)` no laço de retry sem que o módulo `time` estivesse importado no arquivo. Isso não quebrava o caminho feliz, mas fazia qualquer retry real (tentativa 1 ou 2 de 3 falhando) estourar `NameError` em vez de tentar de novo — mascarando o erro original e anulando o propósito do retry. Corrigido com `import time` no topo do arquivo.

> 🔒 **Novo:** `principal_zeroum`/`principal_energiabet`/`backfill_dim_usuario`/`backfill_dim_usuario_por_ids` foram alterados para incluir os campos novos de identificação pessoal em `dim_usuario` e parar de descriptografar nome/data de nascimento/celular. Ver seção **Proteção de Dados Pessoais em `dim_usuario`** para detalhes completos (colunas novas, decisão de produto, e dois novos modos de `--cliente` para a carga histórica).
>
> 🆕 **Novo:** `processa_agregacao_pix`/`_sql_agregacao_pix` (carga diária isolada de `agg_pix_cliente`) e `processa_bonus`/`processa_bonus_backfill`/`processa_bonus_backfill_awarding_nulo` (carga diária isolada de `fact_user_bonus`/`dim_bonus`/`bridge_bonus_product`, com backfill histórico em dois passos). Ver seções **Agregação Pix** e **Bônus** abaixo para o desenho completo, incluindo um bug real (schema/`PartnerId` fixos em várias queries auxiliares de Bônus) encontrado e corrigido ao ativar para ENERGIABET.

**`agregacao_dim_usuario.py`** — alimenta as tabelas físicas que substituem o processamento pesado das views `vw_dim_usuario` e `vw_fato_usuarios_diario` no Redshift. Executado ao final de cada carga ZEROUM, escopado apenas aos usuários impactados na janela incremental. Não se aplica à Energiabet.

**`agregacao_bonus.py`** — alimenta `inplay.agg_bonus_concessoes` (visão agrupada por `client_id`+`bonus_id`, equivalente em espírito a `agg_pix_cliente`). Chamado de dentro de `processa_bonus()`, logo após o merge de `fact_user_bonus`, recebendo o mesmo DataFrame já extraído — não faz nenhuma extração adicional do Metabase, só recalcula (via `GROUP BY` filtrado) os pares impactados na carga do dia. Uma falha aqui não derruba a carga de `fact_user_bonus`, que já está commitada nesse ponto. Ver seção **Bônus** para detalhes.


## Banco de Dados

### Grupos de conexão

| Grupo | Banco | Clientes |
|---|---|---|
| Data Warehouse destino | Redshift (`dlzeroum`) | ZEROUM, ZRO_1_BET |
| Data Warehouse destino | Redshift (`dlenergiabet`) | ENERGIABET |
| Origem ZRO_1_BET | PostgreSQL externo (`zro1_bet_adtk`) | ZRO_1_BET |
| Origem ZEROUM/ENERGIABET | API Metabase (Clickhouse) — cards salvos e queries nativas | ZEROUM, ENERGIABET |

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
| `agg_pix_cliente` | Chaves Pix por cliente (saques), com soft delete (`is_active`/`deleted_at`) | ZEROUM, ENERGIABET |
| `fact_user_bonus` | Fato granular de bônus (1 linha por concessão) | ZEROUM, ENERGIABET |
| `dim_bonus` | Dimensão de configuração de bônus | ZEROUM, ENERGIABET |
| `bridge_bonus_product` | Ponte bônus × produto elegível | ZEROUM, ENERGIABET |
| `agg_bonus_concessoes` | Agregado por `client_id`+`bonus_id`, atualização incremental | ZEROUM, ENERGIABET |

> 🆕 **`agg_pix_cliente`, `fact_user_bonus`, `dim_bonus`, `bridge_bonus_product` e
> `agg_bonus_concessoes` são cargas diárias isoladas**, fora do pipeline horário principal
> (`principal_zeroum`/`principal_energiabet` não as chamam). Ver seções **Agregação Pix** e
> **Bônus** abaixo.

> 🔒 **`dim_usuario` ganhou 11 colunas novas** relacionadas à proteção de dados pessoais
> (`lastname`, `taxnumber`, `documenttype`, `documentnumber`, `documentissuedby`,
> `isdocumentverified`, `kycstatus`, `kycdocsstatus`, `first_name_protegido`,
> `mobile_number_protegido`, `birth_date_protegido`). Ver seção **Proteção de Dados
> Pessoais em `dim_usuario`** para o detalhamento completo.

**Stage (temporárias, limpas a cada carga):**
`stg_fact_user_daily`, `stg_fact_user_daily_sport`, `stg_game`,
`stg_fact_deposits_withdraws_summarized`, `stg_fact_casino_games_hourly`,
`stg_usuario`, `stg_vendas_data`, `stg_usuarios_impactados`,
`stg_fact_user_atividade_diaria`, `stg_agg_usuario_metricas`,
`stg_agg_usuario_reativacao`, `stg_usuario_backfill`,
`stg_agregacao_pix_cliente`, `stg_fact_user_bonus`, `stg_dim_bonus`,
`stg_bridge_bonus_product`, `stg_pares_bonus_impactados`, `stg_agg_bonus_concessoes`,
`stg_backfill_trigger_ref_bonus` *(só a ferramenta pontual `backfill_trigger_ref_client`)*

**Log/auditoria:**
`log_vendas_data_rejeitados`, `stg_vendas_data_rejeitados_tmp`,
`verificacao_zero_um`, `verificacao_energia_bet`

### Scripts de migração (rodar uma vez no banco)

| Script | Onde rodar | Finalidade |
|---|---|---|
| `migration_tabelas_fisicas.sql` | `dlzeroum` | Cria tabelas de agregação (ZEROUM) |
| `migration_etl_execution_logs.sql` | `dlzeroum` **e** `dlenergiabet` | Cria tabela de log de execuções |
| `alter_dim_usuario_dados_pessoais.sql` | `dlzeroum` **e** `dlenergiabet` | Adiciona as 11 colunas de proteção de dados pessoais em `dim_usuario`/`stg_usuario`, e cria `stg_usuario_backfill` |
| `migration_agregacao_pix.sql` | `dlzeroum` **e** `dlenergiabet` | Cria `agg_pix_cliente`/`stg_agregacao_pix_cliente` |
| `migration_energiabet.sql` | `dlenergiabet` | Cria as tabelas de Pix e Bônus (`fact_user_bonus`/`dim_bonus`/`bridge_bonus_product` + stages) — DDL reconciliado com o de produção real de `dlzeroum` |
| `migration_agg_bonus_concessoes.sql` | `dlzeroum` **e** `dlenergiabet` | Cria `agg_bonus_concessoes`/`stg_pares_bonus_impactados`/`stg_agg_bonus_concessoes` |

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

# Agregação Pix (carga diária isolada, incremental por padrão)
python main.py --cliente ZEROUM_PIX
python main.py --cliente ENERGIABET_PIX

# Bônus (carga diária isolada, incremental por padrão -- já dispara
# AGG_BONUS_CONCESSOES automaticamente, não precisa chamar separado)
python main.py --cliente ZEROUM_BONUS
python main.py --cliente ENERGIABET_BONUS
```

> Para carga histórica do ZRO_1_BET, alterar o parâmetro `modo` para `"historico"` na chamada de `principal_zro_1_bet()` em `consume_api.py`.

### Validação de queries nativas (card vs. nativo)

Antes de trocar a fonte de uma extração de "card do Metabase" para "query nativa" em produção (ver seção **Extrações nativas** abaixo), usar `valida_aposta` para comparar os dois resultados na mesma janela de tempo:

```bash
python main.py --cliente ZEROUM_VALIDA_APOSTA
```

Compara primeira e última aposta (card vs. nativo) e reporta linhas só numa fonte, duplicados e divergências — inclusive separando divergências explicáveis por diferença de horário entre as duas chamadas (`divergentes_timing_skew`) das genuinamente suspeitas (`divergentes_suspeitos`). Gera CSVs de diagnóstico quando há divergência.

### Regularização pontual de `dim_usuario`

Quando o pipeline principal falha no meio da execução (antes de chegar na etapa de usuários), `dim_usuario` pode ficar com `updated_at`/`import_date` desatualizados em relação às demais tabelas — porque a janela da incremental normal é calculada a partir de `fact_user_daily`, não de `dim_usuario`, e portanto não revisita sozinha o período em que a falha ocorreu. Duas rotinas cobrem esse cenário sem reprocessar o pipeline inteiro:

```bash
# Por janela de data (cobre todos os usuários atualizados a partir de uma data)
python main.py --cliente ZEROUM_BACKFILL_USUARIO --data-inicial-backfill 2026-07-19T00:00:00
python main.py --cliente ENERGIABET_BACKFILL_USUARIO --data-inicial-backfill 2026-07-19T00:00:00

# Por lista específica de ids (ex.: ids identificados com carga incompleta)
python main.py --cliente ZEROUM_BACKFILL_USUARIO_POR_IDS --ids-backfill 123,456,789
python main.py --cliente ENERGIABET_BACKFILL_USUARIO_POR_IDS --ids-backfill 123,456,789
```

Ambas mexem só em `stg_usuario`/`dim_usuario` (não rodam agregações). Usar a de data como primeiro recurso ao descobrir o problema; a de ids quando já se tem uma lista exata de registros incompletos (ex.: via `SELECT id FROM dim_usuario WHERE updated_at IS NULL`), evitando reprocessar uma janela de dias inteira por poucos registros.

### Carga histórica de proteção de dados pessoais (execução única)

Ver seção **Proteção de Dados Pessoais em `dim_usuario`** para o contexto completo. Comando:

```bash
python main.py --cliente ZEROUM_BACKFILL_HISTORICO_PROTECAO \
    --data-inicial-backfill 2015-01-01T00:00:00 --data-final 2026-08-01T00:00:00

python main.py --cliente ENERGIABET_BACKFILL_HISTORICO_PROTECAO \
    --data-inicial-backfill 2015-01-01T00:00:00 --data-final 2026-08-01T00:00:00
```

Extrai em janelas de tempo por `registration_date` (protege contra o teto de exportação
de 1.048.575 linhas do Metabase), grava em `stg_usuario_backfill` e aplica um único
`UPDATE` deduplicado por `id` ao final. Idempotente — pode ser reexecutado quantas vezes
for necessário, inclusive para cobrir só o intervalo que ainda faltar (comparar
`registration_date` de quem ainda está com as colunas novas em `NULL`).

> Depois de uma carga grande (milhões de linhas), rodar `VACUUM`/`ANALYZE` em
> `dim_usuario` manualmente — não é feito automaticamente pelo script.

Logs de execução gravados em `etl.log` (rotação automática: 5 arquivos × 10 MB). Em caso de falha, e-mail enviado automaticamente e resultado gravado em `inplay.etl_execution_logs`.


## Alertas de Erro e Log de Execução

### E-mail (`send_email.py`)

Implementa o padrão DataGuvi. Lê credenciais diretamente do `.env` (`EMAIL_SENDER`, `SENDER_PASSWORD`, `EMAIL_RECEIVER`). Disparado em qualquer exceção nos métodos de orquestração de nível mais alto (`principal_zeroum`, `principal_energiabet`, `principal_zro_1_bet`, `valida_dados`, `valida_aposta`, `backfill_dim_usuario`, `backfill_dim_usuario_por_ids`, `processa_saldo_diario`).

> As camadas internas de comunicação com a API (`conection`, `extrai_csv`, `extrai_dados_card`, `extrai_csv_nativo`) **não enviam e-mail** — só logam e deixam a exceção subir. Isso é proposital: até essa correção, uma mesma falha de rede/timeout podia gerar até 3 e-mails (um por camada da pilha de chamadas). Agora só o método de nível mais alto notifica, uma vez por falha.

### Log no banco (`db_logger.py`)

Implementa o padrão DataGuvi. Grava na tabela `inplay.etl_execution_logs` do banco correspondente ao cliente. Cobertura:

| Operação | Banco do log |
|---|---|
| `ETL_ZEROUM` | `dlzeroum` |
| `ETL_ENERGIABET` | `dlenergiabet` |
| `ETL_ZRO_1_BET` | `dlzeroum` |
| `VALIDACAO_ZEROUM` | `dlzeroum` |
| `VALIDACAO_ENERGIABET` | `dlenergiabet` |
| `VALIDA_APOSTA_ZEROUM` | `dlzeroum` |
| `VALIDA_APOSTA_ENERGIABET` | `dlenergiabet` |
| `BACKFILL_DIM_USUARIO_ZEROUM` | `dlzeroum` |
| `BACKFILL_DIM_USUARIO_ENERGIABET` | `dlenergiabet` |
| `BACKFILL_DIM_USUARIO_POR_IDS_ZEROUM` | `dlzeroum` |
| `BACKFILL_DIM_USUARIO_POR_IDS_ENERGIABET` | `dlenergiabet` |
| `BACKFILL_HISTORICO_PROTECAO_ZEROUM` | `dlzeroum` |
| `BACKFILL_HISTORICO_PROTECAO_ENERGIABET` | `dlenergiabet` |
| `ETL_ZEROUM_PIX` | `dlzeroum` |
| `ETL_ENERGIABET_PIX` | `dlenergiabet` |
| `ETL_BONUS` (cliente=`ZEROUM`) | `dlzeroum` |
| `ETL_BONUS` (cliente=`ENERGIABET`) | `dlenergiabet` |

Consultas úteis de monitoramento estão documentadas em `migration_etl_execution_logs.sql`.


## Proteção de Dados Pessoais em `dim_usuario`

`dim_usuario` recebeu 11 colunas novas para armazenar dados de identificação pessoal do
cliente. O objetivo é duplo: (1) trazer campos que nunca existiram na dimensão
(sobrenome, CPF, documento, status KYC), e (2) parar de gravar em claro os campos que já
existiam e continham dado sensível descriptografado (nome, data de nascimento, celular).

### Colunas novas

| Coluna | Tipo | Origem (Metabase/`Client`) | Fica cifrada? |
|---|---|---|---|
| `lastname` | `VARCHAR(256)` | `EncryptedLastName` | Sim |
| `taxnumber` | `VARCHAR(100)` | `EncryptedTaxNumber` | Sim |
| `documenttype` | `INTEGER` | `DocumentType` | Não (sem par cifrado na origem) |
| `documentnumber` | `VARCHAR(50)` | `DocumentNumber` | Não |
| `documentissuedby` | `VARCHAR(255)` | `DocumentIssuedBy` | Não |
| `isdocumentverified` | `BOOLEAN` | `IsDocumentVerified` | — |
| `kycstatus` | `INTEGER` | `KYCStatus` | — |
| `kycdocsstatus` | `INTEGER` | `KYCDocsStatus` | — |
| `first_name_protegido` | `VARCHAR(256)` | `EncryptedFirstName` | Sim |
| `mobile_number_protegido` | `VARCHAR(256)` | `EncryptedPhoneNumber` | Sim |
| `birth_date_protegido` | `VARCHAR(256)` | `DateOfBirth` | Sim |

### Decisão de produto: `first_name`/`birth_date`/`mobile_number` ficam congelados

As três colunas que já existiam (`first_name`, `birth_date`, `mobile_number`) **não são
mais lidas nem gravadas** por nenhum dos métodos do pipeline — `principal_zeroum`,
`principal_energiabet`, `backfill_dim_usuario` e `backfill_dim_usuario_por_ids` deixaram de
selecioná-las e o bloco de descriptografia AES (`Util.descriptografar`) foi removido por
completo desses métodos (o import de `Util` também foi removido de `consume_api.py` — a
descriptografia deixou de acontecer em qualquer ponto deste pipeline). Essas três colunas
ficam congeladas no último valor que já tinham antes desta mudança; só as colunas
`*_protegido` continuam recebendo atualização, sempre cifradas.

> Motivo: essas colunas são consumidas por uma API do cliente. A decisão foi não alterar o
> formato do que a API já recebe — em vez de trocar o conteúdo (que quebraria o consumo
> existente), o dado sensível passa a ser trazido só nas colunas novas, em paralelo.

### Card do Metabase

Os cards `card__14826` (ZeroUm) e `card__15850` (EnergiaBet) já foram atualizados em
produção para expor as colunas novas — incluindo `first_name_protegido`,
`mobile_number_protegido` e `birth_date_protegido` diretamente na `SELECT` (como aliases
duplicados de `EncryptedFirstName`/`EncryptedPhoneNumber`/`DateOfBirth`, já que essas
fontes também alimentam `first_name`/`mobile_number`/`birth_date`, que continuam sendo
extraídas do card mas não são mais persistidas). Nenhuma descriptografia acontece: as
colunas `*_protegido` são gravadas exatamente como retornam do Metabase.

Para apontar um ambiente para um card de teste/clonado em vez do de produção, usar as
variáveis `METABASE_CARD_USUARIOS_ZEROUM`/`METABASE_CARD_USUARIOS_ENERGIABET` no `.env`
(ver seção **Variáveis de Ambiente**) — nunca editar `enums.py` para isso.

### Carga histórica

Ver comando em **Comandos de Execução** → *Carga histórica de proteção de dados
pessoais*. Pontos importantes de implementação:

- **Teto de exportação do Metabase (1.048.575 linhas):** `_extrai_usuario_em_janelas`
  subdivide o intervalo pedido recursivamente por `registration_date` sempre que uma
  janela bate no teto ou falha por timeout/conexão — mesmo espírito de
  `extrai_dados_card_por_periodo` (ver seção **Extrações nativas**), mas específico para o
  card de usuários (via `extrai_dados_card`, não uma query nativa).
- **Sub-lotes de inserção:** cada janela extraída é inserida em `stg_usuario_backfill` em
  sub-lotes de 100 mil linhas (não a janela inteira de uma vez), com retry de até 5
  tentativas e backoff exponencial (10s a 90s) — mitiga quedas de conexão em execuções
  longas (histórico completo pode levar horas).
- **`UPDATE` final deduplicado:** o `UPDATE` de `stg_usuario_backfill` para `dim_usuario`
  passa por uma subconsulta com `ROW_NUMBER() OVER (PARTITION BY id)` antes de aplicar —
  protege contra duplicidade de `id` que pode ocorrer quando o filtro `BETWEEN` (inclusivo
  nas duas pontas) captura o mesmo registro em duas janelas adjacentes.
- Não é destrutivo: só popula colunas que não existiam ou que não têm mais nenhuma outra
  escrita concorrente. Pode ser reexecutado livremente.


## Agregação Pix

Tabela `inplay.agg_pix_cliente` — resumo de chaves Pix usadas por cliente em saques
(`ClientId`+`PixKey`+`PixType`, com a contagem de transações), consumida via endpoint por
um **cliente/parceiro externo**. Carga diária isolada, fora do pipeline horário
(`ZEROUM_PIX`/`ENERGIABET_PIX`).

### Decisão: incremental, não full diário

A origem (`PaymentRequest`, filtrada por `Type=1, Status IN (7,8,12), PixKey IS NOT NULL`)
tem ~77,5M linhas / ~775K grupos resultantes para o ZEROUM — rodar o `GROUP BY` completo
todo dia é inviável a médio prazo (tabela transacional, só cresce). O desenho:

- **Backfill (`modo="full"`), uma vez só**: popula a tabela do zero. Obrigatório antes de
  ativar o incremental (sem ele não há cursor de partida).
- **Diário (`modo="incremental"`, padrão)**: usa `max(updated_at)` do destino como cursor,
  filtra a origem por `_peerdb_synced_at` (CDC) desde esse cursor (margem de 4h), identifica
  os `ClientId` impactados e recalcula do zero o agrupamento completo **só** para esses
  clientes — não faz delta (`count = count + delta`), porque não há garantia de que um
  `PaymentRequest` não mude de status depois de atingir 7/8/12 (estorno, reprocessamento).

A origem é `SharedReplacingMergeTree` (CDC) — a query sempre deduplica por `Id`
(`QUALIFY ROW_NUMBER() ... ORDER BY _peerdb_version DESC) = 1`), mesmo padrão de
`_sql_saldo_diario`.

### Soft delete, não DELETE físico

Como o parceiro externo faz um pull completo inicial e depois sincroniza incrementalmente
via `updated_at`, um `DELETE` físico esconderia remoções lógicas dele (uma chave Pix que
deixa de existir simplesmente sumiria da tabela, sem sinalização). Em vez disso:

- Grupos que deixam de aparecer no recálculo são marcados `is_active = false`,
  `deleted_at = <timestamp>`, com `updated_at` atualizado **no mesmo `UPDATE`** — é esse
  campo que carrega o "algo mudou aqui" pro parceiro.
- Grupos que continuam válidos (ou voltaram a existir) são upsertados de volta com
  `is_active = true`, `deleted_at = NULL`.
- Rodar `modo="full"` ocasionalmente (ex.: mensal) funciona como reconciliação de
  segurança — o soft-delete nesse modo varre a tabela inteira, não só uma janela.

### `PartnerId` por cliente

| Cliente | `PartnerId` (PaymentRequest) | Confirmação |
|---|---|---|
| ZEROUM | `180` | Confirmado em produção (mesmo valor já usado em `valida_aposta()`) |
| ENERGIABET | `181` | Confirmado empiricamente (volume/backfill reais rodaram corretamente) |

`ConsumeAPI.__init__` e `processa_agregacao_pix` resolvem esse valor automaticamente pelo
`cliente` — não precisa informar `partner_id` manualmente para ZEROUM/ENERGIABET. Para
qualquer cliente novo, é obrigatório informar `partner_id=<valor confirmado>` explicitamente
(o método levanta exceção em vez de assumir um default silenciosamente).


## Bônus

Três tabelas (`fact_user_bonus`, `dim_bonus`, `bridge_bonus_product`) + uma agregação
incremental (`agg_bonus_concessoes`, ver `agregacao_bonus.py`). Carga diária isolada
(`ZEROUM_BONUS`/`ENERGIABET_BONUS`), com `AGG_BONUS_CONCESSOES` disparada automaticamente
dentro de `processa_bonus()` — não é preciso chamar em separado.

### Achado real: schema/`PartnerId` fixos nas queries (corrigido)

Ao ativar para ENERGIABET, `_sql_fact_user_bonus`, `_sql_dim_bonus`,
`_sql_bridge_bonus_product`, `_perfil_diario_bonus`, `_perfil_diario_bonus_generico`,
`_valida_bloco_bonus`, `_valida_bloco_bonus_awarding_nulo` e `_sql_trigger_ref_backfill`
tinham `partner_zeroum.` fixo no `FROM` e/ou `PartnerId = 180` fixo no `WHERE` — rodar para
ENERGIABET falhava com `ACCESS_DENIED` (o usuário `readonly_energiabet` do Clickhouse não
tem grant sobre `partner_zeroum.*`) ou, pior, teria lido dado do parceiro errado
silenciosamente caso a permissão não bloqueasse. Corrigido em todos os pontos:

- Prefixo de schema removido — a conexão do Metabase (`database`, resolvida por `cliente`)
  já define a origem certa, mesmo padrão de `_sql_saldo_diario`/`_sql_agregacao_pix`.
- `PartnerId` parametrizado (`partner_id`) em toda a cadeia de chamadas, incluindo as
  funções auxiliares de perfil/validação usadas só pelo backfill — esse foi o bug mais
  sutil: a assinatura já aceitava o parâmetro, mas 3 pontos de chamada diferentes
  esqueciam de repassar o valor recebido, caindo no default `180` silenciosamente.

`ConsumeAPI.__init__`/`processa_bonus`/`processa_bonus_backfill`/
`processa_bonus_backfill_awarding_nulo` resolvem `partner_id` automaticamente por
`cliente` (`180` ZEROUM, `181` ENERGIABET — este último confirmado via script de produção
real do ambiente Energiabet). Mesma regra do Pix: cliente desconhecido exige `partner_id`
explícito, sem default silencioso.

### Backfill histórico: dois passos obrigatórios

```bash
# via python -c (main.py não expõe --modo/--data-corte para backfill de bônus)
python -c "
from consume_api import ConsumeAPI
api = ConsumeAPI.__new__(ConsumeAPI)
# ... setup real (ver run_bonus_backfill_energiabet.py) ...
api.processa_bonus_backfill(cliente='ENERGIABET', data_inicio_historico='...', data_corte='...', partner_id=181)
api.processa_bonus_backfill_awarding_nulo(cliente='ENERGIABET', data_inicio_historico='...', data_corte='...', partner_id=181)
"
```

1. **`processa_bonus_backfill`** — carga principal, janelada por volume, filtrada por
   `AwardingTime`.
2. **`processa_bonus_backfill_awarding_nulo`** — suplementar, **obrigatória**. Bônus do
   tipo freebet/freespin/riskfree (`BonusType` 12/14/15) nunca têm `AwardingTime`
   preenchido — sem esse segundo passo, esses registros ficam de fora do backfill
   silenciosamente (~9,6M de registros no caso do ZEROUM).

> ⚠️ **Cuidado com `data_corte` = "agora":** o cálculo de janelas trabalha em granularidade
> de dia inteiro — um `data_corte` no meio do dia de hoje ainda inclui o dia inteiro no
> bloco final. Como a tabela de origem é viva (bônus sendo concedidos em tempo real), isso
> pode gerar uma pequena divergência na validação de contagem origem×destino (poucas
> linhas, tipicamente <10 em ~625K) por escrita concorrente durante o backfill. Fixar
> `data_corte` num dia seguro no passado (ex.: ontem) evita o problema; o dia excluído fica
> coberto pela carga incremental normal, que roda logo em seguida.

### `agg_bonus_concessoes` (AGG_BONUS_CONCESSOES)

Visão agrupada por `client_id`+`bonus_id` (qtd de concessões, total concedido, custo,
rollover pendente etc.), pré-calculada para não exigir `GROUP BY` em tempo real sobre
`fact_user_bonus` (58M+ linhas) a cada chamada do endpoint. Estratégia: recalcula **só**
os pares `client_id`+`bonus_id` impactados na carga incremental do dia (via staging +
`INNER JOIN` filtrado, sem escanear a tabela inteira), e faz `MERGE` (upsert) no destino.
Não depende do bug de schema/`PartnerId` acima — só opera sobre `fact_user_bonus` já
carregado no Redshift, nunca volta ao Clickhouse/Metabase.

> Cobre só o que passa pela carga incremental a partir de quando for ativado. O histórico
> já existente em `fact_user_bonus` precisa de uma carga inicial completa, feita uma única
> vez, antes de ativar o hook incremental (não implementada neste README — ver com quem
> ativou o módulo se já existe, ou construir seguindo o mesmo `GROUP BY` de
> `agregacao_bonus.py`, sem o filtro de pares impactados).


## Fluxo do ETL

### ZEROUM e ENERGIABET (legado + agregação)

```
Autenticação Metabase
        ↓
Determina janela incremental (último updated_at em fact_user_daily)
        ↓
Limpa stages
        ↓
Extração de cards Metabase + queries nativas
(stage, depósito, saque, bônus, apostas, jogos, usuários)
        ↓
Tratamento (NaN→None)
        ↓
Para cada subconjunto: insere_dados_bulk (stage) → mergeia_dados (fact/dim)
        ↓
[ZEROUM] Atualiza tabelas físicas de agregação
(fact_user_atividade_diaria, agg_usuario_metricas,
 agg_usuario_recorrencia_30d, agg_usuario_reativacao)
        ↓
Grava SUCCESS em etl_execution_logs
```

### Extrações nativas (otimização de performance)

Alguns cards do Metabase têm um problema estrutural: eles agregam (`GROUP BY`) o **histórico completo** de uma tabela grande (`Bet`, 9M+ linhas) e só aplicam o filtro de janela incremental **depois**, por fora — obrigando o ClickHouse a escanear/agregar tudo antes de descartar o que não interessa. Em produção isso já causou `MEMORY_LIMIT_EXCEEDED` e timeouts de dezenas de minutos.

Três extrações foram reescritas como queries SQL nativas (rodando direto no ClickHouse via `extrai_csv_nativo`, em vez de um card salvo), com o filtro embutido **antes** da agregação:

| Card original | Método novo | Observação de semântica |
|---|---|---|
| `ZeroUm_PrimeiraAposta` / `ZeroUm_UltimaAposta` | `extrai_aposta_nativo` (`_sql_aposta`) | Filtra **quem** entra na agregação (subquery de `ClientId`), mas agrega o histórico completo desse subconjunto — filtrar direto a agregação mudaria o resultado (confirmado via `valida_aposta`) |
| `ZeroUm_UsuariosTotalizadorBet` | `extrai_usuario_totalizador_bet_nativo` (`_sql_usuario_totalizador_bet`) | Mesmo princípio: filtra `cliente_ajustado` (pequeno) antes do `JOIN` com `Bet` (grande) |
| `ZeroUm_ApostasJogosHora` | `extrai_apostas_jogos_hora_nativo` (`_sql_apostas_jogos_hora`) | Sem risco de mudança de semântica — o agrupamento já deriva do mesmo campo usado no filtro, então filtrar antes ou depois do `GROUP BY` dá o mesmo resultado |

> **EnergiaBet:** a correção de `ApostasJogosHora` e de primeira/última aposta depende de `PartnerId`, ainda não confirmado para a EnergiaBet — essas duas continuam no card antigo para esse cliente (sinalizado em comentário no código). `UsuariosTotalizadorBet` não depende de `PartnerId` e já foi aplicado para os dois clientes.

Cada método nativo tem timeout próprio, maior que o de cards normais (900s, versus 2400s de `extrai_dados_card`) — não porque a query nativa seja mais lenta no geral, mas porque quando aplicável a query nativa está fazendo um trabalho estrutural diferente (agregação de histórico completo de um subconjunto), medido manualmente antes de calibrar a margem.

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

> Seguindo a correção de e-mails duplicados: se o novo método chamar outros métodos internos da classe (`extrai_csv`, `extrai_csv_nativo`, `extrai_dados_card`, etc.), esses métodos internos não devem enviar e-mail — só o método de orquestração de nível mais alto deve notificar, uma vez por falha.


## Testes e Validação (Pix e Bônus)

Scripts auxiliares para validar a integridade de Pix/Bônus/AGG_BONUS_CONCESSOES sem depender só de leitura de log. Do mais barato ao mais completo:

| Script | Toca rede/banco? | O que valida |
|---|---|---|
| `test_integridade_etl.py` | Não | Import, wiring (`AGG_BONUS_CONCESSOES` chamado no lugar certo, dispatch de cliente presente), SQL gerada sem schema fixo e com `PartnerId` correto, auditoria de que toda chamada às funções auxiliares de bônus repassa `partner_id` |
| Import direto (`python -c "from consume_api import ConsumeAPI"`) | Não | Dependência faltando / import circular |
| `pre_flight_test_energiabet.py` | Só leitura (Metabase) | Autenticação + volume real de Pix/Bônus antes de um backfill grande |
| `auditoria_integridade_etl.sql` | Só leitura (Redshift) | Volume, soft-delete consistente, PK duplicada, e **reconciliação** de `agg_bonus_concessoes` contra `fact_user_bonus` (recalcula uma amostra na unha e compara) |
| `run_pix_backfill_energiabet.py` / `run_bonus_backfill_energiabet.py` | Grava dado real | Backfill histórico completo, não-interativo (via `argparse`, sem `input()` — seguro para `Start-Process`/segundo plano no PowerShell) |

Rodar nessa ordem (estático → import → funcional pequeno → banco) cobre a cadeia inteira sem precisar de um backfill completo só para validar uma mudança de código.

>