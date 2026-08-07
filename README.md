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
├── requirements.txt
├── .env                         # Credenciais (não versionado)
├── .env.example                 # Modelo sem valores reais
└── .gitignore
```

### Descrição dos módulos novos

**`db_logger.py`** — persiste cada execução do ETL na tabela `inplay.etl_execution_logs` (banco determinado pelo cliente: ZEROUM/ZRO_1_BET → `dlzeroum`; ENERGIABET → `dlenergiabet`, via lista explícita `CLIENTES_ZEROUM`). Registra operação, status (`SUCCESS`/`FAILED`), horário de início/fim, duração e motivo do erro. Nunca lança exceção — uma falha no logger não interrompe o ETL.

> ✅ **Corrigido:** `_resolver_banco` decide pelo cliente estar ou não em `CLIENTES_ZEROUM` (um `set` explícito), não por correspondência exata a `'ZEROUM'`. Ao adicionar `ZEROUM_VALIDA_APOSTA`/`ZEROUM_SALDO`/`ZEROUM_VALIDACAO`, os dois modos de backfill (`ZEROUM_BACKFILL_USUARIO`, `ZEROUM_BACKFILL_USUARIO_POR_IDS`) tinham ficado de fora da lista — a checagem inicial de "tabela garantida" ia para `dlenergiabet` por engano (chamadas explícitas de `log_operation(cliente='ZEROUM', ...)` já usavam o banco certo, então o log em si não corrompia, só a mensagem inicial). Os dois foram adicionados a `CLIENTES_ZEROUM`, junto com `ZEROUM_BACKFILL_HISTORICO_PROTECAO` (ver seção **Proteção de Dados Pessoais** abaixo). Os modos `ENERGIABET_*` não precisam de entrada própria: caem no `else` (`dlenergiabet`) corretamente por padrão.

**`consume_api.py`** — ⚠️ até esta revisão, `extrai_csv_nativo` chamava `time.sleep(...)` no laço de retry sem que o módulo `time` estivesse importado no arquivo. Isso não quebrava o caminho feliz, mas fazia qualquer retry real (tentativa 1 ou 2 de 3 falhando) estourar `NameError` em vez de tentar de novo — mascarando o erro original e anulando o propósito do retry. Corrigido com `import time` no topo do arquivo.

> 🔒 **Novo:** `principal_zeroum`/`principal_energiabet`/`backfill_dim_usuario`/`backfill_dim_usuario_por_ids` foram alterados para incluir os campos novos de identificação pessoal em `dim_usuario` e parar de descriptografar nome/data de nascimento/celular. Ver seção **Proteção de Dados Pessoais em `dim_usuario`** para detalhes completos (colunas novas, decisão de produto, e dois novos modos de `--cliente` para a carga histórica).

**`agregacao_dim_usuario.py`** — alimenta as tabelas físicas que substituem o processamento pesado das views `vw_dim_usuario` e `vw_fato_usuarios_diario` no Redshift. Executado ao final de cada carga ZEROUM, escopado apenas aos usuários impactados na janela incremental. Não se aplica à Energiabet.


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
`stg_agg_usuario_reativacao`, `stg_usuario_backfill`

**Log/auditoria:**
`log_vendas_data_rejeitados`, `stg_vendas_data_rejeitados_tmp`,
`verificacao_zero_um`, `verificacao_energia_bet`

### Scripts de migração (rodar uma vez no banco)

| Script | Onde rodar | Finalidade |
|---|---|---|
| `migration_tabelas_fisicas.sql` | `dlzeroum` | Cria tabelas de agregação (ZEROUM) |
| `migration_etl_execution_logs.sql` | `dlzeroum` **e** `dlenergiabet` | Cria tabela de log de execuções |
| `alter_dim_usuario_dados_pessoais.sql` | `dlzeroum` **e** `dlenergiabet` | Adiciona as 11 colunas de proteção de dados pessoais em `dim_usuario`/`stg_usuario`, e cria `stg_usuario_backfill` |

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