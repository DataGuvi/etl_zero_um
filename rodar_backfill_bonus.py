"""
rodar_backfill_bonus.py

Roda a carga histórica (backfill) de Bônus, em blocos, com validação
automática por bloco.

IMPORTANTE: a parte de inicialização abaixo (logger, engine_dw, db_logger
etc.) precisa ser IDÊNTICA ao que você já ajustou em teste_isolado_bonus.py
para o teste anterior ter funcionado. Copie exatamente esse mesmo trecho
de setup para cá antes de rodar — só o bloco de chamada do método muda.

Como rodar:
    python rodar_backfill_bonus.py
"""

import logging
import time
from datetime import datetime, timedelta
from consume_api import ConsumeAPI

# ============================================================================
# SETUP — copiar exatamente o que já funcionou em teste_isolado_bonus.py
# ============================================================================
api = ConsumeAPI.__new__(ConsumeAPI)

logging.basicConfig(level=logging.INFO)
api.logger = logging.getLogger("backfill_bonus")

# api.engine_dw = api.create_engine_dw()
# api.db_logger = DBLogger(cliente='ZEROUM')
# (repetir aqui qualquer outra linha extra que você precisou adicionar
#  para o teste_isolado_bonus.py funcionar)

# ============================================================================
# RECOMENDAÇÃO: rode primeiro um teste pequeno (2-3 janelas), não o
# histórico inteiro de uma vez. Comente/descomente conforme a etapa.
# ============================================================================

MODO_TESTE_PEQUENO = False  # já retomando o backfill real a partir de julho/2025

if MODO_TESTE_PEQUENO:
    print("=== TESTE PEQUENO ===")
    data_inicio = '2025-04-11T00:00:00'
    data_corte = '2025-06-10T00:00:00'   # ~2 meses, só para validar o loop e a validação
    limite_linhas = 300000
else:
    print("=== BACKFILL COMPLETO (retomando de onde parou) ===")
    data_inicio = '2025-11-29T00:00:00'   # ajustar se MAX(awarding_time) mudou
    data_corte = datetime.now().strftime('%Y-%m-%dT00:00:00')  # até hoje
    limite_linhas = 300000  # janelas menores = feedback mais frequente

print(f"Período: {data_inicio} até {data_corte}, janelas de até {limite_linhas:,} linhas")
print("O tempo por bloco e a estimativa de tempo restante já aparecem nos logs "
      "de dentro de processa_bonus_backfill (linhas '[BACKFILL BÔNUS] Bloco N/M OK em ...').\n")

# ============================================================================
# Contador de tempo total (o detalhamento por bloco já vem dos logs do
# próprio método processa_bonus_backfill, que sabe quantos blocos existem
# no total desde o início — não precisa duplicar essa lógica aqui fora)
# ============================================================================
inicio_execucao = time.monotonic()

try:
    api.processa_bonus_backfill(
        cliente='ZEROUM',
        data_inicio_historico=data_inicio,
        data_corte=data_corte,
        limite_linhas_por_janela=limite_linhas,
        validar_blocos=True
        # tamanho_bloco_dias NÃO é passado -- deixa o modo otimizado (perfil
        # de volume real) decidir o tamanho das janelas. Se precisar do
        # comportamento antigo por algum motivo, passe
        # tamanho_bloco_dias=30 aqui explicitamente.
    )
    tempo_total = time.monotonic() - inicio_execucao
    print(f"\n✅ Backfill concluído sem exceções. Tempo total: {timedelta(seconds=int(tempo_total))}")
except Exception as e:
    tempo_total = time.monotonic() - inicio_execucao
    print(f"\n❌ Backfill parou com erro após {timedelta(seconds=int(tempo_total))}: {e}")
    print("Veja no log a mensagem '[BACKFILL BÔNUS] Falhou no bloco ...' — ")
    print("ela já te diz a data exata para retomar (parâmetro data_inicio_historico).")
    raise

print("""
=== Validações finais (rodar no Redshift, schema inplay) ===

SELECT COUNT(*) FROM inplay.fact_user_bonus;
SELECT MIN(awarding_time), MAX(awarding_time) FROM inplay.fact_user_bonus;
SELECT id, COUNT(*) FROM inplay.fact_user_bonus GROUP BY id HAVING COUNT(*) > 1;
""")