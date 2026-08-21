"""
run_pix_backfill_energiabet.py
================================
Roda o backfill (modo="full") da Agregação Pix para ENERGIABET.

Uso (PowerShell):
    python run_pix_backfill_energiabet.py *> backfill_pix_energiabet.log

Ou, para rodar em segundo plano de verdade (libera o terminal):
    Start-Process python -ArgumentList "run_pix_backfill_energiabet.py" `
        -RedirectStandardOutput backfill_pix_energiabet.log `
        -RedirectStandardError backfill_pix_energiabet_err.log `
        -NoNewWindow

Acompanhar o log em outro terminal:
    Get-Content backfill_pix_energiabet.log -Wait
"""
from consume_api import ConsumeAPI

ConsumeAPI(cliente='ENERGIABET_PIX', modo='full', partner_id=181)
