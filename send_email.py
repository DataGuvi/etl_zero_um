import smtplib
import ssl
import os
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()

# ==========================================================================
# CONFIGURAÇÃO SMTP
# ==========================================================================
# O protocolo é detectado automaticamente pela porta (ver SMTP_USE_SSL
# abaixo), pois cada provedor exige um método diferente:
#   - emailemnuvem.com.br exige SSL implícito desde o início da conexão
#     (porta 465) e rejeita conexões que começam em texto plano, mesmo
#     que façam STARTTLS em seguida -> erro 554 5.7.1.
#   - Gmail (e outros) usam STARTTLS na porta 587: a conexão começa em
#     texto plano e depois faz upgrade para TLS.
#
# Por padrão mantém o provedor/porta originais (emailemnuvem.com.br:465,
# SSL implícito). Se precisar migrar para Gmail, basta configurar no
# Coolify: SMTP_HOST=smtp.gmail.com, SMTP_PORT=587, e usar uma "senha de
# app" gerada na conta Google — sem precisar alterar este código.
# ==========================================================================

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.emailemnuvem.com.br")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_TIMEOUT = int(os.getenv("SMTP_TIMEOUT", "60"))
SMTP_DEBUG = os.getenv("SMTP_DEBUG", "0") == "1"

# Detecção automática do protocolo pela porta:
#   465 -> SSL implícito (SMTP_SSL) - exigido pelo emailemnuvem.com.br
#   587 -> STARTTLS (SMTP + starttls) - padrão usado pelo Gmail, por ex.
# Pode ser forçado via env var SMTP_USE_SSL=1 ou SMTP_USE_SSL=0.
_force_ssl = os.getenv("SMTP_USE_SSL")
if _force_ssl is not None:
    SMTP_USE_SSL = _force_ssl == "1"
else:
    SMTP_USE_SSL = SMTP_PORT == 465

# Suporta os dois padrões de nome de variável já usados no projeto,
# para evitar o problema de divergência entre .env local e Coolify.
SENDER_EMAIL = os.getenv("EMAIL_SENDER") or os.getenv("SENDER_EMAIL")
APP_PASSWORD = os.getenv("SENDER_PASSWORD") or os.getenv("APP_PASSWORD")
TO_EMAIL = os.getenv("EMAIL_RECEIVER") or os.getenv("TO_EMAIL")


def send_email(subject, body):
    """
    Envia e-mail de alerta via SMTP, detectando automaticamente se deve
    usar SSL implícito (porta 465) ou STARTTLS (porta 587) conforme a
    porta configurada.

    Variáveis de ambiente esperadas (aceita os dois nomes por compatibilidade):
        EMAIL_SENDER / SENDER_EMAIL   : remetente
        SENDER_PASSWORD / APP_PASSWORD: senha ou app password do remetente
        EMAIL_RECEIVER / TO_EMAIL     : destinatário(s), separados por ";"

    Opcionais:
        SMTP_HOST    (default: smtp.emailemnuvem.com.br)
        SMTP_PORT    (default: 465)
        SMTP_USE_SSL ("1" força SSL implícito, "0" força STARTTLS;
                      se omitido, é inferido pela porta: 465 -> SSL, outra -> STARTTLS)
        SMTP_TIMEOUT (default: 60)
        SMTP_DEBUG   ("1" para logar o diálogo SMTP completo)
    """
    if not SENDER_EMAIL or not APP_PASSWORD:
        print("Error: EMAIL_SENDER/SENDER_EMAIL e SENDER_PASSWORD/APP_PASSWORD "
              "não encontrados nas variáveis de ambiente.")
        return

    if not TO_EMAIL:
        print("Error: EMAIL_RECEIVER/TO_EMAIL não encontrado nas variáveis de ambiente.")
        return

    # suporta múltiplos destinatários separados por ";"
    destinatarios = [e.strip() for e in TO_EMAIL.split(";") if e.strip()]

    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = f"Logs <{SENDER_EMAIL}>"
    msg["To"] = ", ".join(destinatarios)

    try:
        context = ssl.create_default_context()

        if SMTP_USE_SSL:
            # SSL implícito (ex.: emailemnuvem.com.br na porta 465).
            # Alguns provedores rejeitam conexões que iniciam em texto
            # plano mesmo com STARTTLS em seguida, exigindo SSL desde
            # o handshake inicial.
            server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT, context=context)
        else:
            # STARTTLS (ex.: Gmail na porta 587).
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT)

        try:
            if SMTP_DEBUG:
                server.set_debuglevel(1)

            server.ehlo()
            if not SMTP_USE_SSL:
                server.starttls(context=context)
                server.ehlo()

            server.login(SENDER_EMAIL, APP_PASSWORD)

            resultado = server.sendmail(SENDER_EMAIL, destinatarios, msg.as_string())

            if resultado:
                # sendmail retorna um dict apenas para os destinatários que falharam
                print(f"Falha parcial no envio para: {resultado}")
            else:
                print(f"Email enviado com sucesso para {destinatarios}")
        finally:
            server.quit()

    except Exception as e:
        print(f"Falha ao enviar e-mail. Erro: {e}")


if __name__ == "__main__":
    test_subject = "[FALHA ENGENHARIA] ZeroUm - Mensagem de teste ETL"
    test_body = "ZeroUm - Isso é um teste e não deve ser considerado."

    try:
        with open('etl.log', 'r', encoding='latin-1') as log_file:
            linhas = log_file.readlines()
            ultimos_logs = "".join(linhas[-30:])
            test_body += f"\n\n=== HISTÓRICO DO LOGGER (TESTE) ===\n{ultimos_logs}"
    except Exception as e:
        test_body += f"\n\n(Não foi possível carregar o etl.log: {e})"

    send_email(test_subject, test_body)