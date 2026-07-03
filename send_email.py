import smtplib
from email.message import EmailMessage
import os
from dotenv import load_dotenv


def send_email(subject, body):
    """
    Envia e-mail de alerta via SMTP Email em Nuvem.
    Lê as credenciais diretamente do .env — sem dependência de config.py.

    Variáveis esperadas no .env:
        EMAIL_SENDER     : remetente (ex: logs.guvi@dataguvi.com.br)
        SENDER_PASSWORD  : senha ou app password do remetente
        EMAIL_RECEIVER   : destinatário (ex: dataguvi@gmail.com)
    """
    load_dotenv()

    sender_email = os.getenv("EMAIL_SENDER")
    app_password = os.getenv("SENDER_PASSWORD")
    to_email     = os.getenv("EMAIL_RECEIVER")

    if not sender_email or not app_password:
        print("Error: EMAIL_SENDER e SENDER_PASSWORD não encontrados no .env.")
        return

    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From']    = f"Logs <{sender_email}>"
    msg['To']      = to_email

    try:
        with smtplib.SMTP_SSL('smtp.emailemnuvem.com.br', 465) as smtp:
            smtp.login(sender_email, app_password)
            smtp.send_message(msg)
            print(f"Email enviado com sucesso para {to_email}")
    except Exception as e:
        print(f"Falha ao enviar e-mail. Erro: {e}")


if __name__ == "__main__":
    test_subject = "[FALHA ENGENHARIA] ZeroUm - Mensagem de teste ETL"
    test_body    = "ZeroUm - Isso é um teste e não deve ser considerado."

    try:
        with open('etl.log', 'r', encoding='latin-1') as log_file:
            linhas      = log_file.readlines()
            ultimos_logs = "".join(linhas[-30:])
            test_body   += f"\n\n=== HISTÓRICO DO LOGGER (TESTE) ===\n{ultimos_logs}"
    except Exception as e:
        test_body += f"\n\n(Não foi possível carregar o etl.log: {e})"

    send_email(test_subject, test_body)
