import smtplib
from email.message import EmailMessage
from config import SENDER_EMAIL, APP_PASSWORD, TO_EMAIL
import urllib.parse

def send_email(subject, body):

    # Parse the app password (using unquote to decode any URL-encoded characters)
    #app_password = urllib.parse.unquote(app_password)

    if not SENDER_EMAIL or not APP_PASSWORD:
        print("Error: Please set SENDER_EMAIL and EMAIL_APP_PASSWORD environment variables.")
        return

    print(SENDER_EMAIL)
    print(APP_PASSWORD)
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = f"Logs <{SENDER_EMAIL}>"
    msg['To'] = TO_EMAIL
    print('Sender name: {}'.format(msg['from'].addresses[0].display_name))

    try:
        # Using Email em Nuvem SMTP server.
        with smtplib.SMTP_SSL('smtp.emailemnuvem.com.br', 465) as smtp:
            smtp.login(SENDER_EMAIL, APP_PASSWORD)
            smtp.send_message(msg)
            print(f"Email sent successfully to {TO_EMAIL}")
    except Exception as e:
        print(f"Failed to send email. Error: {e}")

if __name__ == "__main__":
    # Example usage:
    # Make sure to set your environment variables before running:
    # $env:SENDER_EMAIL="logs.guvi@dataguvi.com.br"
    # $env:EMAIL_APP_PASSWORD="sua_senha_aqui"
    
    test_subject = "[FALHA ENGENHARIA] ZeroUm - Mensagem de teste ETL"
    test_body = "ZeroUm - Isso é um teste e não deve ser considerado (consume_api.py)."
    
    # Adicionando parte do logger no body do e-mail de teste
    try:
        with open('etl.log', 'r', encoding='latin') as log_file:
            linhas = log_file.readlines()
            ultimos_logs = "".join(linhas[-30:]) # Pegando as últimas 30 linhas
            test_body += f"\n\n=== HISTÓRICO DO LOGGER (TESTE) ===\n{ultimos_logs}"
    except Exception as e:
        test_body += f"\n\n(Não foi possível carregar o etl.log no teste: {e})"
 
    send_email(test_subject, test_body)
