import base64
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from Crypto.Random import get_random_bytes
from config import AES_KEY_ZEROUM, AES_IV_ZEROUM, AES_KEY_ENERGIABET, AES_IV_ENERGIABET

class Util:
    def __init__(self, debug=False):
        self.debug = debug

    def descriptografar(self, cliente, texto):
        try:
            # valida nulo
            if texto is None:
                return None

            # seleciona chave/iv por cliente
            if cliente == "ZEROUM":
                key_raw = AES_KEY_ZEROUM
                iv_raw = AES_IV_ZEROUM
            else:
                key_raw = AES_KEY_ENERGIABET
                iv_raw = AES_IV_ENERGIABET

            # converte base64 → bytes  
            key = base64.b64decode(key_raw)
            iv = base64.b64decode(iv_raw)

            # debug tamanho
            if self.debug:
                print(f"[DEBUG] Key len: {len(key)} | IV len: {len(iv)}")

            # trata base64 com segurança

            if isinstance(texto, str):
                try:
                    # garante padding correto
                    missing_padding = len(texto) % 4
                    if missing_padding:
                        texto += '=' * (4 - missing_padding)

                    texto = base64.b64decode(texto)

                except Exception:
                    # se não for base64 válido, retorna original
                    if self.debug:
                        print(f"[DEBUG] Valor não é base64 válido: {texto}")
                    return texto

            # descriptografia
            cipher_dec = AES.new(key, AES.MODE_CBC, iv)

            # Protege erro de padding AES (quando dado não foi criptografado corretamente)

            try:
                decrypted_data = unpad(cipher_dec.decrypt(texto), AES.block_size)
            except Exception:
                if self.debug:
                    print(f"[DEBUG] Falha ao descriptografar (provavelmente não criptografado): {texto}")
                return texto

            resultado = decrypted_data.decode('utf-8')

            # 🔹 debug resultado
            if self.debug:
                print(f"[DEBUG] Resultado: {resultado}")

            return resultado

        except Exception as e:
            print(f"[ERRO DESCRIPTOGRAFIA] Valor: {texto} | Erro: {e}")
            return None