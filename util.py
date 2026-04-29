from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from Crypto.Random import get_random_bytes
from config import AES_KEY_ZEROUM, AES_IV_ZEROUM, AES_KEY_ENERGIABET, AES_IV_ENERGIABET

class Util:
    def descriptografar(self, cliente, texto):
        key = AES_KEY_ZEROUM if cliente == "ZEROUM" else AES_KEY_ENERGIABET
        iv = AES_IV_ZEROUM if cliente == "ZEROUM" else AES_IV_ENERGIABET
        cipher_dec = AES.new(key, AES.MODE_CBC, iv)
        decrypted_padded_data = cipher_dec.decrypt(texto)

        decrypted_data = unpad(decrypted_padded_data, AES.block_size)

        print(f"Ciphertext: {texto}")
        print(f"Decrypted Data: {decrypted_data.decode('utf-8')}")