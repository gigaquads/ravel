from typing import Text
from base64 import b64decode, b64encode

from Crypto.Cipher import AES as AES_Cipher
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad, unpad

from ravel.util.uuid_util import random_uuid
from ravel.util.json_encoder import JsonEncoder


class AES:
    def __init__(self, key: Text = None, iv: bytes = None):
        self.json = JsonEncoder()
        self.iv = iv or get_random_bytes(16)
        self.key = (key or random_uuid().hex).encode('utf-8')

    def encrypt_CBC(self, value: object) -> Text:
        json_str = self.json.encode(value).encode('utf-8')
        cipher = AES_Cipher.new(self.key, AES_Cipher.MODE_CBC, self.iv)
        enc_bytes = cipher.encrypt(pad(json_str, AES_Cipher.block_size))
        return b64encode(enc_bytes).decode('utf-8')

    def decrypt_CBC(self, encrypted_value: Text) -> object:
        cipher = AES_Cipher.new(self.key, AES_Cipher.MODE_CBC, self.iv)
        enc_bytes = b64decode(encrypted_value.encode('utf-8'))
        json_str = unpad(cipher.decrypt(enc_bytes), AES_Cipher.block_size)
        return self.json.decode(json_str)