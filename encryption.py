import base64
from Crypto.Cipher import AES

api_key = b'kknxYeSOYFLeZotEhLG'.rjust(32)
secret_key = b'9qw8as7zx6er5df4cv3ty2gh1bnuijkl'

cipher = AES.new(secret_key,AES.MODE_ECB) # never use ECB in strong systems obviously
encoded_api = base64.b64encode(cipher.encrypt(api_key))