import requests
import jwt
import time
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend

class Auth:
    def __init__(self, base_url, realm, client_id, client_secret):
        self.token_url = f"{base_url}/realms/{realm}/protocol/openid-connect/token"
        self.cert_url = f"{base_url}/realms/{realm}/protocol/openid-connect/certs"
        self.client_id = client_id
        self.client_secret = client_secret
        self.decoded_token = ""
        self.token = ""
        self.public_key = None
        self.otp = None

    def login(self, username, password, otp_code=None):
        data = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": username,
            "password": password,
            "scope": "openid profile email",
        }

        self.otp = otp_code
        if otp_code:
            data["totp"] = otp_code

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.token_url, data=data, headers=headers)
        if response.status_code == 200:
            access_token = response.json().get("access_token")
            self.token = access_token
            self.decoded_token = self.__decode_jwt(access_token)
            return {
                "access_token": self.token,
                "decoded_token": self.decoded_token
            }
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")

    def __decode_jwt(self, token):
        try:
            if isinstance(token, dict):
                token = token.get("access_token", "")
            token = str(token)
            return jwt.decode(token, options={"verify_signature": False})
        except Exception as e:
            raise Exception(f"Error al decodificar el token: {str(e)}")

    def validate_token(self, token):
        try:
            if self.public_key is None:
                self.public_key = self.__get_public_key()

            decoded_token = jwt.decode(token, self.public_key, algorithms=["RS256"], audience=self.client_id)

            if decoded_token["exp"] < time.time():
                raise Exception("El token ha expirado")
            
            return decoded_token
        except jwt.ExpiredSignatureError:
            raise Exception("El token ha expirado")
        except jwt.InvalidTokenError as e:
            raise Exception(f"Token inválido: {str(e)}")

    def __get_public_key(self):
        response = requests.get(self.cert_url)
        if response.status_code == 200:
            certs = response.json()
            cert_str = certs["keys"][0]["x5c"][0]
            cert_pem = f"-----BEGIN CERTIFICATE-----\n{cert_str}\n-----END CERTIFICATE-----"
            cert_obj = load_pem_x509_certificate(cert_pem.encode(), default_backend())
            public_key = cert_obj.public_key()
            return public_key
        else:
            raise Exception(f"Error al obtener la clave pública: {response.status_code}")

    def exchange_token_from(self, au1: "Auth"):
        if not au1.token:
            raise ValueError("El Auth origen (au1) no tiene un token válido")

        response = requests.post(
            self.token_url,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "subject_token": au1.token,
                "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        if response.status_code == 200:
            access_token = response.json().get("access_token")
            if not access_token:
                raise ValueError("No se recibió access_token en la respuesta")

            self.token = access_token
            self.decoded_token = self.__decode_jwt(access_token)
            return {
                "access_token": self.token,
                "decoded_token": self.decoded_token
            }
        else:
            raise RuntimeError(f" Error {response.status_code}: {response.text}")
