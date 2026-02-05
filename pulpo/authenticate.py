import requests
import jwt
import time
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend

class Auth:
    def __init__(self, base_url, realm, client_id, client_secret):
        """
        Inicializa el cliente de autenticación Keycloak
        
        Args:
            base_url: URL base de Keycloak (ej: https://seguridad.merocomsolutions.com)
            realm: Nombre del realm
            client_id: ID del cliente
            client_secret: Secret del cliente
        """
        self.base_url = base_url
        self.realm = realm
        self.token_url = f"{base_url}/realms/{realm}/protocol/openid-connect/token"
        self.cert_url = f"{base_url}/realms/{realm}/protocol/openid-connect/certs"
        self.userinfo_url = f"{base_url}/realms/{realm}/protocol/openid-connect/userinfo"
        self.logout_url = f"{base_url}/realms/{realm}/protocol/openid-connect/logout"
        self.client_id = client_id
        self.client_secret = client_secret
        self.decoded_token = ""
        self.token = ""
        self.refresh_token = ""  # ✅ AÑADIDO: Guardar refresh token
        self.public_key = None
        self.otp = None

    def login(self, username, password, otp_code=None):
        """
        Realiza login con usuario, contraseña y OTP opcional
        
        Args:
            username: Nombre de usuario
            password: Contraseña
            otp_code: Código OTP/TOTP (opcional)
            
        Returns:
            dict: {"access_token": str, "decoded_token": dict, "refresh_token": str, "expires_in": int}
        """
        data = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": username,
            "password": password,
            "scope": "openid profile email",
        }

        # Si se proporciona un código OTP, lo añadimos a los datos de la solicitud
        self.otp = otp_code
        if otp_code:
            data['totp'] = otp_code

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.token_url, data=data, headers=headers)
        
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            refresh_token = token_data.get("refresh_token")  # ✅ AÑADIDO
            
            self.token = access_token
            self.refresh_token = refresh_token  # ✅ AÑADIDO: Guardar refresh token
            self.decoded_token = self.__decode_jwt(access_token)
            
            return {
                "access_token": self.token,
                "decoded_token": self.decoded_token,
                "refresh_token": self.refresh_token,  # ✅ AÑADIDO
                "expires_in": token_data.get("expires_in"),  # ✅ AÑADIDO
                "token_type": token_data.get("token_type", "Bearer")  # ✅ AÑADIDO
            }
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")

    def refresh_access_token(self):
        """
        ✅ NUEVO MÉTODO: Refresca el access token usando el refresh token
        
        Returns:
            dict: {"access_token": str, "decoded_token": dict, "refresh_token": str}
        """
        if not self.refresh_token:
            raise ValueError("No hay refresh token disponible. Debe hacer login primero.")

        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token
        }

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.token_url, data=data, headers=headers)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            refresh_token = token_data.get("refresh_token")
            
            self.token = access_token
            self.refresh_token = refresh_token
            self.decoded_token = self.__decode_jwt(access_token)
            
            return {
                "access_token": self.token,
                "decoded_token": self.decoded_token,
                "refresh_token": self.refresh_token,
                "expires_in": token_data.get("expires_in")
            }
        else:
            raise Exception(f"Error al refrescar token {response.status_code}: {response.text}")

    def logout(self):
        """
        ✅ NUEVO MÉTODO: Cierra sesión invalidando el refresh token
        
        Returns:
            bool: True si el logout fue exitoso
        """
        if not self.refresh_token:
            # No hay refresh token, simplemente limpiar tokens locales
            self.token = ""
            self.decoded_token = ""
            return True

        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token
        }

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.logout_url, data=data, headers=headers)
        
        # Limpiar tokens locales independientemente del resultado
        self.token = ""
        self.refresh_token = ""
        self.decoded_token = ""
        self.otp = None
        
        return response.status_code == 204

    def get_userinfo(self):
        """
        ✅ NUEVO MÉTODO: Obtiene información del usuario desde Keycloak
        
        Returns:
            dict: Información del usuario
        """
        if not self.token:
            raise ValueError("No hay token de acceso disponible")

        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(self.userinfo_url, headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error al obtener userinfo {response.status_code}: {response.text}")

    def is_token_expired(self, margin_seconds=30):
        """
        ✅ NUEVO MÉTODO: Verifica si el token actual está expirado
        
        Args:
            margin_seconds: Margen de segundos antes de la expiración (default: 30)
            
        Returns:
            bool: True si el token está expirado o a punto de expirar
        """
        if not self.decoded_token:
            return True
        
        exp = self.decoded_token.get("exp", 0)
        return exp < (time.time() + margin_seconds)

    def __decode_jwt(self, token):
        """Método privado para decodificar JWT sin validar firma"""
        try:
            # Si el token es un diccionario, extraemos el 'access_token'
            if isinstance(token, dict):
                token = token.get("access_token", "")
            
            # Convertimos a string si es necesario
            token = str(token)
            
            # Decodificamos el token
            return jwt.decode(token, options={"verify_signature": False})
        except Exception as e:
            raise Exception(f"Error al decodificar el token: {str(e)}")

    def validate_token(self, token=None, verify_signature=False):
        """
        ✅ MEJORADO: Valida un token con opción de verificar firma
        
        Args:
            token: Token a validar (usa self.token si no se proporciona)
            verify_signature: Si True, verifica la firma RSA (default: False)
            
        Returns:
            dict: Token decodificado si es válido
        """
        if token is None:
            token = self.token

        if not verify_signature:
            # Validación rápida sin verificar firma
            try:
                decoded = jwt.decode(token, options={"verify_signature": False})
                
                # Verificar expiración
                if decoded.get("exp", 0) < time.time():
                    raise Exception("El token ha expirado")
                
                return decoded
            except jwt.InvalidTokenError as e:
                raise Exception(f"Token inválido: {str(e)}")

        # Validación con firma
        try:
            # Si no has cargado la clave pública, obténla desde Keycloak
            if self.public_key is None:
                self.public_key = self.__get_public_key()

            # Decodificamos y validamos la firma
            decoded_token = jwt.decode(
                token, 
                self.public_key, 
                algorithms=["RS256"], 
                audience=self.client_id
            )

            # Verificamos si el token está expirado
            if decoded_token["exp"] < time.time():
                raise Exception("El token ha expirado")
            
            return decoded_token  # Token válido

        except jwt.ExpiredSignatureError:
            raise Exception("El token ha expirado")
        except jwt.InvalidTokenError as e:
            raise Exception(f"Token inválido: {str(e)}")

    def __get_public_key(self):
        """✅ CORREGIDO: Obtiene clave pública de Keycloak"""
        response = requests.get(self.cert_url)
        if response.status_code == 200:
            certs = response.json()
            keys = certs.get("keys", [])
            
            if not keys:
                raise Exception("No se encontraron claves públicas en Keycloak")
            
            cert_str = keys[0]["x5c"][0]
            
            # ✅ CORRECCIÓN PRINCIPAL: Usar \n real en lugar de \\n
            cert_pem = f"-----BEGIN CERTIFICATE-----\n{cert_str}\n-----END CERTIFICATE-----"
            
            cert_obj = load_pem_x509_certificate(cert_pem.encode(), default_backend())
            public_key = cert_obj.public_key()
            return public_key
        else:
            raise Exception(f"Error al obtener la clave pública: {response.status_code}")

    def exchange_token_from(self, source_auth):
        """
        ✅ MEJORADO: Intercambia un token de acceso por un nuevo token
        
        Args:
            source_auth: Objeto Auth con token válido del que intercambiar
            
        Returns:
            dict: {"access_token": str, "decoded_token": dict}
        """
        if not source_auth.token:
            raise ValueError("El Auth origen no tiene un token válido")

        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": source_auth.token,
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        }

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.token_url, data=data, headers=headers)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            
            if not access_token:
                raise ValueError("No se recibió access_token en la respuesta")

            self.token = access_token
            self.decoded_token = self.__decode_jwt(access_token)
            
            return {
                "access_token": self.token,
                "decoded_token": self.decoded_token,
                "expires_in": token_data.get("expires_in", 0)  # ✅ AÑADIDO
            }
        else:
            raise RuntimeError(f"Error {response.status_code}: {response.text}")

    def __repr__(self):
        """✅ NUEVO: Representación del objeto"""
        return f"Auth(realm={self.realm}, client_id={self.client_id}, has_token={bool(self.token)})"


# ============================================================================
# ✅ EJEMPLO DE USO MEJORADO
# ============================================================================
if __name__ == "__main__":
    
    print("🔐 EJEMPLO DE USO DE LA CLASE AUTH")
    print("=" * 70)
    
    # Configuración
    KEYCLOAK_URL = "https://seguridad.merocomsolutions.com"
    REALM = "master"  # o "CompAI"
    CLIENT_ID = "informes"
    CLIENT_SECRET = "M5DaowmNZJR4t6MrFxX27Y7CNTyxR0bC"
    USERNAME = "dos"
    PASSWORD = "dos"
    
    try:
        # === PASO 1: LOGIN CON OTP ===
        print("\n📱 PASO 1: Login con OTP")
        print("-" * 70)
        
        auth = Auth(KEYCLOAK_URL, REALM, CLIENT_ID, CLIENT_SECRET)
        
        otp = input("🔢 Código OTP: ")
        token_response = auth.login(USERNAME, PASSWORD, otp)
        
        print("✅ Login exitoso")
        print(f"   Usuario: {token_response['decoded_token']['preferred_username']}")
        print(f"   Expira en: {token_response['expires_in']}s")
        print(f"   Token: {token_response['access_token'][:50]}...")
        
        # === PASO 2: VALIDAR TOKEN (SIN FIRMA) ===
        print("\n🔍 PASO 2: Validar token (sin verificar firma)")
        print("-" * 70)
        
        try:
            valid_token = auth.validate_token(verify_signature=False)
            print(f"✅ Token válido")
            print(f"   Expira: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(valid_token['exp']))}")
        except Exception as e:
            print(f"❌ Error: {e}")
        
        # === PASO 3: OBTENER USERINFO ===
        print("\n👤 PASO 3: Obtener información del usuario")
        print("-" * 70)
        
        try:
            userinfo = auth.get_userinfo()
            print(f"✅ Userinfo obtenido:")
            print(f"   Email: {userinfo.get('email')}")
            print(f"   Email verificado: {userinfo.get('email_verified')}")
        except Exception as e:
            print(f"❌ Error: {e}")
        
        # === PASO 4: TOKEN EXCHANGE ===
        print("\n🔄 PASO 4: Token Exchange (mismo cliente)")
        print("-" * 70)
        
        auth2 = Auth(KEYCLOAK_URL, REALM, CLIENT_ID, CLIENT_SECRET)
        token_response2 = auth2.exchange_token_from(auth)
        
        print("✅ Token exchange exitoso")
        print(f"   Nuevo token: {token_response2['access_token'][:50]}...")
        
        # === PASO 5: REFRESCAR TOKEN ===
        print("\n🔄 PASO 5: Refrescar token")
        print("-" * 70)
        
        if auth.refresh_token:
            try:
                refreshed = auth.refresh_access_token()
                print("✅ Token refrescado")
                print(f"   Nuevo token: {refreshed['access_token'][:50]}...")
                print(f"   Expira en: {refreshed['expires_in']}s")
            except Exception as e:
                print(f"❌ Error: {e}")
        else:
            print("⚠️  No hay refresh token disponible")
        
        # === PASO 6: LOGOUT ===
        print("\n🚪 PASO 6: Logout")
        print("-" * 70)
        
        logout_success = auth.logout()
        if logout_success:
            print("✅ Logout exitoso")
            print(f"   Token limpiado: {not auth.token}")
        else:
            print("⚠️  Logout con advertencias")
        
        print("\n" + "=" * 70)
        print("✅ EJEMPLO COMPLETADO")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
