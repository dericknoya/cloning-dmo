import pandas as pd
from datetime import datetime
import os
import requests
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import time
from dotenv import load_dotenv
from tqdm import tqdm
import json

# --- 1. Configuração e Constantes ---
load_dotenv()
SF_LOGIN_URL = os.getenv("SF_LOGIN_URL", "https://login.salesforce.com")
SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PRIVATE_KEY_FILE = os.getenv("SF_PRIVATE_KEY_FILE", "private.pem")
USE_PROXY = os.getenv("USE_PROXY", "True").lower() == "true"
PROXY_URL = os.getenv("PROXY_URL")
VERIFY_SSL = os.getenv("VERIFY_SSL", "False").lower() == "true"
proxies = {'http': PROXY_URL, 'https': PROXY_URL} if USE_PROXY else None

# --- Constantes Específicas da Tarefa ---
API_VERSION = "v64.0" # Use a versão mais apropriada para seu ambiente
INPUT_CSV_FILE = "dmo_list.csv" # Nome do arquivo CSV de entrada
NEW_DATA_SPACE_NAME = 'IUBR'
NEW_DMO_PREFIX = "iub_"

def get_timestamp():
    """Retorna o timestamp atual formatado para logs."""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# --- 2. Autenticação ---
def authenticate_jwt(login_url, client_id, username, private_key_file):
    """Autentica usando o fluxo JWT e retorna o token de acesso e a URL da instância."""
    print(f"{get_timestamp()} 🔐  Iniciando autenticação JWT...")
    try:
        with open(private_key_file, "rb") as key_file:
            private_key = serialization.load_pem_private_key(key_file.read(), password=None, backend=default_backend())
        
        payload = {"iss": client_id, "sub": username, "aud": login_url, "exp": int(time.time()) + 180}
        token = jwt.encode(payload, private_key, algorithm="RS256")
        data = {"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": token}
        auth_url = f"{login_url}/services/oauth2/token"
        
        response = requests.post(auth_url, data=data, proxies=proxies, verify=VERIFY_SSL)
        response.raise_for_status()
        
        auth_data = response.json()
        print(f"{get_timestamp()} ✅  Autenticação bem-sucedida!")
        print(auth_data.get('access_token'))
        return auth_data.get('access_token'), auth_data.get('instance_url')

    except FileNotFoundError:
        print(f"{get_timestamp()} ❌ ERRO DE AUTENTICAÇÃO: Arquivo de chave privada não encontrado em '{private_key_file}'")
    except Exception as e:
        print(f"{get_timestamp()} ❌ ERRO DE AUTENTICAÇÃO: {e}")
    
    return None, None

authenticate_jwt(SF_LOGIN_URL, SF_CLIENT_ID, SF_USERNAME, SF_PRIVATE_KEY_FILE)