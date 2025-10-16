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
        return auth_data.get('access_token'), auth_data.get('instance_url')

    except FileNotFoundError:
        print(f"{get_timestamp()} ❌ ERRO DE AUTENTICAÇÃO: Arquivo de chave privada não encontrado em '{private_key_file}'")
    except Exception as e:
        print(f"{get_timestamp()} ❌ ERRO DE AUTENTICAÇÃO: {e}")
    
    return None, None

# --- 3. Lógica da API ---

def get_dmo_definition(access_token, instance_url, dmo_name):
    """Busca a definição completa de um DMO via GET request."""
    get_url = f"{instance_url}/services/data/{API_VERSION}/ssot/data-model-objects/{dmo_name}"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    try:
        response = requests.get(get_url, headers=headers, proxies=proxies, verify=VERIFY_SSL)
        response.raise_for_status()
        print(f"{get_timestamp()}    - GET bem-sucedido para {dmo_name}")
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"{get_timestamp()}    - ❌ ERRO no GET para {dmo_name}: {e.response.status_code} - {e.response.text}")
        return None

def transform_payload_for_post(get_payload):
    """Transforma o payload do GET para o formato exigido pelo POST."""
    if not get_payload:
        return None

    original_label = get_payload.get('label', '')
    original_api_name = get_payload.get('name', '')
    
    base_api_name = original_api_name.replace('__dlm', '')

    post_payload = {
        # Mantém o prefixo no 'name' (API Name)
        "name": f"{NEW_DMO_PREFIX}{base_api_name}",
        # Remove o prefixo do 'label', usando apenas o valor original.
        "label": original_label,
        "description": get_payload.get('description', ''),
        "dataSpaceName": NEW_DATA_SPACE_NAME,
        "category": get_payload.get('category', 'OTHER'),
        "fields": []
    }
    
    for field in get_payload.get('fields', []):
        if field.get('creationType') == 'System':
            continue
        
        new_field = {
            "name": field.get('name', '').replace('__c', ''),
            "label": field.get('label', ''),
            "description": field.get('description', ''),
            "isPrimaryKey": field.get('isPrimaryKey', False),
            "isDynamicLookup": False,
            "dataType": field.get('type')
        }
        post_payload["fields"].append(new_field)
        
    return post_payload

def create_new_dmo(access_token, instance_url, post_payload):
    """Cria um novo DMO via POST request."""
    post_url = f"{instance_url}/services/data/{API_VERSION}/ssot/data-model-objects"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(post_url, headers=headers, data=json.dumps(post_payload), proxies=proxies, verify=VERIFY_SSL)
        response.raise_for_status()
        dmo_name = post_payload.get('name')
        print(f"{get_timestamp()}    - ✅ POST bem-sucedido! DMO '{dmo_name}' criado.")
        return True
    except requests.exceptions.HTTPError as e:
        dmo_name = post_payload.get('name')
        print(f"{get_timestamp()}    - ❌ ERRO no POST para '{dmo_name}': {e.response.status_code} - {e.response.text}")
        return False

# --- 4. Orquestração Principal ---

def main():
    """Função principal que orquestra todo o processo."""
    print("\n" + "="*50)
    print(f"{get_timestamp()} 🚀 Iniciando script de clonagem de DMOs...")
    print("="*50)
    
    access_token, instance_url = authenticate_jwt(SF_LOGIN_URL, SF_CLIENT_ID, SF_USERNAME, SF_PRIVATE_KEY_FILE)
    if not all([access_token, instance_url]):
        print(f"{get_timestamp()} 🚫  A execução não pode continuar devido à falha na autenticação.")
        return

    try:
        dmo_df = pd.read_csv(INPUT_CSV_FILE)
        if "DmoDeveloperName" not in dmo_df.columns:
            print(f"{get_timestamp()} ❌ ERRO: O arquivo '{INPUT_CSV_FILE}' deve conter uma coluna chamada 'DmoDeveloperName'.")
            return
        dmo_list = dmo_df["DmoDeveloperName"].dropna().unique().tolist()
        print(f"\n{get_timestamp()} 📄 Arquivo '{INPUT_CSV_FILE}' carregado. {len(dmo_list)} DMOs únicos para processar.")
    except FileNotFoundError:
        print(f"{get_timestamp()} ❌ ERRO: Arquivo '{INPUT_CSV_FILE}' não encontrado.")
        return

    success_count = 0
    failure_count = 0
    
    for dmo_name in tqdm(dmo_list, desc=f"{get_timestamp()} Processando DMOs"):
        print(f"\n{get_timestamp()} 🔄 Iniciando processamento para: {dmo_name}")
        
        dmo_definition = get_dmo_definition(access_token, instance_url, dmo_name)
        if not dmo_definition:
            failure_count += 1
            continue
            
        new_dmo_payload = transform_payload_for_post(dmo_definition)
        if not new_dmo_payload:
            print(f"{get_timestamp()}    - ❌ ERRO: Falha ao transformar o payload para {dmo_name}.")
            failure_count += 1
            continue

        if create_new_dmo(access_token, instance_url, new_dmo_payload):
            success_count += 1
        else:
            failure_count += 1

    print("\n" + "="*50)
    print(f"{get_timestamp()} 🎉 Processo de clonagem concluído!")
    print(f"  - Total de DMOs processados: {len(dmo_list)}")
    print(f"  - ✅ Sucessos: {success_count}")
    print(f"  - ❌ Falhas: {failure_count}")
    print("="*50)

if __name__ == "__main__":
    main()