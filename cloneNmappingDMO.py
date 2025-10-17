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
API_VERSION = "v64.0"
INPUT_CSV_FILE = "dmo_list.csv"
NEW_DATA_SPACE_NAME = 'IUBR'
NEW_DMO_PREFIX = "iub_"
SYSTEM_FIELDS_TO_EXCLUDE = ("DataSourceObject__c", "DataSource__c", "InternalOrganization__c")
REQUESTS_TIMEOUT = 90 

# --- Modos de Operação ---
RUN_CLONE_DMO = os.getenv("RUN_CLONE_DMO", "True").lower() == "true"
RUN_CREATE_MAPPING = os.getenv("RUN_CREATE_MAPPING", "True").lower() == "true"

def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# --- Funções de API (sem alterações) ---
def authenticate_jwt(login_url, client_id, username, private_key_file):
    print(f"{get_timestamp()} 🔐  Iniciando autenticação JWT...")
    try:
        with open(private_key_file, "rb") as key_file:
            private_key = serialization.load_pem_private_key(key_file.read(), password=None, backend=default_backend())
        payload = {"iss": client_id, "sub": username, "aud": login_url, "exp": int(time.time()) + 180}
        token = jwt.encode(payload, private_key, algorithm="RS256")
        data = {"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": token}
        auth_url = f"{login_url}/services/oauth2/token"
        response = requests.post(auth_url, data=data, proxies=proxies, verify=VERIFY_SSL, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        auth_data = response.json()
        print(f"{get_timestamp()} ✅  Autenticação bem-sucedida!")
        return auth_data.get('access_token'), auth_data.get('instance_url')
    except Exception as e:
        print(f"{get_timestamp()} ❌ ERRO DE AUTENTICAÇÃO: {e}")
        return None, None

def get_dmo_definition(access_token, instance_url, dmo_name):
    get_url = f"{instance_url}/services/data/{API_VERSION}/ssot/data-model-objects/{dmo_name}"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(get_url, headers=headers, proxies=proxies, verify=VERIFY_SSL, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        print(f"{get_timestamp()}    - GET DMO bem-sucedido para {dmo_name}")
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"{get_timestamp()}    - ❌ ERRO no GET DMO para {dmo_name}: {e.response.status_code} - {e.response.text}")
        return None

def create_new_dmo(access_token, instance_url, get_payload):
    if not get_payload: return None, False
    original_label = get_payload.get('label', '')
    original_api_name = get_payload.get('name', '')
    base_api_name = original_api_name.replace('__dlm', '')
    post_payload = {"name": f"{NEW_DMO_PREFIX}{base_api_name}", "label": original_label, "description": get_payload.get('description', ''), "dataSpaceName": NEW_DATA_SPACE_NAME, "category": get_payload.get('category', 'OTHER'), "fields": []}
    for field in get_payload.get('fields', []):
        if field.get('creationType') == 'System': continue
        post_payload["fields"].append({"name": field.get('name', '').replace('__c', ''), "label": field.get('label', ''), "description": field.get('description', ''), "isPrimaryKey": field.get('isPrimaryKey', False), "isDynamicLookup": False, "dataType": field.get('type')})
    post_url = f"{instance_url}/services/data/{API_VERSION}/ssot/data-model-objects"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    try:
        response = requests.post(post_url, headers=headers, data=json.dumps(post_payload), proxies=proxies, verify=VERIFY_SSL, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        new_dmo_name = post_payload.get('name')
        print(f"{get_timestamp()}    - ✅ POST DMO bem-sucedido! DMO '{new_dmo_name}' criado.")
        return new_dmo_name, True
    except requests.exceptions.HTTPError as e:
        dmo_name = post_payload.get('name')
        print(f"{get_timestamp()}    - ❌ ERRO no POST DMO para '{dmo_name}': {e.response.status_code} - {e.response.text}")
        return dmo_name, False

def get_dmo_mappings(access_token, instance_url, original_dmo_name):
    print(f"{get_timestamp()}    - Buscando mapeamentos para o DMO original: {original_dmo_name}")
    get_url = f"{instance_url}/services/data/{API_VERSION}/ssot/data-model-object-mappings?dataspace=default&dmoDeveloperName={original_dmo_name}"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(get_url, headers=headers, proxies=proxies, verify=VERIFY_SSL, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if data.get("objectSourceTargetMaps"):
            print(f"{get_timestamp()}    - {len(data['objectSourceTargetMaps'])} mapeamento(s) encontrado(s).")
            return data["objectSourceTargetMaps"]
        else:
            print(f"{get_timestamp()}    - Nenhum mapeamento encontrado para {original_dmo_name}.")
            return []
    except requests.exceptions.HTTPError as e:
        print(f"{get_timestamp()}    - ❌ ERRO no GET Mappings para {original_dmo_name}: {e.response.status_code} - {e.response.text}")
        return None

def create_new_mappings(access_token, instance_url, original_mappings, new_dmo_name):
    if not original_mappings: return True
    post_url = f"{instance_url}/services/data/{API_VERSION}/ssot/data-model-object-mappings?dataspace={NEW_DATA_SPACE_NAME}"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    all_successful = True
    for mapping in original_mappings:
        filtered_fields = [{"sourceFieldDeveloperName": f["sourceFieldDeveloperName"], "targetFieldDeveloperName": f["targetFieldDeveloperName"]} for f in mapping.get("fieldMappings", []) if f["sourceFieldDeveloperName"] not in SYSTEM_FIELDS_TO_EXCLUDE]
        if not filtered_fields:
            print(f"{get_timestamp()}    - Nenhum campo a ser mapeado para DLO '{mapping.get('sourceEntityDeveloperName')}' após a filtragem.")
            continue
        post_payload = {"sourceEntityDeveloperName": mapping.get("sourceEntityDeveloperName"), "targetEntityDeveloperName": f"{new_dmo_name}__dlm", "fieldMapping": filtered_fields}
        try:
            response = requests.post(post_url, headers=headers, data=json.dumps(post_payload), proxies=proxies, verify=VERIFY_SSL, timeout=REQUESTS_TIMEOUT)
            response.raise_for_status()
            print(f"{get_timestamp()}    - ✅ POST Mapping bem-sucedido para DLO: {mapping.get('sourceEntityDeveloperName')}")
        except requests.exceptions.HTTPError as e:
            print(f"{get_timestamp()}    - ❌ ERRO no POST Mapping para DLO '{mapping.get('sourceEntityDeveloperName')}': {e.response.status_code} - {e.response.text}")
            all_successful = False
    return all_successful

# --- 5. Orquestração Principal (COM LÓGICA CONDICIONAL) ---
def main():
    """Função principal que orquestra todo o processo."""
    print("\n" + "="*50)
    print(f"{get_timestamp()} 🚀 Iniciando script...")
    print(f"    - Modo Clonar DMO: {'ATIVADO' if RUN_CLONE_DMO else 'DESATIVADO'}")
    print(f"    - Modo Criar Mapeamento: {'ATIVADO' if RUN_CREATE_MAPPING else 'DESATIVADO'}")
    print("="*50)

    if not RUN_CLONE_DMO and not RUN_CREATE_MAPPING:
        print(f"{get_timestamp()} ⚠️  Nenhum modo de operação foi ativado. Verifique as variáveis RUN_CLONE_DMO e RUN_CREATE_MAPPING no arquivo .env. Encerrando.")
        return

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
    
    for original_dmo_name in tqdm(dmo_list, desc=f"{get_timestamp()} Processando DMOs"):
        print(f"\n{get_timestamp()} 🔄 Iniciando processamento para: {original_dmo_name}")
        
        dmo_succeeded = False
        mapping_succeeded = False
        
        # --- ETAPA 1: Clonagem do DMO ---
        if RUN_CLONE_DMO:
            dmo_definition = get_dmo_definition(access_token, instance_url, original_dmo_name)
            if dmo_definition:
                new_dmo_name, created = create_new_dmo(access_token, instance_url, dmo_definition)
                dmo_succeeded = created
            else:
                dmo_succeeded = False
        else:
            # Se não clonamos, consideramos a etapa "bem-sucedida" para permitir a execução do mapeamento.
            dmo_succeeded = True
            base_api_name = original_dmo_name.replace('__dlm', '')
            new_dmo_name = f"{NEW_DMO_PREFIX}{base_api_name}"
            print(f"{get_timestamp()}    - ⏩ Clonagem de DMO pulada. Usando nome de DMO de destino: {new_dmo_name}")

        # --- ETAPA 2: Criação do Mapeamento ---
        if dmo_succeeded and RUN_CREATE_MAPPING:
            original_mappings = get_dmo_mappings(access_token, instance_url, original_dmo_name)
            if original_mappings is not None:
                mapping_succeeded = create_new_mappings(access_token, instance_url, original_mappings, new_dmo_name)
            else:
                # Erro ao buscar mapeamentos é uma falha na etapa
                mapping_succeeded = False
        elif not RUN_CREATE_MAPPING:
            mapping_succeeded = True # Se não executamos, consideramos sucesso.

        # --- Contabilização do Resultado Final ---
        if (RUN_CLONE_DMO and dmo_succeeded and not RUN_CREATE_MAPPING) or \
           (RUN_CREATE_MAPPING and mapping_succeeded and not RUN_CLONE_DMO) or \
           (RUN_CLONE_DMO and dmo_succeeded and RUN_CREATE_MAPPING and mapping_succeeded):
            success_count += 1
        else:
            failure_count += 1

    print("\n" + "="*50)
    print(f"{get_timestamp()} 🎉 Processo de clonagem concluído!")
    print(f"  - Total de DMOs na lista: {len(dmo_list)}")
    print(f"  - ✅ Processos concluídos com sucesso: {success_count}")
    print(f"  - ❌ Processos com falha: {failure_count}")
    print("="*50)

if __name__ == "__main__":
    main()