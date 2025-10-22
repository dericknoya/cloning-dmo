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
REQUESTS_TIMEOUT = 90 

# --- Modos de Operação ---
RUN_CLONE_DMO = os.getenv("RUN_CLONE_DMO", "True").lower() == "true"
RUN_CREATE_MAPPING = os.getenv("RUN_CREATE_MAPPING", "True").lower() == "true"

def get_timestamp():
    """Retorna o timestamp atual formatado para logs."""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# --- Funções de API ---
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

def create_new_dmo(access_token, instance_url, get_payload, new_dmo_name_base, new_data_space):
    if not get_payload: return None, False
    original_label = get_payload.get('label', '')

    post_payload = {
        "name": new_dmo_name_base,
        "label": original_label, 
        "description": get_payload.get('description', ''),
        "dataSpaceName": new_data_space,
        "category": get_payload.get('category', 'OTHER'),
        "fields": []
    }
    
    # Esta é a lógica de filtro que devemos replicar no mapeamento
    for field in get_payload.get('fields', []):
        if field.get('creationType') == 'System': 
            continue
        post_payload["fields"].append({"name": field.get('name', '').replace('__c', ''), "label": field.get('label', ''), "description": field.get('description', ''), "isPrimaryKey": field.get('isPrimaryKey', False), "isDynamicLookup": False, "dataType": field.get('type')})
    
    post_url = f"{instance_url}/services/data/{API_VERSION}/ssot/data-model-objects"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    
    try:
        response = requests.post(post_url, headers=headers, data=json.dumps(post_payload), proxies=proxies, verify=VERIFY_SSL, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        print(f"{get_timestamp()}    - ✅ POST DMO bem-sucedido! DMO '{new_dmo_name_base}' criado em '{new_data_space}'.")
        return new_dmo_name_base, True
    except requests.exceptions.HTTPError as e:
        print(f"{get_timestamp()}    - ❌ ERRO no POST DMO para '{new_dmo_name_base}': {e.response.status_code} - {e.response.text}")
        return new_dmo_name_base, False

def get_dmo_mappings(access_token, instance_url, original_dmo_name, source_data_space):
    print(f"{get_timestamp()}    - Buscando mapeamentos para DMO '{original_dmo_name}' no Data Space '{source_data_space}'")
    get_url = f"{instance_url}/services/data/{API_VERSION}/ssot/data-model-object-mappings?dataspace={source_data_space}&dmoDeveloperName={original_dmo_name}"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(get_url, headers=headers, proxies=proxies, verify=VERIFY_SSL, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if data.get("objectSourceTargetMaps"):
            print(f"{get_timestamp()}    - {len(data['objectSourceTargetMaps'])} definições de mapeamento encontradas.")
            return data["objectSourceTargetMaps"]
        else:
            print(f"{get_timestamp()}    - Nenhum mapeamento encontrado para {original_dmo_name} em {source_data_space}.")
            return []
    except requests.exceptions.HTTPError as e:
        print(f"{get_timestamp()}    - ❌ ERRO no GET Mappings para {original_dmo_name}: {e.response.status_code} - {e.response.text}")
        return None

# AJUSTE: Adicionado o parâmetro 'system_target_fields_to_skip'
def create_new_mappings(access_token, instance_url, original_mappings, new_dmo_name_base, new_data_space, system_target_fields_to_skip):
    if not original_mappings: return True

    consolidated_mappings = {}
    for mapping in original_mappings:
        dlo_name = mapping.get("sourceEntityDeveloperName")
        if not dlo_name: continue
        fields = mapping.get("fieldMappings", [])
        if dlo_name not in consolidated_mappings:
            consolidated_mappings[dlo_name] = []
        consolidated_mappings[dlo_name].extend(fields)
    
    print(f"{get_timestamp()}    - {len(consolidated_mappings)} DLO(s) de origem únicos para mapear.")

    post_url = f"{instance_url}/services/data/{API_VERSION}/ssot/data-model-object-mappings?dataspace={new_data_space}"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    all_successful = True

    for dlo_name, all_fields in consolidated_mappings.items():
        
        # AJUSTE: Filtra os campos de sistema (baseado no DMO de origem)
        filtered_fields = [
            {"sourceFieldDeveloperName": f["sourceFieldDeveloperName"], "targetFieldDeveloperName": f["targetFieldDeveloperName"]}
            for f in all_fields
            if f.get("sourceFieldDeveloperName") and f.get("targetFieldDeveloperName") not in system_target_fields_to_skip
        ]
        
        unique_filtered_fields = [dict(t) for t in {tuple(d.items()) for d in filtered_fields}]

        if not unique_filtered_fields:
            print(f"{get_timestamp()}    - Nenhum campo a ser mapeado para DLO '{dlo_name}' (após filtrar campos de sistema).")
            continue
        
        post_payload = {"sourceEntityDeveloperName": dlo_name, "targetEntityDeveloperName": f"{new_dmo_name_base}__dlm", "fieldMapping": unique_filtered_fields}
        
        try:
            response = requests.post(post_url, headers=headers, data=json.dumps(post_payload), proxies=proxies, verify=VERIFY_SSL, timeout=REQUESTS_TIMEOUT)
            response.raise_for_status()
            print(f"{get_timestamp()}    - ✅ POST Mapping bem-sucedido para DLO: {dlo_name}")
        except requests.exceptions.HTTPError as e:
            print(f"{get_timestamp()}    - ❌ ERRO no POST Mapping para DLO '{dlo_name}': {e.response.status_code} - {e.response.text}")
            all_successful = False
            
    return all_successful

# --- 5. Orquestração Principal ---
def main():
    print("\n" + "="*50)
    print(f"{get_timestamp()} 🚀 Iniciando script de clonagem universal...")
    print(f"    - Modo Clonar DMO: {'ATIVADO' if RUN_CLONE_DMO else 'DESATIVADO'}")
    print(f"    - Modo Criar Mapeamento: {'ATIVADO' if RUN_CREATE_MAPPING else 'DESATIVADO'}")
    print("="*50)

    # ... (autenticação) ...
    access_token, instance_url = authenticate_jwt(SF_LOGIN_URL, SF_CLIENT_ID, SF_USERNAME, SF_PRIVATE_KEY_FILE)
    if not all([access_token, instance_url]):
        print(f"{get_timestamp()} 🚫  A execução não pode continuar devido à falha na autenticação.")
        return

    # ... (leitura do CSV) ...
    try:
        dmo_df = pd.read_csv(INPUT_CSV_FILE)
        required_cols = ['SourceDmoName', 'SourceDataSpace', 'TargetDmoName', 'TargetDataSpace']
        if not all(col in dmo_df.columns for col in required_cols):
            print(f"{get_timestamp()} ❌ ERRO: O arquivo '{INPUT_CSV_FILE}' deve conter as colunas: {', '.join(required_cols)}")
            return
        dmo_df.dropna(subset=required_cols, inplace=True)
        print(f"\n{get_timestamp()} 📄 Arquivo '{INPUT_CSV_FILE}' carregado. {len(dmo_df)} tarefas de clonagem para processar.")
    except FileNotFoundError:
        print(f"{get_timestamp()} ❌ ERRO: Arquivo '{INPUT_CSV_FILE}' não encontrado.")
        return
    except Exception as e:
        print(f"{get_timestamp()} ❌ ERRO ao ler CSV: {e}")
        return

    success_count = 0
    failure_count = 0
    
    for _, row in tqdm(dmo_df.iterrows(), total=dmo_df.shape[0], desc=f"{get_timestamp()} Processando DMOs"):
        
        original_dmo_name = row['SourceDmoName']
        source_data_space = row['SourceDataSpace']
        new_dmo_name_base = row['TargetDmoName'].replace('__dlm', '') 
        new_data_space = row['TargetDataSpace']
        
        print(f"\n{get_timestamp()} 🔄 Iniciando tarefa: Clonar '{original_dmo_name}' ({source_data_space}) para '{new_dmo_name_base}' ({new_data_space})")
        
        dmo_succeeded = False
        mapping_succeeded = False
        system_target_fields_to_skip = set()

        # AJUSTE: Precisamos da definição do DMO de origem em ambos os modos
        # para saber quais campos de sistema pular no mapeamento.
        dmo_definition = get_dmo_definition(access_token, instance_url, original_dmo_name)
        if not dmo_definition:
            print(f"{get_timestamp()}    - ❌ ERRO: Não foi possível obter a definição do DMO de origem '{original_dmo_name}'. Pulando esta tarefa.")
            failure_count += 1
            continue
        
        # Extrai a lista de campos de sistema do DMO de origem
        system_target_fields_to_skip = {f['name'] for f in dmo_definition.get('fields', []) if f.get('creationType') == 'System'}
        print(f"{get_timestamp()}    - (Identificados {len(system_target_fields_to_skip)} campos de sistema para ignorar no mapeamento)")

        if RUN_CLONE_DMO:
            new_dmo_name, created = create_new_dmo(access_token, instance_url, dmo_definition, new_dmo_name_base, new_data_space)
            dmo_succeeded = created
        else:
            dmo_succeeded = True
            print(f"{get_timestamp()}    - ⏩ Clonagem de DMO pulada. Assumindo que o DMO de destino '{new_dmo_name_base}' já existe.")

        if dmo_succeeded and RUN_CREATE_MAPPING:
            original_mappings = get_dmo_mappings(access_token, instance_url, original_dmo_name, source_data_space)
            if original_mappings is not None:
                # Passa a lista de campos de sistema para a função de mapeamento
                mapping_succeeded = create_new_mappings(access_token, instance_url, original_mappings, new_dmo_name_base, new_data_space, system_target_fields_to_skip)
            else:
                mapping_succeeded = False
        elif not RUN_CREATE_MAPPING:
            mapping_succeeded = True

        process_ok = (not RUN_CLONE_DMO or dmo_succeeded) and \
                     (not RUN_CREATE_MAPPING or mapping_succeeded)

        if process_ok:
            success_count += 1
        else:
            failure_count += 1

    print("\n" + "="*50)
    print(f"{get_timestamp()} 🎉 Processo concluído!")
    print(f"  - Total de tarefas na lista: {len(dmo_df)}")
    print(f"  - ✅ Processos concluídos com sucesso: {success_count}")
    print(f"  - ❌ Processos com falha: {failure_count}")
    print("="*50)

if __name__ == "__main__":
    main()