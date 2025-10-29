#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para atualizar um DMO de destino com campos de um DMO de origem.

Este script lê um arquivo CSV para identificar um DMO de origem e um DMO de destino.
Ele então:
1. Obtém a definição de ambos os DMOs.
2. Identifica os campos do DMO de origem que NÃO existem no DMO de destino.
3. Aplica a mesma lógica de transformação de nome de campo do script original (removendo prefixos/sufixos).
4. Adiciona os novos campos ao DMO de destino usando uma requisição PATCH.

Baseado no 'cloneNmappingDMO.py' original, reutilizando a autenticação JWT
e as configurações de ambiente.
"""

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
import warnings

# --- 1. Configuração e Constantes ---
load_dotenv()

# Desabilitar avisos de SSL se VERIFY_SSL for False
if os.getenv("VERIFY_SSL", "False").lower() == "false":
    warnings.filterwarnings('ignore', message='Unverified HTTPS request')

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

# --- Modo de Operação ---
DRY_RUN = os.getenv("DRY_RUN", "True").lower() == "true"
"""
Se 'True', o script apenas listará os campos que *seriam* adicionados, 
sem enviar a requisição PATCH. 
Defina como "False" no .env para executar as atualizações.
"""

def get_timestamp():
    """Retorna o timestamp atual formatado para logs."""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# --- 2. Funções de API (Autenticação e GET) ---
#    (Reutilizadas do script original)

def authenticate_jwt(login_url, client_id, username, private_key_file):
    """Autentica no Salesforce usando o fluxo JWT Bearer."""
    print(f"{get_timestamp()} 🔐  Iniciando autenticação JWT...")
    try:
        with open(private_key_file, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(), password=None, backend=default_backend()
            )
        
        # Expiração de 3 minutos (180 segundos)
        payload = {
            "iss": client_id,
            "sub": username,
            "aud": login_url,
            "exp": int(time.time()) + 180
        }
        
        token = jwt.encode(payload, private_key, algorithm="RS256")
        
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": token
        }
        
        auth_url = f"{login_url}/services/oauth2/token"
        
        response = requests.post(
            auth_url, 
            data=data, 
            proxies=proxies, 
            verify=VERIFY_SSL, 
            timeout=REQUESTS_TIMEOUT
        )
        response.raise_for_status()  # Lança exceção para status HTTP 4xx/5xx
        
        auth_data = response.json()
        print(f"{get_timestamp()} ✅  Autenticação bem-sucedida!")
        return auth_data.get('access_token'), auth_data.get('instance_url')
        
    except FileNotFoundError:
        print(f"{get_timestamp()} ❌ ERRO DE AUTENTICAÇÃO: Arquivo de chave privada '{private_key_file}' não encontrado.")
        return None, None
    except Exception as e:
        print(f"{get_timestamp()} ❌ ERRO DE AUTENTICAÇÃO: {e}")
        return None, None

def get_dmo_definition(access_token, instance_url, dmo_name):
    """Busca a definição completa de um DMO específico."""
    get_url = f"{instance_url}/services/data/{API_VERSION}/ssot/data-model-objects/{dmo_name}"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    try:
        response = requests.get(
            get_url, 
            headers=headers, 
            proxies=proxies, 
            verify=VERIFY_SSL, 
            timeout=REQUESTS_TIMEOUT
        )
        response.raise_for_status()
        print(f"{get_timestamp()}    - GET DMO Definition bem-sucedido para '{dmo_name}'")
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"{get_timestamp()}    - ❌ ERRO no GET DMO para '{dmo_name}': {e.response.status_code} - {e.response.text}")
        return None

# --- 3. Função de API (PATCH) ---
#    (Nova função para esta tarefa)

def update_dmo_fields(access_token, instance_url, target_dmo_name, fields_to_add):
    """
    Adiciona uma lista de novos campos a um DMO existente via PATCH.
    """
    patch_url = f"{instance_url}/services/data/{API_VERSION}/ssot/data-model-objects/{target_dmo_name}"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    
    # O payload para PATCH de adição de campos é {"fields": [...]}
    patch_payload = {"fields": fields_to_add}
    
    print(f"{get_timestamp()}    - Enviando PATCH para adicionar {len(fields_to_add)} campos a '{target_dmo_name}'...")
    
    try:
        response = requests.patch(
            patch_url, 
            headers=headers, 
            data=json.dumps(patch_payload), 
            proxies=proxies, 
            verify=VERIFY_SSL, 
            timeout=REQUESTS_TIMEOUT
        )
        response.raise_for_status()
        print(f"{get_timestamp()}    - ✅ PATCH bem-sucedido! Campos adicionados a '{target_dmo_name}'.")
        return True
    except requests.exceptions.HTTPError as e:
        print(f"{get_timestamp()}    - ❌ ERRO no PATCH DMO para '{target_dmo_name}': {e.response.status_code} - {e.response.text}")
        return False

# --- 4. Lógica de Transformação de Campo ---

def transform_field_from_source(source_field_def):
    """
    Transforma uma definição de campo de origem no formato de payload 
    para criação/atualização.
    
    Aplica a mesma lógica de 'create_new_dmo' do script original.
    """
    # 1. Aplicar lógica de filtro (ignorar campos de sistema não-PK)
    if source_field_def.get('creationType') == 'System' and not source_field_def.get('isPrimaryKey', False):
        return None, None

    # 2. Aplicar lógica de transformação de nome
    original_api_name = source_field_def.get('name', '')
    transformed_name = original_api_name
    
    if transformed_name.startswith('ssot__'):
        transformed_name = transformed_name.replace('ssot__', '', 1)
    else:
        if transformed_name.endswith('__c'):
            transformed_name = transformed_name[:-3]

    if not transformed_name:
        return None, None # Ignora campos sem nome

    # 3. Construir o payload do novo campo
    new_field_payload = {
        "name": transformed_name,
        "label": source_field_def.get('label', transformed_name), # Garante um label
        "description": source_field_def.get('description', ''),
        "isPrimaryKey": source_field_def.get('isPrimaryKey', False),
        "isDynamicLookup": False, # Padrão do script original
        "dataType": source_field_def.get('type')
    }
    
    return transformed_name, new_field_payload

# --- 5. Orquestração Principal ---
def main():
    print("\n" + "="*50)
    print(f"{get_timestamp()} 🚀 Iniciando script de ADIÇÃO DE CAMPOS em DMO...")
    if DRY_RUN:
        print("    - ⚠️  Modo DRY RUN está ATIVADO. Nenhuma alteração real será feita.")
    else:
        print("    - 🔥  Modo de EXECUÇÃO. Alterações REAIS serão enviadas via PATCH.")
    print("="*50)

    access_token, instance_url = authenticate_jwt(
        SF_LOGIN_URL, SF_CLIENT_ID, SF_USERNAME, SF_PRIVATE_KEY_FILE
    )
    if not all([access_token, instance_url]):
        print(f"{get_timestamp()} 🚫  A execução não pode continuar devido à falha na autenticação.")
        return

    # --- Leitura e Validação do CSV ---
    try:
        dmo_df = pd.read_csv(INPUT_CSV_FILE)
        required_cols = ['SourceDmoName', 'TargetDmoName']
        if not all(col in dmo_df.columns for col in required_cols):
            print(f"{get_timestamp()} ❌ ERRO: O arquivo '{INPUT_CSV_FILE}' deve conter as colunas: {', '.join(required_cols)}")
            return
        
        dmo_df.dropna(subset=required_cols, inplace=True)
        print(f"\n{get_timestamp()} 📄 Arquivo '{INPUT_CSV_FILE}' carregado. {len(dmo_df)} tarefas de atualização para processar.")
    
    except FileNotFoundError:
        print(f"{get_timestamp()} ❌ ERRO: Arquivo '{INPUT_CSV_FILE}' não encontrado.")
        return
    except Exception as e:
        print(f"{get_timestamp()} ❌ ERRO ao ler CSV: {e}")
        return

    success_count = 0
    failure_count = 0
    
    # --- Loop de Processamento ---
    for _, row in tqdm(dmo_df.iterrows(), total=dmo_df.shape[0], desc=f"{get_timestamp()} Processando DMOs"):
        
        source_dmo_name = row['SourceDmoName']
        target_dmo_name = row['TargetDmoName']
        
        print(f"\n{get_timestamp()} 🔄 Iniciando tarefa: Adicionar campos de '{source_dmo_name}' para '{target_dmo_name}'")
        
        # 1. Obter definição do DMO de Origem
        source_def = get_dmo_definition(access_token, instance_url, source_dmo_name)
        if not source_def:
            print(f"{get_timestamp()}    - ❌ ERRO: Não foi possível obter a definição de origem '{source_dmo_name}'. Pulando esta tarefa.")
            failure_count += 1
            continue
            
        # 2. Obter definição do DMO de Destino
        target_def = get_dmo_definition(access_token, instance_url, target_dmo_name)
        if not target_def:
            print(f"{get_timestamp()}    - ❌ ERRO: Não foi possível obter a definição de destino '{target_dmo_name}'. Pulando esta tarefa.")
            failure_count += 1
            continue
            
        # 3. Identificar campos existentes no Destino
        #    O nome do campo na definição (ex: 'FirstName') é o que usamos para comparar.
        target_existing_field_names = {
            f.get('name') for f in target_def.get('fields', [])
        }
        print(f"{get_timestamp()}    - '{target_dmo_name}' possui atualmente {len(target_existing_field_names)} campos.")

        # 4. Identificar campos novos para adicionar
        fields_to_add = []
        source_fields = source_def.get('fields', [])
        
        for source_field in source_fields:
            # Aplica a mesma lógica de transformação/filtro do script original
            transformed_name, new_field_payload = transform_field_from_source(source_field)
            
            if not transformed_name:
                # Campo foi filtrado (ex: campo de sistema não-PK)
                continue
            
            # Verificar se o campo (pelo nome transformado) já existe no destino
            if transformed_name in target_existing_field_names:
                # print(f"{get_timestamp()}    - Campo '{transformed_name}' já existe. Pulando.")
                pass
            else:
                print(f"{get_timestamp()}    - ➕ Campo novo identificado: '{transformed_name}' (Tipo: {new_field_payload['dataType']})")
                fields_to_add.append(new_field_payload)

        # 5. Executar a atualização (ou simular em DRY_RUN)
        if not fields_to_add:
            print(f"{get_timestamp()}    - ✅ Nenhum campo novo para adicionar. Tarefa concluída.")
            success_count += 1
            continue
            
        print(f"{get_timestamp()}    - {len(fields_to_add)} campos novos serão adicionados a '{target_dmo_name}'.")

        if DRY_RUN:
            print(f"{get_timestamp()}    - [DRY RUN] Simulação de PATCH concluída.")
            # Opcional: printar os campos que seriam adicionados
            # for f in fields_to_add:
            #     print(f"      - {f['name']} ({f['dataType']})")
            success_count += 1
        else:
            # Modo de execução: Enviar o PATCH
            success = update_dmo_fields(access_token, instance_url, target_dmo_name, fields_to_add)
            if success:
                success_count += 1
            else:
                failure_count += 1

    # --- Resumo Final ---
    print("\n" + "="*50)
    print(f"{get_timestamp()} 🎉 Processo de atualização de campos concluído!")
    print(f"  - Total de tarefas na lista: {len(dmo_df)}")
    print(f"  - ✅ Tarefas concluídas com sucesso: {success_count}")
    print(f"  - ❌ Tarefas com falha: {failure_count}")
    print("="*50)

if __name__ == "__main__":
    main()