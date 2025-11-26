import os
import requests
from bs4 import BeautifulSoup
import time
import logging
import locale
import json
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, '')
    except:
        pass

FORM_URL = os.environ.get("FORM_URL")
REPORT_URL = os.environ.get("REPORT_URL")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not all([FORM_URL, REPORT_URL, SUPABASE_URL, SUPABASE_KEY]):
    raise ValueError("Faltam variáveis de ambiente (FORM_URL, REPORT_URL, SUPABASE_URL, SUPABASE_KEY)")

# Inicializa Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Data Dinâmica
ANO_ATUAL = str(datetime.now().year)

PARAMS_RELATORIO = {
    'tipo': '0',                
    'anoInicial': ANO_ATUAL,    
    'anoFinal': ANO_ATUAL,      
    'situacao_pagamento': '0',  
    'repasse_cre': '0',         
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

session = requests.Session()
session.headers.update(HEADERS)

def get_options_from_select(select_id: str, page_content: str):
    soup = BeautifulSoup(page_content, 'html.parser')
    select_tag = soup.find('select', {'id': select_id})
    if not select_tag:
        return []
    options = []
    for option in select_tag.find_all('option'):
        value = option.get('value')
        text = option.text.strip()
        if value and value not in ["0", ""]:
            options.append({'value': value, 'text': text})
    return options

def fetch_page_content(url: str, params: dict = None):
    try:
        response = session.get(url, params=params, timeout=45)
        response.raise_for_status()
        response.encoding = 'iso-8859-1' 
        return response.text
    except requests.RequestException as e:
        logging.error(f"Erro ao buscar URL: {e}")
        return None

def extract_total_from_report(html_content: str) -> float | None:
    soup = BeautifulSoup(html_content, 'html.parser')
    bold_tags = soup.find_all('b')
    
    for i, tag in enumerate(bold_tags):
        if 'total geral' in tag.get_text(strip=True).lower():
            if i + 1 < len(bold_tags):
                valor_str = bold_tags[i+1].get_text(strip=True).replace("R$", "").strip()
                try:
                    valor_limpo = valor_str.replace('.', '').replace(',', '.')
                    return float(valor_limpo)
                except ValueError:
                    return 0.0
    return None

def salvar_no_supabase(dados):
    """Insere o lote de dados no Supabase"""
    try:
        data = supabase.table("historico_scrapes").insert(dados).execute()
        logging.info(f"Sucesso! {len(dados)} registros inseridos no Supabase.")
    except Exception as e:
        logging.error(f"Erro ao inserir no Supabase: {e}")

def main():
    logging.info(f"Iniciando scraping para o ano: {ANO_ATUAL}")
    
    dados_para_inserir = []
    lista_erros = []

    content_ini = fetch_page_content(FORM_URL)
    if not content_ini: return
    regionais = get_options_from_select('cmbSubsecretaria', content_ini)

    for regional in regionais:
        logging.info(f"Regional: {regional['text']}")
        
        content_mun = fetch_page_content(FORM_URL, params={'cmbSubsecretaria': regional['value']})
        if not content_mun: continue
        municipios = get_options_from_select('cmbMunicipio', content_mun)

        for municipio in municipios:
            logging.info(f"  Município: {municipio['text']}")
            
            params_esc = {'cmbSubsecretaria': regional['value'], 'cmbMunicipio': municipio['value']}
            content_esc = fetch_page_content(FORM_URL, params=params_esc)
            if not content_esc: continue
            escolas = get_options_from_select('cmbUnidadeEnsino', content_esc)

            for escola in escolas:
                try:
                    try:
                        codigo_mec, nome_escola = escola['text'].split(" - ", 1)
                    except:
                        codigo_mec = None
                        nome_escola = escola['text']

                    p_final = PARAMS_RELATORIO.copy()
                    p_final.update({
                        'subsecretaria': regional['value'],
                        'Municipio': municipio['value'],
                        'Escola': escola['value']
                    })
                    
                    html_report = fetch_page_content(REPORT_URL, params=p_final)
                    valor = extract_total_from_report(html_report) if html_report else 0.0

                    if valor is not None:
                        registro = {
                            "id_mec": codigo_mec.strip() if codigo_mec else None,
                            "id_interno": escola['value'],
                            "nome_escola": nome_escola.strip(),
                            "municipio": municipio['text'],
                            "regional": regional['text'],
                            "total_valor": valor,
                            "ano_referencia": int(ANO_ATUAL),
                            "data_extracao": datetime.now().isoformat()
                        }
                        dados_para_inserir.append(registro)
                        logging.info(f"    -> {nome_escola[:30]}... : R$ {valor}")
                    else:
                        raise ValueError("Valor não encontrado no HTML")

                except Exception as e:
                    logging.error(f"    [ERRO] Falha na escola {escola['text']}: {e}")
                    lista_erros.append({
                        "escola": escola['text'],
                        "id_interno": escola['value'],
                        "erro": str(e),
                        "data": datetime.now().isoformat()
                    })
                
                time.sleep(0.5)
    
    if dados_para_inserir:
        batch_size = 100
        batch_size = 100
        for i in range(0, len(dados_para_inserir), batch_size):
            batch = dados_para_inserir[i:i + batch_size]
            salvar_no_supabase(batch)
    
    if lista_erros:
        logging.warning(f"Total de {len(lista_erros)} escolas com erro. Salvando JSON.")
        with open('erros_extracao.json', 'w', encoding='utf-8') as f:
            json.dump(lista_erros, f, ensure_ascii=False, indent=2)
    else:
        logging.info("Processo finalizado sem erros!")

if __name__ == "__main__":
    main()