import os
import time
import logging
import locale
import json
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except Exception:
    try:
        locale.setlocale(locale.LC_ALL, '')
    except Exception:
        pass

FORM_URL = os.environ.get("FORM_URL")
REPORT_URL = os.environ.get("REPORT_URL")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TABELA_PRINCIPAL = "DadosEscolas"

BATCH_SIZE = 50
ANO_ATUAL = str(datetime.now().year)

if not all([FORM_URL, REPORT_URL, SUPABASE_URL, SUPABASE_KEY]):
    raise ValueError("Faltam variáveis de ambiente obrigatórias.")

# Inicializa Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    # Tenta 3 vezes se receber erros de servidor ou falha de conexão
    retry_strategy = Retry(
        total=3,
        backoff_factor=1, # Espera 1s, 2s, 4s...
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

session = get_session()

# Parâmetros Base
PARAMS_RELATORIO = {
    'tipo': '0', 'anoInicial': ANO_ATUAL, 'anoFinal': ANO_ATUAL,
    'situacao_pagamento': '0', 'repasse_cre': '0'
}

# --- Funções de Extração (Scraping) ---

def fetch_content(url: str, params: dict = None):
    """Busca conteúdo com tratamento de erro e retry automático"""
    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        response.encoding = 'iso-8859-1'
        return response.text
    except Exception as e:
        logging.error(f"Erro de rede ao acessar {url}: {e}")
        return None

def get_options(select_id: str, html: str):
    """Extrai opções de um select"""
    if not html: return []
    soup = BeautifulSoup(html, 'html.parser')
    select = soup.find('select', {'id': select_id})
    if not select: return []
    
    return [
        {'value': opt.get('value'), 'text': opt.text.strip()}
        for opt in select.find_all('option')
        if opt.get('value') and opt.get('value') not in ["0", ""]
    ]

def parse_valor_total(html: str) -> float:
    """Extrai o valor monetário do HTML"""
    if not html: return 0.0
    soup = BeautifulSoup(html, 'html.parser')
    
    # Procura por "Total Geral" (insensível a maiúsculas/minúsculas)
    target = soup.find(lambda tag: tag.name == "b" and "total geral" in tag.get_text(strip=True).lower())
    
    if target:
        # Tenta pegar o próximo elemento, que geralmente contém o valor
        proximo = target.find_next('b')
        if proximo:
            valor_texto = proximo.get_text(strip=True).replace("R$", "").strip()
            try:
                # Converte "1.234,56" para float 1234.56
                return float(valor_texto.replace('.', '').replace(',', '.'))
            except ValueError:
                return 0.0
    return 0.0

def processar_escola(regional, municipio, escola):
    """Processa uma única escola e retorna o dicionário de dados ou erro"""
    try:
        # Lógica para separar Código MEC do Nome
        parts = escola['text'].split(" - ", 1)
        codigo_mec = parts[0].strip() if len(parts) > 1 else None
        nome_escola = parts[1].strip() if len(parts) > 1 else escola['text']

        # Busca dados do relatório
        params = PARAMS_RELATORIO.copy()
        params.update({
            'subsecretaria': regional['value'],
            'Municipio': municipio['value'],
            'Escola': escola['value']
        })
        
        html = fetch_content(REPORT_URL, params)
        valor = parse_valor_total(html)

        dados = {
            "id_mec": codigo_mec,
            "id_interno": escola['value'],
            "nome_escola": nome_escola,
            "municipio": municipio['text'],
            "regional": regional['text'],
            "total_valor": valor,
            "ano_referencia": int(ANO_ATUAL),
            "data_extracao": datetime.now().isoformat()
        }
        return dados, None

    except Exception as e:
        erro_info = {
            "escola": escola['text'],
            "id_interno": escola['value'],
            "erro": str(e),
            "data": datetime.now().isoformat()
        }
        return None, erro_info

# --- Funções de Banco de Dados (Batch Operations) ---

def flush_dados(buffer_dados):
    """Salva um lote de dados no Supabase (Histórico e Atualização da Tabela Principal)"""
    if not buffer_dados: return

    logging.info(f"💾 Salvando lote de {len(buffer_dados)} registros...")
    
    try:
        # 1. Insert no Histórico (Log de tudo que aconteceu)
        supabase.table("historico_scrapes").insert(buffer_dados).execute()
        
        # 2. Upsert na Tabela Principal (Muito mais rápido que update um por um)
        # Prepara dados contendo apenas ID e Valor para atualização
        dados_update = [
            {"Id": item['id_mec'], "investimento_ano_atual": item['total_valor']}
            for item in buffer_dados
            if item['id_mec'] # Só atualiza se tiver ID MEC válido
        ]
        
        if dados_update:
            # O 'upsert' atualiza se o ID existir. Importante: 'Id' deve ser PK ou Unique.
            supabase.table(TABELA_PRINCIPAL).upsert(
                dados_update, on_conflict="Id"
            ).execute()
            
        logging.info("✅ Lote salvo com sucesso.")
        
    except Exception as e:
        logging.error(f"❌ Erro crítico ao salvar lote no Supabase: {e}")

# --- Loop Principal ---

def main():
    logging.info(f"🚀 Iniciando scraping otimizado | Ano: {ANO_ATUAL}")
    
    buffer_dados = []
    lista_erros = []
    total_processado = 0
    
    html_ini = fetch_content(FORM_URL)
    regionais = get_options('cmbSubsecretaria', html_ini)

    for regional in regionais:
        logging.info(f"📍 Regional: {regional['text']}")
        
        html_mun = fetch_content(FORM_URL, {'cmbSubsecretaria': regional['value']})
        municipios = get_options('cmbMunicipio', html_mun)

        for municipio in municipios:
            logging.info(f"  🏙️ Município: {municipio['text']}")
            
            html_esc = fetch_content(FORM_URL, {
                'cmbSubsecretaria': regional['value'],
                'cmbMunicipio': municipio['value']
            })
            escolas = get_options('cmbUnidadeEnsino', html_esc)

            for escola in escolas:
                # Processa escola individualmente
                dados, erro = processar_escola(regional, municipio, escola)
                
                if dados:
                    buffer_dados.append(dados)
                    logging.info(f"    -> {dados['nome_escola'][:30]}... : R$ {dados['total_valor']}")
                    total_processado += 1
                elif erro:
                    lista_erros.append(erro)
                    logging.error(f"    ❌ Erro em {escola['text']}")

                # FLUSH: Se o buffer encher (50 itens), salva no banco e limpa memória
                if len(buffer_dados) >= BATCH_SIZE:
                    flush_dados(buffer_dados)
                    buffer_dados = [] # Limpa buffer
                
                # Delay reduzido pois agora temos retries e batch saving
                time.sleep(0.1)

    # Flush final (salva o que sobrou no buffer)
    if buffer_dados:
        flush_dados(buffer_dados)

    # Relatório de Erros
    if lista_erros:
        logging.warning(f"⚠️ Processo finalizado com {len(lista_erros)} erros.")
        with open('erros_extracao.json', 'w', encoding='utf-8') as f:
            json.dump(lista_erros, f, ensure_ascii=False, indent=2)
    else:
        logging.info("✨ Processo finalizado com SUCESSO ABSOLUTO!")
    
    logging.info(f"📊 Total de registros processados: {total_processado}")

if __name__ == "__main__":
    main()