import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import locale
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()
FORM_URL = os.environ.get("FORM_URL")
REPORT_URL = os.environ.get("REPORT_URL")

# --- Configuração de Log e Locale ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configura o locale para Português do Brasil
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    logging.warning("Locale 'pt_BR.UTF-8' não encontrado. Tentando locale padrão do sistema.")
    try:
        locale.setlocale(locale.LC_ALL, '') # Windows
    except locale.Error:
        logging.error("Não foi possível configurar nenhum locale. O parsing de moeda pode falhar.")

# Parâmetros fixos para o relatório
PARAMS_RELATORIO = {
    'tipo': '0',                # 0 = Todos
    'anoInicial': '2025',
    'anoFinal': '2025',
    'situacao_pagamento': '0',  # 0 = Todos
    'repasse_cre': '0',         # 0 = Padrão
}

# Cabeçalho para simular um navegador
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Sessão para reaproveitar conexões
session = requests.Session()
session.headers.update(HEADERS)


def get_options_from_select(select_id: str, page_content: str):
    """
    Extrai todos os <option> (value e text) de um <select> no HTML.
    Ignora a primeira opção ("Selecione...").
    """
    soup = BeautifulSoup(page_content, 'html.parser')
    select_tag = soup.find('select', {'id': select_id})
    
    if not select_tag:
        logging.warning(f"Não foi possível encontrar o select com id '{select_id}'")
        return []

    options = []
    for option in select_tag.find_all('option'):
        value = option.get('value')
        text = option.text.strip()
        # Ignoramos a opção "Selecione..." ou "Todos" (valor 0 ou vazio)
        if value and value not in ["0", ""]:
            options.append({'value': value, 'text': text})
    return options

def fetch_page_content(url: str, params: dict = None):
    """
    Busca uma página e retorna seu conteúdo de texto.
    """
    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        # Usamos 'latin-1' (iso-8859-1)
        response.encoding = 'iso-8859-1' 
        return response.text
    except requests.RequestException as e:
        logging.error(f"Erro ao buscar {url} com params {params}: {e}")
        return None

def extract_total_from_report(html_content: str) -> float | None:
    """
    Encontra o "Total Geral" no HTML do relatório e converte para float.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    bold_tags = soup.find_all('b')
    total_geral_valor_str = None
    
    for i, tag in enumerate(bold_tags):
        tag_text = tag.get_text(strip=True).lower()
        if 'total geral' in tag_text:
            if i + 1 < len(bold_tags):
                valor_tag = bold_tags[i+1]
                total_geral_valor_str = valor_tag.get_text(strip=True)
                break
    
    if not total_geral_valor_str:
        return None

    # Limpa e converte o valor
    valor_limpo = total_geral_valor_str.replace("R$", "").strip()
    try:
        valor_numerico = locale.atof(valor_limpo)
        return valor_numerico
    except (ValueError, locale.Error):
        logging.warning(f"Não foi possível converter o valor '{valor_limpo}' para número.")
        return None


def main():
    """
    Função principal que orquestra o scraping.
    """
    logging.info("Iniciando o script de extração...")
    
    # Lista para armazenar os resultados
    resultados_finais = []

    # --- NÍVEL 1: Buscar Coordenadorias Regionais ---
    logging.info("Buscando lista de Coordenadorias Regionais...")
    initial_page_content = fetch_page_content(FORM_URL)
    if not initial_page_content:
        logging.error("Falha ao carregar página inicial de formulários. Abortando.")
        return

    regionais = get_options_from_select('cmbSubsecretaria', initial_page_content)
    logging.info(f"Encontradas {len(regionais)} regionais.")
    time.sleep(1)

    for regional in regionais:
        logging.info(f"[Regional] Processando: {regional['text']} (ID: {regional['value']})")
        
        # --- NÍVEL 2: Buscar Municípios para cada Regional ---
        params_regional = {'cmbSubsecretaria': regional['value']}
        page_content_mun = fetch_page_content(FORM_URL, params=params_regional)
        
        if not page_content_mun:
            continue

        municipios = get_options_from_select('cmbMunicipio', page_content_mun)
        logging.info(f"  -> Encontrados {len(municipios)} municípios.")
        time.sleep(1)

        for municipio in municipios:
            logging.info(f"  [Município] Processando: {municipio['text']} (ID: {municipio['value']})")

            # --- NÍVEL 3: Buscar Escolas para cada Município ---
            params_municipio = {
                'cmbSubsecretaria': regional['value'],
                'cmbMunicipio': municipio['value']
            }
            page_content_esc = fetch_page_content(FORM_URL, params=params_municipio)

            if not page_content_esc:
                continue

            escolas = get_options_from_select('cmbUnidadeEnsino', page_content_esc)
            logging.info(f"    -> Encontradas {len(escolas)} escolas.")
            time.sleep(1)

            for escola in escolas:

                # Lógica para dividir os IDs da escola
                escola_id_interno = escola['value']
                escola_texto_completo = escola['text']

                escola_codigo_mec = None
                escola_nome = escola_texto_completo

                try:
                    # Tenta dividir o texto no primeiro " - "
                    partes = escola_texto_completo.split(" - ", 1)
                    if len(partes) == 2:
                        escola_codigo_mec = partes[0].strip()
                        escola_nome = partes[1].strip()
                except Exception as e:
                    logging.warning(f"      -> AVISO: Não foi possível parsear o nome da escola: {escola_texto_completo}. Erro: {e}")

                # Log atualizado para mostrar os dois IDs
                logging.info(f"    [Escola] Buscando total de: {escola_nome} (ID_MEC: {escola_codigo_mec} | ID_Interno: {escola_id_interno})")
                
                # --- NÍVEL 4: Buscar o Relatório e Extrair o Total ---
                
                # Prepara os parâmetros para a URL do RELATÓRIO
                final_params = PARAMS_RELATORIO.copy()
                final_params['subsecretaria'] = regional['value']
                final_params['Municipio'] = municipio['value']
                final_params['Escola'] = escola_id_interno # Usamos o ID interno para a consulta
                
                report_html = fetch_page_content(REPORT_URL, params=final_params)
                
                if report_html:
                    total_escola = extract_total_from_report(report_html)
                    
                    if total_escola is not None:
                        logging.info(f"      -> SUCESSO: R$ {total_escola:.2f}")

                        # Salva os dois IDs na lista de resultados
                        resultados_finais.append({
                            'id_mec': escola_codigo_mec,
                            'id_interno_seduc': escola_id_interno,
                            'nome_escola': escola_nome,
                            'nome_municipio': municipio['text'],
                            'nome_regional': regional['text'],
                            'total_geral_2025': total_escola
                        })
                    else:
                        logging.info("      -> INFO: 'Total Geral' não encontrado para esta escola (provavelmente R$ 0,00).")
                else:
                    logging.error("      -> ERRO: Falha ao buscar o relatório desta escola.")

                time.sleep(1) # Pausa para não sobrecarregar o servidor

    # --- Compilação Final ---
    if not resultados_finais:
        logging.warning("Nenhum dado foi extraído em toda a execução.")
        return

    logging.info("Compilando todos os resultados em um único arquivo CSV...")
    
    # Converte a lista de dicionários em um DataFrame do Pandas
    df = pd.DataFrame(resultados_finais)

    # Salva em CSV
    output_file = "totais_por_escola_2025.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig', decimal=',', sep=';')

    logging.info(f"--- Processo Concluído ---")
    logging.info(f"Total de escolas com valores encontrados: {len(df)}")
    logging.info(f"Arquivo salvo como: {output_file}")
    logging.info("PS: O arquivo foi salvo com ';' como separador e ',' como decimal, ideal para abrir no Excel em português.")


# Executa a função principal
if __name__ == "__main__":
    main()