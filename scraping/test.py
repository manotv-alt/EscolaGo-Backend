import os
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime

# Carrega variáveis locais se existirem (para teste local)
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Erro: Variáveis SUPABASE_URL ou SUPABASE_KEY não encontradas.")
    exit(1)

def teste_conexao():
    print("--- INICIANDO TESTE DE CONEXÃO SUPABASE ---")
    print(f"URL Alvo: {SUPABASE_URL[:15]}...") # Mostra só o começo por segurança

    # Inicializa cliente
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"❌ Erro ao criar cliente Supabase: {e}")
        return

    # Dados fake para teste
    dados_teste = {
        "id_mec": "TESTE_DEV",
        "id_interno": "000000",
        "nome_escola": "ESCOLA DE TESTE DE CONEXÃO",
        "municipio": "MUNICÍPIO TESTE",
        "regional": "REGIONAL TESTE",
        "total_valor": 123.45,
        "ano_referencia": 2025,
        "data_extracao": datetime.now().isoformat()
    }

    print("Tentando inserir registro de teste...")

    try:
        # Tenta inserir
        response = supabase.table("historico_scrapes").insert(dados_teste).execute()
        
        # Verifica se retornou dados (sucesso)
        if response.data:
            print("✅ SUCESSO! Dados inseridos com êxito.")
            print("Dados retornados:", response.data)
            print("Pode conferir no painel do Supabase, deve haver uma linha nova lá.")
        else:
            print("⚠️ Aviso: O comando rodou, mas nenhum dado foi retornado. Verifique as permissões (RLS) no Supabase.")
            
    except Exception as e:
        print(f"❌ FALHA: Erro ao inserir dados no Supabase.")
        print(f"Detalhe do erro: {e}")

if __name__ == "__main__":
    teste_conexao()