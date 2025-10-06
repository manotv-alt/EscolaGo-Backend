import os
from fastapi import FastAPI, HTTPException
from supabase import create_client, Client
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

# Cria o cliente Supabase
supabase: Client = create_client(url, key)

# Inicializa o aplicativo FastAPI
app = FastAPI()

@app.get("/api")
def read_root():
    return {"message": "API conectada ao Supabase!"}

@app.get("/api/Escolas")
def get_schools():
    try:
        response = supabase.table('DadosEscolas').select("Id, Nome, Municipio").execute()
        
        return {"status": "success", "data": response.data}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/Escolas/{school_id}")
def get_school_by_id(school_id: int):
    try:
        response = supabase.table('DadosEscolas').select("*").eq("Id", school_id).execute()
        
        if response.data:
            return {"status": "success", "data": response.data[0]}
        else:
            raise HTTPException(status_code=404, detail="Escola não encontrada")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/dadosTotais")
def get_total_data():
    try:
        response = supabase.table('DadosTotais').select("*").execute()
        
        if response.data:
            return {"status": "success", "data": response.data[0]}
        else:
            raise HTTPException(status_code=404, detail="Escola não encontrada")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))