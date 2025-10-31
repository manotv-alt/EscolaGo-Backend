import os
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
SECRET_TOKEN = os.environ.get("SECRET_TOKEN")

# Cria o cliente Supabase
supabase: Client = create_client(url, key)

# Inicializa o aplicativo FastAPI
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://escola-go.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api")
def read_root():
    return {"message": "API conectada ao Supabase!"}

@app.get("/api/Escolas")
def get_schools():
    try:
        response = supabase.table('DadosEscolas').select("Id, Nome, Municipio").limit(1100).execute()
        
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
            raise HTTPException(status_code=404, detail="Dados não encontrados")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/api/Escolas/{school_id}")
def update_school(school_id: str, school_data: dict, authorization: str = Header(...)):
    try:
        if not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Token inválido ou ausente")
        
        token = authorization.split("Bearer ")[1]
        if token != SECRET_TOKEN:
            raise HTTPException(status_code=403, detail="Acesso negado")
        
        print(f"school_id recebido: {school_id}")
        print(f"school_data recebido: {school_data}")
        
        check_response = supabase.table('DadosEscolas').select("*").eq("Id", school_id).execute()
        print(f"Verificação de existência: {check_response.data}")

        if not check_response.data:
            raise HTTPException(status_code=404, detail="Escola não encontrada para atualização")
        
        if "investimento_ano_atual" in school_data:
            school_data["investimento_ano_atual"] = float(school_data["investimento_ano_atual"])
        
        response = supabase.table('DadosEscolas').update(school_data).eq("Id", school_id).execute()
        print(f"Resposta do Supabase: {response}")
        
        if response.data:
            return {"status": "success", "data": response.data[0]}
        else:
            raise HTTPException(status_code=404, detail="Escola não encontrada")

    except Exception as e:
        print(f"Erro: {e}")
        raise HTTPException(status_code=500, detail=str(e))
