import os
import resend
from fastapi import FastAPI, HTTPException, Header, Request
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from html import escape
from email_validator import validate_email, EmailNotValidError
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
SECRET_TOKEN = os.environ.get("SECRET_TOKEN")
resend.api_key = os.environ.get("EMAIL_KEY")

# Cria o cliente Supabase
supabase: Client = create_client(url, key)

# Inicializa o aplicativo FastAPI e o limitador de taxa
limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://escola-go.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api")
@limiter.limit("5/minute")
def read_root(request: Request):
    return {"message": "API conectada ao Supabase!"}

@app.get("/api/Escolas")
@limiter.limit("20/minute")
def get_schools(request: Request):
    try:
        response = supabase.table('DadosEscolas').select("Id, Nome, Municipio").limit(1100).execute()
        
        return {"status": "success", "data": response.data}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/Escolas/{school_id}")
@limiter.limit("20/minute")
def get_school_by_id(request: Request, school_id: int):
    try:
        response = supabase.table('DadosEscolas').select("*").eq("Id", school_id).execute()
        
        if response.data:
            return {"status": "success", "data": response.data[0]}
        else:
            raise HTTPException(status_code=404, detail="Escola não encontrada")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/dadosTotais")
@limiter.limit("20/minute")
def get_total_data(request: Request):
    try:
        response = supabase.table('DadosTotais').select("*").execute()
        
        if response.data:
            return {"status": "success", "data": response.data[0]}
        else:
            raise HTTPException(status_code=404, detail="Dados não encontrados")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post("/api/SendEmail")
@limiter.limit("5/minute")
def send_email(request: Request, email_data: dict, authorization: str = Header(...)):
    try:
        if not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Token inválido ou ausente")
        
        token = authorization.split("Bearer ")[1]
        if token != SECRET_TOKEN:
            raise HTTPException(status_code=403, detail="Acesso negado")

        # Sanitizar dados recebidos
        name = escape(email_data.get("name", ""))
        email = escape(email_data.get("email", ""))
        subject = escape(email_data.get("subject", ""))
        message = escape(email_data.get("message", "")).replace('\n', '<br>')

        try:
            validate_email(email)
        except EmailNotValidError:
            raise HTTPException(status_code=400, detail="Email inválido")

        formatted_message = message.replace('\n', '<br>')
        
        # Construir o HTML do e-mail
        html_content = f"""
            <p><strong>Nome:</strong> {name}</p>
            <p><strong>Email (para resposta):</strong> {email}</p>
            <p><strong>Assunto:</strong> {subject}</p>
            <hr>
            <p><strong>Mensagem:</strong></p>
            <p>{formatted_message}</p>
        """
        
        # Preparar e enviar o e-mail via Resend
        params = {
            "from": "Formulário de Contato <onboarding@resend.dev>",
            "to": ["emmanuelcontatocomercial@gmail.com"],
            "subject": f"Nova Mensagem (EscolaGO): {subject}",
            "reply_to": email,
            "html": html_content,
        }
        
        email_response = resend.Emails.send(params)

        # Retornar sucesso
        return {"status": "success", "data": email_response}

    except Exception as e:
        print(f"Erro ao enviar email: {e}")
        raise HTTPException(status_code=500, detail=str(e))

'''
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
'''