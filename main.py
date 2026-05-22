from fastapi import FastAPI, Request
from dotenv import load_dotenv
import anthropic
import requests
import os
import json

load_dotenv()

app = FastAPI()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ZAPI_INSTANCE = os.getenv("ZAPI_INSTANCE")
ZAPI_TOKEN = os.getenv("ZAPI_TOKEN")

mensagens_pendentes = {}

def analisar_mensagem(texto):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    resposta = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": f"""Você é assistente de um escritório jurídico especializado em BPC LOAS, inventário e licitações.

Analise a mensagem do cliente abaixo e responda em JSON com:
- urgencia: alta, media ou baixa
- area: bpc_loas, inventario, licitacoes ou geral
- resposta: uma resposta profissional e acolhedora para enviar ao cliente

Mensagem do cliente: {texto}

Responda APENAS com o JSON, sem texto adicional."""
        }]
    )
    return json.loads(resposta.content[0].text)

def enviar_whatsapp(telefone, mensagem):
    url = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}/send-text"
    payload = {"phone": telefone, "message": mensagem}
    requests.post(url, json=payload)

@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()
    
    if data.get("type") != "ReceivedCallback":
        return {"status": "ignorado"}
    
    telefone = data.get("phone", "")
    texto = data.get("text", {}).get("message", "")
    
    if not texto:
        return {"status": "sem texto"}
    
    analise = analisar_mensagem(texto)
    
    mensagens_pendentes[telefone] = {
        "telefone": telefone,
        "mensagem_original": texto,
        "analise": analise,
        "status": "pendente"
    }
    
    print(f"Nova mensagem de {telefone}: {texto}")
    print(f"Análise: {analise}")
    
    return {"status": "recebido"}

@app.get("/pendentes")
async def listar_pendentes():
    return list(mensagens_pendentes.values())

@app.post("/aprovar/{telefone}")
async def aprovar(telefone: str, request: Request):
    data = await request.json()
    mensagem_final = data.get("mensagem", "")
    
    if telefone not in mensagens_pendentes:
        return {"erro": "mensagem não encontrada"}
    
    enviar_whatsapp(telefone, mensagem_final)
    mensagens_pendentes[telefone]["status"] = "enviado"
    
    return {"status": "enviado"}

@app.get("/")
async def root():
    return {"status": "servidor rodando"}