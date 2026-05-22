from fastapi import FastAPI, Request
import anthropic
import requests
import os
import json
import re

app = FastAPI()

mensagens_pendentes = {}

def analisar_mensagem(texto):
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    resposta = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": f"""Você é assistente de um escritório jurídico especializado em BPC LOAS, inventário e licitações.

Analise a mensagem do cliente e responda SOMENTE com um JSON válido, sem nenhum texto antes ou depois.

Formato exato:
{{"urgencia": "alta", "area": "bpc_loas", "resposta": "sua resposta aqui"}}

Valores possíveis:
- urgencia: alta, media ou baixa
- area: bpc_loas, inventario, licitacoes ou geral

Mensagem do cliente: {texto}"""
        }]
    )
    
    texto_resposta = resposta.content[0].text.strip()
    
    match = re.search(r'\{.*\}', texto_resposta, re.DOTALL)
    if match:
        return json.loads(match.group())
    
    return {
        "urgencia": "media",
        "area": "geral",
        "resposta": texto_resposta
    }

def enviar_whatsapp(telefone, mensagem):
    instance = os.environ["ZAPI_INSTANCE"]
    token = os.environ["ZAPI_TOKEN"]
    url = f"https://api.z-api.io/instances/{instance}/token/{token}/send-text"
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