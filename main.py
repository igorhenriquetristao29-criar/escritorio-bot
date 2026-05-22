from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import anthropic
import requests
import os
import json
import re

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

mensagens_pendentes = {}

IGOR = "5564981475621"
LETICIA = "5564981177107"

def notificar_equipe(nome, mensagem, urgencia, area):
    instance = os.environ["ZAPI_INSTANCE"]
    token = os.environ["ZAPI_TOKEN"]
    client_token = os.environ["ZAPI_CLIENT_TOKEN"]
    url = f"https://api.z-api.io/instances/{instance}/token/{token}/send-text"
    headers = {"Client-Token": client_token}
    
    emoji_urgencia = "🔴" if urgencia == "alta" else "🟡" if urgencia == "media" else "🟢"
    
    texto = f"""🔔 *Nova mensagem de cliente!*

👤 *Cliente:* {nome}
💬 *Mensagem:* "{mensagem}"
{emoji_urgencia} *Urgência:* {urgencia.upper()}
📁 *Área:* {area.upper()}

👉 Acesse o painel para responder:
https://web-production-444ef9.up.railway.app/painel"""

    for numero in [IGOR, LETICIA]:
        requests.post(url, json={"phone": numero, "message": texto}, headers=headers)

def analisar_mensagem(texto):
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    resposta = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": f"""Você é atendente de um escritório jurídico especializado em BPC LOAS, inventário e licitações.

REGRA PRINCIPAL: Adapte sua linguagem ao perfil do cliente.
- Se o cliente escreve de forma simples, popular ou com erros → responda de forma simples, calorosa e fácil de entender. Evite termos técnicos.
- Se o cliente escreve de forma formal ou técnica → responda na mesma altura, com vocabulário jurídico adequado.
- SEMPRE seja humano, acolhedor e empático. Nunca robotizado.
- Seja direto e objetivo. Não use frases longas desnecessárias.
- Use o nome do cliente se souber.

Analise a mensagem e responda SOMENTE com um JSON válido:

{{"urgencia": "alta/media/baixa", "area": "bpc_loas/inventario/licitacoes/geral", "perfil": "simples/formal", "resposta": "sua resposta aqui"}}

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
        "perfil": "simples",
        "resposta": texto_resposta
    }

def enviar_whatsapp(telefone, mensagem):
    instance = os.environ["ZAPI_INSTANCE"]
    token = os.environ["ZAPI_TOKEN"]
    client_token = os.environ["ZAPI_CLIENT_TOKEN"]
    url = f"https://api.z-api.io/instances/{instance}/token/{token}/send-text"
    headers = {"Client-Token": client_token}
    payload = {"phone": telefone, "message": mensagem}
    r = requests.post(url, json=payload, headers=headers)
    print(f"Z-API resposta: {r.status_code} - {r.text}")

@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()
    
    if data.get("type") != "ReceivedCallback":
        return {"status": "ignorado"}
    
    telefone = data.get("phone", "")
    texto = data.get("text", {}).get("message", "")
    nome = data.get("senderName", "Cliente")
    
    if not texto:
        return {"status": "sem texto"}
    
    if telefone in [IGOR, LETICIA]:
        return {"status": "ignorado - equipe"}
    
    analise = analisar_mensagem(texto)
    
    mensagens_pendentes[telefone] = {
        "telefone": telefone,
        "nome": nome,
        "foto": data.get("photo", ""),
        "mensagem_original": texto,
        "analise": analise,
        "status": "pendente"
    }
    
    notificar_equipe(nome, texto, analise["urgencia"], analise["area"])
    
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

@app.get("/painel")
async def painel():
    return FileResponse("painel.html")

@app.get("/")
async def root():
    return {"status": "servidor rodando"}