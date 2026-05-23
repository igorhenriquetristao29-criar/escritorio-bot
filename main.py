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

def notificar_equipe(nome, mensagem, urgencia, area, categoria):
    instance = os.environ["ZAPI_INSTANCE"]
    token = os.environ["ZAPI_TOKEN"]
    client_token = os.environ["ZAPI_CLIENT_TOKEN"]
    url = f"https://api.z-api.io/instances/{instance}/token/{token}/send-text"
    headers = {"Client-Token": client_token}
    
    if categoria == "fora_da_area":
        emoji = "🔵"
        tag = "FORA DA ÁREA"
    elif urgencia == "alta":
        emoji = "🔴"
        tag = "URGENTE"
    elif urgencia == "media":
        emoji = "🟡"
        tag = "NORMAL"
    else:
        emoji = "🟢"
        tag = "BAIXA PRIORIDADE"

    texto = f"""{emoji} *{tag} — Nova mensagem!*

👤 *Cliente:* {nome}
💬 *Mensagem:* "{mensagem}"
📁 *Área:* {area.upper()}

👉 Painel:
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
            "content": f"""Você é atendente de um escritório jurídico que atua nas seguintes áreas:
- BPC LOAS (Benefício de Prestação Continuada)
- Inventário e sucessões
- Licitações e contratos administrativos

Analise a mensagem recebida e classifique em uma das 4 categorias:

1. "cliente_nossa_area" — pessoa buscando serviços jurídicos nas nossas áreas de atuação
2. "cliente_fora_area" — pessoa buscando serviços jurídicos em outras áreas (ex: divórcio, trabalhista, criminal, etc)
3. "conversa_pessoal" — conversa cotidiana, mensagem de amigo, familiar ou conhecido
4. "irrelevante" — spam, propaganda, mensagem sem sentido

REGRAS DE RESPOSTA:
- Adapte a linguagem ao perfil do cliente:
  * Cliente escreve simples → responda simples, caloroso, sem juridiquês
  * Cliente escreve formal/técnico → responda na mesma altura
- Para "cliente_nossa_area": resposta acolhedora e profissional, colete mais informações
- Para "cliente_fora_area": resposta acolhedora, explique que não é a área mas que podem indicar um colega especialista
- Para "conversa_pessoal" e "irrelevante": deixe o campo resposta vazio ""

Responda SOMENTE com JSON válido:

{{"categoria": "cliente_nossa_area", "urgencia": "alta/media/baixa", "area": "bpc_loas/inventario/licitacoes/geral/fora_da_area", "perfil": "simples/formal", "resposta": "sua resposta aqui"}}

Mensagem recebida: {texto}"""
        }]
    )
    
    texto_resposta = resposta.content[0].text.strip()
    match = re.search(r'\{.*\}', texto_resposta, re.DOTALL)
    if match:
        return json.loads(match.group())
    
    return {
        "categoria": "irrelevante",
        "urgencia": "baixa",
        "area": "geral",
        "perfil": "simples",
        "resposta": ""
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

    # Ignora grupos e newsletters
    if data.get("isGroup") or data.get("isNewsletter"):
        return {"status": "ignorado - grupo"}

    # Ignora status/stories
    if data.get("isStatusReply"):
        return {"status": "ignorado - status"}

    if data.get("type") != "ReceivedCallback":
        return {"status": "ignorado"}

    telefone = data.get("phone", "")
    texto = data.get("text", {}).get("message", "")
    nome = data.get("senderName", "Cliente")

    if not texto:
        return {"status": "sem texto"}

    # Ignora mensagens da própria equipe
    if telefone in [IGOR, LETICIA]:
        return {"status": "ignorado - equipe"}

    analise = analisar_mensagem(texto)
    categoria = analise.get("categoria", "irrelevante")

    # Ignora conversas pessoais e irrelevantes
    if categoria in ["conversa_pessoal", "irrelevante"]:
        print(f"IGNORADO ({categoria}): {nome} - {texto}")
        return {"status": f"ignorado - {categoria}"}

    mensagens_pendentes[telefone] = {
        "telefone": telefone,
        "nome": nome,
        "foto": data.get("photo", ""),
        "mensagem_original": texto,
        "analise": analise,
        "status": "pendente"
    }

    notificar_equipe(nome, texto, analise["urgencia"], analise["area"], categoria)

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