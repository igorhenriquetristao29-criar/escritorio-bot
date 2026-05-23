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
        tag = "FORA DA AREA"
    elif urgencia == "alta":
        tag = "URGENTE"
    elif urgencia == "media":
        tag = "NORMAL"
    else:
        tag = "BAIXA PRIORIDADE"

    texto = f"""{tag} - Nova mensagem!

Cliente: {nome}
Mensagem: "{mensagem}"
Area: {area.upper()}

Painel:
https://web-production-444ef9.up.railway.app/painel"""

    for numero in [IGOR, LETICIA]:
        requests.post(url, json={"phone": numero, "message": texto}, headers=headers)

def analisar_mensagem(texto, feedback=None):
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    instrucao_feedback = ""
    if feedback:
        instrucao_feedback = f"""
IMPORTANTE: A resposta anterior foi rejeitada. Feedback da equipe: "{feedback}"
Gere 3 opcoes diferentes de resposta considerando esse feedback.
Retorne as 3 opcoes no campo "opcoes" como uma lista.
"""

    resposta = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{
            "role": "user",
            "content": f"""Voce e um captador de clientes de um escritorio juridico especializado em:
- BPC LOAS (Beneficio de Prestacao Continuada para idosos e pessoas com deficiencia)
- Inventario e sucessoes (partilha de bens apos falecimento)
- Licitacoes e contratos administrativos (empresas participando de licitacoes publicas)

SEU OBJETIVO PRINCIPAL: Transformar o contato em cliente com contrato fechado.

REGRAS OBRIGATORIAS:
1. NUNCA use emojis nas respostas ao cliente
2. Adapte a linguagem: se o cliente escreve simples, responda simples. Se escreve formal, responda formal
3. Seja curto e objetivo. Maximo 4 linhas por resposta
4. NUNCA resolva o problema completamente. De informacao suficiente para gerar interesse e necessidade de contratar
5. Crie senso de urgencia sutil quando pertinente (prazos, riscos de nao agir)
6. SEMPRE termine com um proximo passo concreto
7. Quando o cliente demonstrar interesse em consulta, pergunte como prefere: por ligacao, por mensagem/audio no WhatsApp ou presencialmente
8. Se preferir presencial, peca as datas disponiveis dele e informe que verificara a agenda
9. Para clientes fora da area: seja acolhedor, informe que nao e sua especialidade mas que pode indicar um colega especialista

CLASSIFICACOES:
- "cliente_nossa_area": busca servicos nas nossas areas
- "cliente_fora_area": busca servicos juridicos em outras areas
- "conversa_pessoal": conversa cotidiana, nao e cliente
- "irrelevante": spam ou sem sentido

{instrucao_feedback}

Responda SOMENTE com JSON valido:

Sem feedback:
{{"categoria": "cliente_nossa_area", "urgencia": "alta/media/baixa", "area": "bpc_loas/inventario/licitacoes/geral/fora_da_area", "perfil": "simples/formal", "resposta": "sua resposta aqui"}}

Com feedback (3 opcoes):
{{"categoria": "cliente_nossa_area", "urgencia": "alta/media/baixa", "area": "bpc_loas/inventario/licitacoes/geral/fora_da_area", "perfil": "simples/formal", "resposta": "opcao 1 aqui", "opcoes": ["opcao 1 aqui", "opcao 2 aqui", "opcao 3 aqui"]}}

Mensagem do cliente: {texto}"""
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

    if data.get("isGroup") or data.get("isNewsletter"):
        return {"status": "ignorado - grupo"}

    if data.get("isStatusReply"):
        return {"status": "ignorado - status"}

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
    categoria = analise.get("categoria", "irrelevante")

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

@app.post("/rejeitar/{telefone}")
async def rejeitar(telefone: str, request: Request):
    data = await request.json()
    feedback = data.get("feedback", "")

    if telefone not in mensagens_pendentes:
        return {"erro": "mensagem não encontrada"}

    mensagem_original = mensagens_pendentes[telefone]["mensagem_original"]
    nova_analise = analisar_mensagem(mensagem_original, feedback=feedback)

    mensagens_pendentes[telefone]["analise"] = nova_analise
    mensagens_pendentes[telefone]["status"] = "pendente"

    return {"status": "novas_opcoes", "analise": nova_analise}

@app.get("/painel")
async def painel():
    return FileResponse("painel.html")

@app.get("/")
async def root():
    return {"status": "servidor rodando"}