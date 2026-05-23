from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import anthropic
import requests
import os
import json
import re
import sqlite3
import datetime

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
DB_PATH = "mensagens.db"

# ─── BANCO DE DADOS ────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS mensagens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telefone TEXT NOT NULL,
            nome TEXT,
            foto TEXT,
            mensagem_original TEXT,
            urgencia TEXT,
            area TEXT,
            categoria TEXT,
            resposta_sugerida TEXT,
            resposta_enviada TEXT,
            status TEXT DEFAULT 'pendente',
            criado_em TEXT DEFAULT (datetime('now', '-3 hours'))
        )
    ''')
    conn.commit()
    conn.close()

def salvar_mensagem_db(dados):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        analise = dados.get("analise", {})
        c.execute('''
            INSERT INTO mensagens (telefone, nome, foto, mensagem_original, urgencia, area, categoria, resposta_sugerida, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pendente')
        ''', (
            dados["telefone"],
            dados["nome"],
            dados.get("foto", ""),
            dados["mensagem_original"],
            analise.get("urgencia", "baixa"),
            analise.get("area", "geral"),
            analise.get("categoria", "irrelevante"),
            analise.get("resposta", ""),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Erro ao salvar no banco: {e}")

def atualizar_status_db(telefone, status, resposta_enviada=None):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            SELECT id FROM mensagens WHERE telefone = ? AND status = 'pendente'
            ORDER BY criado_em DESC LIMIT 1
        ''', (telefone,))
        row = c.fetchone()
        if row:
            msg_id = row[0]
            if resposta_enviada:
                c.execute(
                    'UPDATE mensagens SET status = ?, resposta_enviada = ? WHERE id = ?',
                    (status, resposta_enviada, msg_id)
                )
            else:
                c.execute('UPDATE mensagens SET status = ? WHERE id = ?', (status, msg_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Erro ao atualizar banco: {e}")

def carregar_pendentes_do_db():
    """Carrega mensagens pendentes do banco ao iniciar — preserva histórico entre reinicializações."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            SELECT telefone, nome, foto, mensagem_original, urgencia, area, categoria, resposta_sugerida
            FROM mensagens WHERE status = 'pendente'
            ORDER BY criado_em ASC
        ''')
        rows = c.fetchall()
        conn.close()
        for row in rows:
            telefone, nome, foto, msg_original, urgencia, area, categoria, resposta = row
            mensagens_pendentes[telefone] = {
                "telefone": telefone,
                "nome": nome or "Cliente",
                "foto": foto or "",
                "mensagem_original": msg_original or "",
                "analise": {
                    "categoria": categoria or "cliente_nossa_area",
                    "urgencia": urgencia or "baixa",
                    "area": area or "geral",
                    "resposta": resposta or ""
                },
                "status": "pendente"
            }
        print(f"Banco carregado: {len(rows)} mensagens pendentes restauradas.")
    except Exception as e:
        print(f"Erro ao carregar banco: {e}")

def buscar_relatorios():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute('SELECT COUNT(*) FROM mensagens')
        total = c.fetchone()[0]

        c.execute('''
            SELECT area, COUNT(*) as total FROM mensagens
            GROUP BY area ORDER BY total DESC
        ''')
        por_area = [{"area": r[0], "total": r[1]} for r in c.fetchall()]

        c.execute('''
            SELECT substr(criado_em, 1, 7) as mes, COUNT(*) as total
            FROM mensagens GROUP BY mes ORDER BY mes DESC LIMIT 12
        ''')
        por_mes = [{"mes": r[0], "total": r[1]} for r in c.fetchall()]

        c.execute('SELECT status, COUNT(*) as total FROM mensagens GROUP BY status')
        por_status = [{"status": r[0], "total": r[1]} for r in c.fetchall()]

        c.execute('''
            SELECT telefone, nome, mensagem_original, area, urgencia, status, criado_em
            FROM mensagens ORDER BY id DESC LIMIT 50
        ''')
        ultimas = [{
            "telefone": r[0], "nome": r[1], "mensagem": r[2],
            "area": r[3], "urgencia": r[4], "status": r[5], "data": r[6]
        } for r in c.fetchall()]

        conn.close()
        return {
            "total": total,
            "por_area": por_area,
            "por_mes": por_mes,
            "por_status": por_status,
            "ultimas": ultimas
        }
    except Exception as e:
        print(f"Erro ao buscar relatórios: {e}")
        return {"total": 0, "por_area": [], "por_mes": [], "por_status": [], "ultimas": []}

# Inicializa banco e carrega pendentes ao subir o servidor
init_db()
carregar_pendentes_do_db()

# ─── HORÁRIO DE ATENDIMENTO ────────────────────────────────────────────────────

def dentro_do_horario():
    # Modo teste: ignora horário e fim de semana
    if os.environ.get("MODO_TESTE") == "1":
        return True

    brasilia = datetime.timezone(datetime.timedelta(hours=-3))
    agora = datetime.datetime.now(brasilia)

    # 0=segunda ... 4=sexta, 5=sábado, 6=domingo
    if agora.weekday() >= 5:
        return False

    hora_inicio = int(os.environ.get("HORA_INICIO", "8"))
    hora_fim = int(os.environ.get("HORA_FIM", "18"))

    return hora_inicio <= agora.hour < hora_fim

MSG_FORA_HORARIO = (
    "Olá! Obrigado pelo contato com o escritório Letícia Marques Advocacia. "
    "Nosso horário de atendimento é de segunda a sexta, das 8h às 18h. "
    "Retornaremos assim que possível."
)

# ─── NOTIFICAÇÕES ──────────────────────────────────────────────────────────────

def notificar_equipe(nome, mensagem, urgencia, area, categoria):
    instance = os.environ["ZAPI_INSTANCE"]
    token = os.environ["ZAPI_TOKEN"]
    client_token = os.environ["ZAPI_CLIENT_TOKEN"]
    url = f"https://api.z-api.io/instances/{instance}/token/{token}/send-text"
    headers = {"Client-Token": client_token}

    if categoria == "cliente_fora_area":
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

# ─── CLAUDE ───────────────────────────────────────────────────────────────────

def analisar_mensagem(texto, feedback=None):
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    if feedback:
        instrucao_feedback = f'IMPORTANTE: A resposta anterior foi rejeitada. Feedback da equipe: "{feedback}". Gere 3 opções diferentes de resposta considerando esse feedback. Coloque todas as 3 no campo "opcoes" e repita a melhor no campo "resposta".'
        formato_json = '{{"categoria": "...", "urgencia": "alta/media/baixa", "area": "bpc_loas/inventario/licitacoes/geral/fora_da_area", "perfil": "simples/formal", "resposta": "melhor opcao aqui", "opcoes": ["opcao 1", "opcao 2", "opcao 3"]}}'
    else:
        instrucao_feedback = ""
        formato_json = '{{"categoria": "...", "urgencia": "alta/media/baixa", "area": "bpc_loas/inventario/licitacoes/geral/fora_da_area", "perfil": "simples/formal", "resposta": "sua resposta ao cliente aqui"}}'

    resposta = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{
            "role": "user",
            "content": f"""Você é um captador de clientes de um escritório jurídico especializado em:
- BPC LOAS (Benefício de Prestação Continuada para idosos e pessoas com deficiência)
- Inventário e sucessões (partilha de bens após falecimento)
- Licitações e contratos administrativos (empresas participando de licitações públicas)

SEU OBJETIVO PRINCIPAL: Transformar o contato em cliente com contrato fechado.

REGRAS OBRIGATÓRIAS:
1. NUNCA use emojis nas respostas ao cliente
2. Use SEMPRE português brasileiro correto: acentuação, pontuação, ortografia e gramática impecáveis
3. Adapte a linguagem: se o cliente escreve simples, responda simples; se escreve formal, responda formal
4. Seja curto e objetivo. Máximo 4 linhas por resposta
5. NUNCA resolva o problema completamente — gere interesse e necessidade de contratar
6. Crie senso de urgência sutil quando pertinente (prazos, riscos de não agir)
7. SEMPRE termine com um próximo passo concreto
8. Quando o cliente quiser consulta, pergunte como prefere: ligação, WhatsApp/áudio ou presencialmente
9. Se preferir presencial, peça as datas disponíveis e informe que verificará a agenda
10. Para clientes fora da área: seja acolhedor e ofereça indicar um colega especialista

CLASSIFICAÇÕES possíveis:
- "cliente_nossa_area": busca serviços nas nossas áreas
- "cliente_fora_area": busca serviços jurídicos em outras áreas
- "conversa_pessoal": conversa cotidiana, não é cliente
- "irrelevante": spam ou sem sentido

{instrucao_feedback}

Mensagem do cliente: {texto}

Responda SOMENTE com JSON válido, sem explicações, sem markdown:
{formato_json}"""
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

# ─── Z-API ────────────────────────────────────────────────────────────────────

def enviar_whatsapp(telefone, mensagem):
    instance = os.environ["ZAPI_INSTANCE"]
    token = os.environ["ZAPI_TOKEN"]
    client_token = os.environ["ZAPI_CLIENT_TOKEN"]
    url = f"https://api.z-api.io/instances/{instance}/token/{token}/send-text"
    headers = {"Client-Token": client_token}
    r = requests.post(url, json={"phone": telefone, "message": mensagem}, headers=headers)
    print(f"Z-API resposta: {r.status_code} - {r.text}")

# ─── ROTAS ────────────────────────────────────────────────────────────────────

@app.post("/login")
async def login(request: Request):
    data = await request.json()
    senha = data.get("senha", "")
    senha_correta = os.environ.get("PAINEL_SENHA", "Afra1988")
    if senha == senha_correta:
        return {"ok": True}
    raise HTTPException(status_code=401, detail="Senha incorreta")

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

    # Verifica horário de atendimento
    if not dentro_do_horario():
        print(f"FORA DO HORARIO: {nome} - {texto}")
        enviar_whatsapp(telefone, MSG_FORA_HORARIO)
        return {"status": "fora do horario - resposta automatica enviada"}

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

    salvar_mensagem_db(mensagens_pendentes[telefone])
    notificar_equipe(nome, texto, analise["urgencia"], analise["area"], categoria)

    return {"status": "recebido"}

@app.get("/pendentes")
async def listar_pendentes():
    return [m for m in mensagens_pendentes.values() if m["status"] == "pendente"]

@app.post("/aprovar/{telefone}")
async def aprovar(telefone: str, request: Request):
    data = await request.json()
    mensagem_final = data.get("mensagem", "")

    if telefone not in mensagens_pendentes:
        return {"erro": "mensagem não encontrada"}

    enviar_whatsapp(telefone, mensagem_final)
    mensagens_pendentes[telefone]["status"] = "enviado"
    atualizar_status_db(telefone, "enviado", mensagem_final)

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

@app.get("/relatorios-dados")
async def relatorios_dados():
    return buscar_relatorios()

@app.get("/painel")
async def painel():
    return FileResponse("painel.html")

@app.get("/relatorios")
async def relatorios():
    return FileResponse("relatorios.html")

@app.get("/")
async def root():
    return {"status": "servidor rodando"}
