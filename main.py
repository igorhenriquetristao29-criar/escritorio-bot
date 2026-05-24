from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
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
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

mensagens_pendentes = {}
IGOR = "5564981475621"
LETICIA = "5564981177107"

# Usa volume Railway (/data) se disponível, senão pasta local
DATA_DIR = "/data" if os.path.isdir("/data") else "."
DB_PATH = os.path.join(DATA_DIR, "mensagens.db")

# ─── BANCO DE DADOS ───────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS mensagens (
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
        fora_horario INTEGER DEFAULT 0,
        criado_em TEXT DEFAULT (datetime('now', '-3 hours'))
    )''')
    # Adiciona colunas que podem não existir em bancos antigos
    for col in [
        "ALTER TABLE mensagens ADD COLUMN fora_horario INTEGER DEFAULT 0",
    ]:
        try:
            c.execute(col)
            conn.commit()
        except:
            pass
    c.execute('''CREATE TABLE IF NOT EXISTS notas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        telefone TEXT NOT NULL UNIQUE,
        texto TEXT DEFAULT '',
        atualizado_em TEXT DEFAULT (datetime('now', '-3 hours'))
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS configuracoes (
        chave TEXT PRIMARY KEY,
        valor TEXT
    )''')
    conn.commit()
    conn.close()

def salvar_mensagem_db(dados, fora_horario=False):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        analise = dados.get("analise", {})
        c.execute('''
            INSERT INTO mensagens
              (telefone, nome, foto, mensagem_original, urgencia, area, categoria, resposta_sugerida, status, fora_horario)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pendente', ?)
        ''', (
            dados["telefone"], dados["nome"], dados.get("foto", ""),
            dados["mensagem_original"],
            analise.get("urgencia", "baixa"), analise.get("area", "geral"),
            analise.get("categoria", "irrelevante"), analise.get("resposta", ""),
            1 if fora_horario else 0,
        ))
        rowid = c.lastrowid
        conn.commit()
        conn.close()
        return rowid
    except Exception as e:
        print(f"Erro ao salvar no banco: {e}")
        return None

def atualizar_status_db(msg_id, status, resposta_enviada=None):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        if resposta_enviada:
            c.execute('UPDATE mensagens SET status=?, resposta_enviada=? WHERE id=?',
                      (status, resposta_enviada, msg_id))
        else:
            c.execute('UPDATE mensagens SET status=? WHERE id=?', (status, msg_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Erro ao atualizar banco: {e}")

def carregar_pendentes_do_db():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            SELECT id, telefone, nome, foto, mensagem_original, urgencia, area, categoria, resposta_sugerida, fora_horario
            FROM mensagens WHERE status = 'pendente' ORDER BY criado_em ASC
        ''')
        rows = c.fetchall()
        conn.close()
        for row in rows:
            db_id, telefone, nome, foto, msg_original, urgencia, area, categoria, resposta, fora_h = row
            chave = str(db_id)
            mensagens_pendentes[chave] = {
                "id": chave, "telefone": telefone,
                "nome": nome or "Cliente", "foto": foto or "",
                "mensagem_original": msg_original or "",
                "analise": {
                    "categoria": categoria or "cliente_nossa_area",
                    "urgencia": urgencia or "baixa",
                    "area": area or "geral",
                    "resposta": resposta or ""
                },
                "fora_horario": bool(fora_h),
                "status": "pendente"
            }
        print(f"Banco carregado: {len(rows)} mensagens pendentes restauradas.")
    except Exception as e:
        print(f"Erro ao carregar banco: {e}")

def buscar_historico_conversa(telefone):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            SELECT mensagem_original, resposta_enviada FROM mensagens
            WHERE telefone=? AND status='enviado' AND resposta_enviada IS NOT NULL
            ORDER BY criado_em ASC
        ''', (telefone,))
        rows = c.fetchall()
        conn.close()
        return rows
    except Exception as e:
        print(f"Erro ao buscar histórico: {e}")
        return []

def buscar_relatorios():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM mensagens')
        total = c.fetchone()[0]
        c.execute('SELECT area, COUNT(*) FROM mensagens GROUP BY area ORDER BY 2 DESC')
        por_area = [{"area": r[0], "total": r[1]} for r in c.fetchall()]
        c.execute('''SELECT substr(criado_em,1,7) as mes, COUNT(*) FROM mensagens
                     GROUP BY mes ORDER BY mes DESC LIMIT 12''')
        por_mes = [{"mes": r[0], "total": r[1]} for r in c.fetchall()]
        c.execute('SELECT status, COUNT(*) FROM mensagens GROUP BY status')
        por_status = [{"status": r[0], "total": r[1]} for r in c.fetchall()]
        c.execute('''SELECT telefone, nome, mensagem_original, area, urgencia, status, criado_em, fora_horario
                     FROM mensagens ORDER BY id DESC LIMIT 50''')
        ultimas = [{"telefone": r[0], "nome": r[1], "mensagem": r[2], "area": r[3],
                    "urgencia": r[4], "status": r[5], "data": r[6], "fora_horario": bool(r[7])}
                   for r in c.fetchall()]
        conn.close()
        return {"total": total, "por_area": por_area, "por_mes": por_mes,
                "por_status": por_status, "ultimas": ultimas}
    except Exception as e:
        print(f"Erro ao buscar relatórios: {e}")
        return {"total": 0, "por_area": [], "por_mes": [], "por_status": [], "ultimas": []}

# ─── CONFIGURAÇÕES ─────────────────────────────────────────────────────────────

def get_config(chave, padrao=""):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT valor FROM configuracoes WHERE chave=?', (chave,))
        row = c.fetchone()
        conn.close()
        return row[0] if row else padrao
    except:
        return padrao

def set_config(chave, valor):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO configuracoes (chave, valor) VALUES (?,?)
                     ON CONFLICT(chave) DO UPDATE SET valor=excluded.valor''', (chave, valor))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Erro ao salvar config: {e}")

# Inicializa banco ao subir
init_db()
carregar_pendentes_do_db()

# ─── HORÁRIO DE ATENDIMENTO ────────────────────────────────────────────────────

def dentro_do_horario():
    if os.environ.get("MODO_TESTE") == "1":
        return True
    brasilia = datetime.timezone(datetime.timedelta(hours=-3))
    agora = datetime.datetime.now(brasilia)
    if agora.weekday() >= 5:
        return False
    hora_inicio = int(get_config("HORA_INICIO", os.environ.get("HORA_INICIO", "8")))
    hora_fim    = int(get_config("HORA_FIM",    os.environ.get("HORA_FIM",    "18")))
    return hora_inicio <= agora.hour < hora_fim

def get_msg_fora_horario():
    return get_config("MSG_FORA_HORARIO",
        "Olá! Obrigado pelo contato com o escritório Letícia Marques Advocacia. "
        "Nosso horário de atendimento é de segunda a sexta, das 8h às 18h. "
        "Retornaremos assim que possível.")

# ─── NOTIFICAÇÕES ──────────────────────────────────────────────────────────────

def notificar_equipe(nome, mensagem, urgencia, area, categoria, fora_horario=False):
    instance     = os.environ["ZAPI_INSTANCE"]
    token        = os.environ["ZAPI_TOKEN"]
    client_token = os.environ["ZAPI_CLIENT_TOKEN"]
    url     = f"https://api.z-api.io/instances/{instance}/token/{token}/send-text"
    headers = {"Client-Token": client_token}

    if fora_horario:
        tag = "FORA DO HORARIO"
    elif categoria == "cliente_fora_area":
        tag = "FORA DA AREA"
    elif urgencia == "alta":
        tag = "URGENTE"
    elif urgencia == "media":
        tag = "NORMAL"
    else:
        tag = "BAIXA PRIORIDADE"

    aviso_fora = "\n(Auto-resposta já enviada — aguarda aprovação no painel)" if fora_horario else ""
    texto = f"""{tag} - Nova mensagem!

Cliente: {nome}
Mensagem: "{mensagem}"
Area: {area.upper()}{aviso_fora}

Painel:
https://web-production-444ef9.up.railway.app/painel"""

    for numero in [IGOR, LETICIA]:
        requests.post(url, json={"phone": numero, "message": texto}, headers=headers)

# ─── CLAUDE ───────────────────────────────────────────────────────────────────

def analisar_mensagem(texto, feedback=None, historico=None):
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    if feedback:
        instrucao_feedback = (
            f'IMPORTANTE: A resposta anterior foi rejeitada. Feedback da equipe: "{feedback}". '
            'Gere 3 opções diferentes considerando esse feedback. '
            'Coloque as 3 no campo "opcoes" e repita a melhor no campo "resposta".'
        )
        formato_json = '{{"categoria":"...","urgencia":"alta/media/baixa","area":"bpc_loas/inventario/licitacoes/geral/fora_da_area","perfil":"simples/formal","resposta":"melhor opcao","opcoes":["opcao 1","opcao 2","opcao 3"]}}'
    else:
        instrucao_feedback = ""
        formato_json = '{{"categoria":"...","urgencia":"alta/media/baixa","area":"bpc_loas/inventario/licitacoes/geral/fora_da_area","perfil":"simples/formal","resposta":"sua resposta ao cliente aqui"}}'

    if historico:
        linhas = []
        for msg_c, resp_e in historico:
            linhas.append(f'  Cliente: "{msg_c}"')
            linhas.append(f'  Escritório: "{resp_e}"')
        contexto_historico = (
            "HISTÓRICO DESTA CONVERSA (mensagens anteriores com este cliente):\n"
            + "\n".join(linhas)
            + "\n\nConsidere o histórico para dar continuidade natural à conversa.\n\n"
        )
    else:
        contexto_historico = ""

    resposta = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{"role": "user", "content": f"""{contexto_historico}Você é um captador de clientes de um escritório jurídico especializado em:
- BPC LOAS (Benefício de Prestação Continuada para idosos e pessoas com deficiência)
- Inventário e sucessões (partilha de bens após falecimento)
- Licitações e contratos administrativos (empresas participando de licitações públicas)

SEU OBJETIVO PRINCIPAL: Transformar o contato em cliente com contrato fechado.

REGRAS OBRIGATÓRIAS:
1. NUNCA use emojis nas respostas ao cliente
2. Use SEMPRE português brasileiro correto: acentuação, pontuação, ortografia e gramática impecáveis
3. Adapte a linguagem: simples se o cliente escreve simples; formal se escreve formal
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
{formato_json}"""}]
    )

    texto_resposta = resposta.content[0].text.strip()
    match = re.search(r'\{.*\}', texto_resposta, re.DOTALL)
    if match:
        return json.loads(match.group())
    return {"categoria": "irrelevante", "urgencia": "baixa", "area": "geral", "perfil": "simples", "resposta": ""}

# ─── Z-API ────────────────────────────────────────────────────────────────────

def enviar_whatsapp(telefone, mensagem):
    instance     = os.environ["ZAPI_INSTANCE"]
    token        = os.environ["ZAPI_TOKEN"]
    client_token = os.environ["ZAPI_CLIENT_TOKEN"]
    url     = f"https://api.z-api.io/instances/{instance}/token/{token}/send-text"
    headers = {"Client-Token": client_token}
    r = requests.post(url, json={"phone": telefone, "message": mensagem}, headers=headers)
    print(f"Z-API resposta: {r.status_code} - {r.text}")

# ─── FORA DO HORÁRIO ───────────────────────────────────────────────────────────

def processar_mensagem_fora_horario(telefone, texto, nome, foto):
    """Processa em background mensagens recebidas fora do horário."""
    try:
        historico = buscar_historico_conversa(telefone)
        analise   = analisar_mensagem(texto, historico=historico)
        categoria = analise.get("categoria", "irrelevante")

        if categoria in ["conversa_pessoal", "irrelevante"]:
            return

        msg = {
            "telefone": telefone, "nome": nome, "foto": foto or "",
            "mensagem_original": texto, "analise": analise,
            "fora_horario": True, "status": "pendente"
        }
        db_id = salvar_mensagem_db(msg, fora_horario=True)
        if db_id:
            chave = str(db_id)
            msg["id"] = chave
            mensagens_pendentes[chave] = msg

        notificar_equipe(nome, texto, analise["urgencia"], analise["area"], categoria, fora_horario=True)
    except Exception as e:
        print(f"Erro ao processar fora do horário: {e}")

# ─── ROTAS ────────────────────────────────────────────────────────────────────

@app.post("/login")
async def login(request: Request):
    data = await request.json()
    if data.get("senha", "") == os.environ.get("PAINEL_SENHA", "Afra1988"):
        return {"ok": True}
    raise HTTPException(status_code=401, detail="Senha incorreta")

@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()

    if data.get("isGroup") or data.get("isNewsletter"):
        return {"status": "ignorado - grupo"}
    if data.get("isStatusReply"):
        return {"status": "ignorado - status"}
    if data.get("type") != "ReceivedCallback":
        return {"status": "ignorado"}

    telefone = data.get("phone", "")
    texto    = data.get("text", {}).get("message", "")
    nome     = data.get("senderName", "Cliente")
    foto     = data.get("photo", "")

    if not texto:
        return {"status": "sem texto"}
    if telefone in [IGOR, LETICIA]:
        return {"status": "ignorado - equipe"}

    if not dentro_do_horario():
        print(f"FORA DO HORARIO: {nome} - {texto}")
        enviar_whatsapp(telefone, get_msg_fora_horario())
        background_tasks.add_task(processar_mensagem_fora_horario, telefone, texto, nome, foto)
        return {"status": "fora do horario - auto-resposta enviada"}

    historico = buscar_historico_conversa(telefone)
    analise   = analisar_mensagem(texto, historico=historico)
    categoria = analise.get("categoria", "irrelevante")

    if categoria in ["conversa_pessoal", "irrelevante"]:
        print(f"IGNORADO ({categoria}): {nome} - {texto}")
        return {"status": f"ignorado - {categoria}"}

    msg = {
        "telefone": telefone, "nome": nome, "foto": foto,
        "mensagem_original": texto, "analise": analise,
        "fora_horario": False, "status": "pendente"
    }
    db_id = salvar_mensagem_db(msg)
    if db_id:
        chave = str(db_id)
        msg["id"] = chave
        mensagens_pendentes[chave] = msg

    notificar_equipe(nome, texto, analise["urgencia"], analise["area"], categoria)
    return {"status": "recebido"}

@app.get("/pendentes")
async def listar_pendentes():
    return [m for m in mensagens_pendentes.values() if m["status"] == "pendente"]

@app.post("/aprovar/{msg_id}")
async def aprovar(msg_id: str, request: Request):
    data = await request.json()
    if msg_id not in mensagens_pendentes:
        return {"erro": "Mensagem não encontrada — pode já ter sido aprovada por outro usuário."}
    telefone = mensagens_pendentes[msg_id]["telefone"]
    mensagem_final = data.get("mensagem", "")
    enviar_whatsapp(telefone, mensagem_final)
    del mensagens_pendentes[msg_id]
    atualizar_status_db(int(msg_id), "enviado", mensagem_final)
    return {"status": "enviado"}

@app.post("/rejeitar/{msg_id}")
async def rejeitar(msg_id: str, request: Request):
    data = await request.json()
    if msg_id not in mensagens_pendentes:
        return {"erro": "mensagem não encontrada"}
    mensagem_original = mensagens_pendentes[msg_id]["mensagem_original"]
    nova_analise = analisar_mensagem(mensagem_original, feedback=data.get("feedback", ""))
    mensagens_pendentes[msg_id]["analise"] = nova_analise
    return {"status": "novas_opcoes", "analise": nova_analise}

@app.post("/contratar/{msg_id}")
async def contratar(msg_id: str):
    if msg_id not in mensagens_pendentes:
        return {"erro": "mensagem não encontrada"}
    atualizar_status_db(int(msg_id), "contrato")
    del mensagens_pendentes[msg_id]
    return {"status": "contrato registrado"}

@app.get("/nota/{telefone}")
async def get_nota(telefone: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT texto FROM notas WHERE telefone=?', (telefone,))
        row = c.fetchone()
        conn.close()
        return {"texto": row[0] if row else ""}
    except:
        return {"texto": ""}

@app.post("/nota/{telefone}")
async def salvar_nota(telefone: str, request: Request):
    data = await request.json()
    texto = data.get("texto", "")
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO notas (telefone, texto) VALUES (?,?)
                     ON CONFLICT(telefone) DO UPDATE SET texto=excluded.texto,
                     atualizado_em=datetime('now','-3 hours')''', (telefone, texto))
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"erro": str(e)}

@app.get("/config")
async def get_configuracoes():
    return {
        "MSG_FORA_HORARIO": get_msg_fora_horario(),
        "HORA_INICIO": get_config("HORA_INICIO", "8"),
        "HORA_FIM":    get_config("HORA_FIM",    "18"),
    }

@app.post("/config")
async def salvar_configuracoes(request: Request):
    data = await request.json()
    for chave in ["MSG_FORA_HORARIO", "HORA_INICIO", "HORA_FIM"]:
        if chave in data:
            set_config(chave, str(data[chave]))
    return {"ok": True}

@app.get("/relatorios-dados")
async def relatorios_dados():
    return buscar_relatorios()

@app.get("/painel")
async def painel():
    return FileResponse("painel.html")

@app.get("/relatorios")
async def relatorios_page():
    return FileResponse("relatorios.html")

@app.get("/configuracoes")
async def configuracoes_page():
    return FileResponse("configuracoes.html")

@app.get("/")
async def root():
    return {"status": "servidor rodando"}
