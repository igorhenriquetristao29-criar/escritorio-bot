from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import anthropic
import requests
import os, json, re, datetime, csv, io, asyncio
import psycopg2
import psycopg2.extras

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

mensagens_pendentes = {}
triagem_pendente    = {}   # { telefone: {stage, nome, foto, criado_em, primeiro_texto} }
IGOR    = "5564981475621"
LETICIA = "5564981177107"
API_BASE = "https://web-production-3c5ee.up.railway.app"

# Preços Claude Sonnet (USD por token)
PRECO_INPUT  = 3.0  / 1_000_000   # $3 por milhão de tokens de entrada
PRECO_OUTPUT = 15.0 / 1_000_000   # $15 por milhão de tokens de saída

# ─── BANCO DE DADOS ────────────────────────────────────────────────────────────

def get_conn():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise Exception("DATABASE_URL não configurada. Adicione o PostgreSQL no Railway.")
    return psycopg2.connect(db_url)

def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS mensagens (
        id                SERIAL PRIMARY KEY,
        telefone          TEXT NOT NULL,
        nome              TEXT,
        foto              TEXT,
        mensagem_original TEXT,
        urgencia          TEXT,
        area              TEXT,
        categoria         TEXT,
        resposta_sugerida TEXT,
        resposta_enviada  TEXT,
        status            TEXT DEFAULT 'pendente',
        fora_horario      BOOLEAN DEFAULT FALSE,
        funil_status      TEXT DEFAULT 'novo',
        retorno_cliente   BOOLEAN DEFAULT FALSE,
        aprovado_por      TEXT,
        criado_em         TIMESTAMP DEFAULT NOW()
    )''')

    # Colunas novas em tabelas existentes (seguro rodar várias vezes)
    for col_sql in [
        "ALTER TABLE mensagens ADD COLUMN IF NOT EXISTS funil_status TEXT DEFAULT 'novo'",
        "ALTER TABLE mensagens ADD COLUMN IF NOT EXISTS retorno_cliente BOOLEAN DEFAULT FALSE",
        "ALTER TABLE mensagens ADD COLUMN IF NOT EXISTS aprovado_por TEXT",
        "ALTER TABLE mensagens ADD COLUMN IF NOT EXISTS respondido_em TIMESTAMP",
        "ALTER TABLE mensagens ADD COLUMN IF NOT EXISTS follow_up_enviado BOOLEAN DEFAULT FALSE",
    ]:
        c.execute(col_sql)

    c.execute('''CREATE TABLE IF NOT EXISTS notas (
        id            SERIAL PRIMARY KEY,
        telefone      TEXT NOT NULL UNIQUE,
        texto         TEXT DEFAULT '',
        atualizado_em TIMESTAMP DEFAULT NOW()
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS configuracoes (
        chave TEXT PRIMARY KEY,
        valor TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS modelos (
        id        SERIAL PRIMARY KEY,
        titulo    TEXT NOT NULL,
        texto     TEXT NOT NULL,
        criado_em TIMESTAMP DEFAULT NOW()
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS uso_claude (
        id             SERIAL PRIMARY KEY,
        msg_id         INTEGER,
        tokens_entrada INTEGER DEFAULT 0,
        tokens_saida   INTEGER DEFAULT 0,
        custo_usd      NUMERIC(10,6) DEFAULT 0,
        criado_em      TIMESTAMP DEFAULT NOW()
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS lembretes (
        id            SERIAL PRIMARY KEY,
        msg_id        INTEGER,
        telefone      TEXT NOT NULL,
        nome          TEXT,
        texto         TEXT,
        data_lembrete DATE NOT NULL,
        ativo         BOOLEAN DEFAULT TRUE,
        criado_em     TIMESTAMP DEFAULT NOW()
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS prazos (
        id                 SERIAL PRIMARY KEY,
        processo           TEXT,
        cliente            TEXT NOT NULL,
        tipo               TEXT,
        descricao          TEXT,
        data_prazo         DATE NOT NULL,
        responsavel        TEXT DEFAULT 'Equipe',
        alerta_7d_enviado  BOOLEAN DEFAULT FALSE,
        alerta_3d_enviado  BOOLEAN DEFAULT FALSE,
        alerta_1d_enviado  BOOLEAN DEFAULT FALSE,
        alerta_dia_enviado BOOLEAN DEFAULT FALSE,
        ativo              BOOLEAN DEFAULT TRUE,
        criado_em          TIMESTAMP DEFAULT NOW()
    )''')

    conn.commit()
    c.close()
    conn.close()
    print("Banco PostgreSQL iniciado com sucesso.")

def salvar_mensagem_db(dados, fora_horario=False):
    try:
        conn = get_conn()
        c = conn.cursor()
        analise  = dados.get("analise", {})
        telefone = dados["telefone"]

        # Verifica se é cliente recorrente
        c.execute('SELECT COUNT(*) FROM mensagens WHERE telefone=%s', (telefone,))
        retorno = c.fetchone()[0] > 0

        c.execute('''
            INSERT INTO mensagens
              (telefone, nome, foto, mensagem_original, urgencia, area, categoria,
               resposta_sugerida, status, fora_horario, retorno_cliente)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'pendente',%s,%s)
            RETURNING id
        ''', (
            telefone, dados["nome"], dados.get("foto",""),
            dados["mensagem_original"],
            analise.get("urgencia","baixa"), analise.get("area","geral"),
            analise.get("categoria","irrelevante"), analise.get("resposta",""),
            fora_horario, retorno,
        ))
        rowid = c.fetchone()[0]
        conn.commit()
        c.close()
        conn.close()
        return rowid, retorno
    except Exception as e:
        print(f"Erro ao salvar no banco: {e}")
        return None, False

def atualizar_status_db(msg_id, status, resposta_enviada=None, aprovado_por=None):
    try:
        conn = get_conn()
        c = conn.cursor()
        if resposta_enviada and aprovado_por:
            c.execute('''UPDATE mensagens SET status=%s, resposta_enviada=%s,
                         aprovado_por=%s, respondido_em=NOW() WHERE id=%s''',
                      (status, resposta_enviada, aprovado_por, msg_id))
        elif resposta_enviada:
            c.execute('''UPDATE mensagens SET status=%s, resposta_enviada=%s,
                         respondido_em=NOW() WHERE id=%s''',
                      (status, resposta_enviada, msg_id))
        else:
            c.execute('UPDATE mensagens SET status=%s WHERE id=%s', (status, msg_id))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        print(f"Erro ao atualizar banco: {e}")

def carregar_pendentes_do_db():
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('''
            SELECT id, telefone, nome, foto, mensagem_original, urgencia, area, categoria,
                   resposta_sugerida, fora_horario, funil_status, retorno_cliente,
                   TO_CHAR(criado_em - INTERVAL '3 hours', 'YYYY-MM-DD HH24:MI:SS')
            FROM mensagens WHERE status='pendente' ORDER BY criado_em ASC
        ''')
        rows = c.fetchall()
        c.close()
        conn.close()
        for row in rows:
            db_id, telefone, nome, foto, msg_original, urgencia, area, categoria, \
                resposta, fora_h, funil, retorno, criado_em = row
            chave = str(db_id)
            mensagens_pendentes[chave] = {
                "id": chave, "telefone": telefone,
                "nome": nome or "Cliente", "foto": foto or "",
                "mensagem_original": msg_original or "",
                "analise": {
                    "categoria": categoria or "cliente_nossa_area",
                    "urgencia":  urgencia  or "baixa",
                    "area":      area      or "geral",
                    "resposta":  resposta  or ""
                },
                "fora_horario":    bool(fora_h),
                "funil_status":    funil or "novo",
                "retorno_cliente": bool(retorno),
                "criado_em":       criado_em or "",
                "status": "pendente"
            }
        print(f"Banco carregado: {len(rows)} pendentes restauradas.")
    except Exception as e:
        print(f"Erro ao carregar banco: {e}")

def buscar_historico_conversa(telefone):
    """Histórico resumido para contexto do Claude."""
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('''
            SELECT mensagem_original, resposta_enviada FROM mensagens
            WHERE telefone=%s AND status='enviado' AND resposta_enviada IS NOT NULL
            ORDER BY criado_em ASC
        ''', (telefone,))
        rows = c.fetchall()
        c.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"Erro ao buscar histórico: {e}")
        return []

def buscar_historico_completo(telefone):
    """Histórico completo para modal do painel."""
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('''
            SELECT mensagem_original, resposta_enviada, status, area, urgencia,
                   TO_CHAR(criado_em - INTERVAL '3 hours','DD/MM/YYYY HH24:MI'), aprovado_por
            FROM mensagens WHERE telefone=%s ORDER BY criado_em ASC
        ''', (telefone,))
        rows = c.fetchall()
        c.close()
        conn.close()
        return [{"mensagem": r[0], "resposta": r[1], "status": r[2],
                 "area": r[3], "urgencia": r[4], "data": r[5], "aprovado_por": r[6]}
                for r in rows]
    except Exception as e:
        print(f"Erro ao buscar histórico completo: {e}")
        return []

def buscar_relatorios():
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM mensagens')
        total = c.fetchone()[0]
        c.execute('SELECT area, COUNT(*) FROM mensagens GROUP BY area ORDER BY 2 DESC')
        por_area = [{"area": r[0], "total": r[1]} for r in c.fetchall()]
        c.execute('''SELECT TO_CHAR(criado_em - INTERVAL '3 hours','YYYY-MM') as mes, COUNT(*)
                     FROM mensagens GROUP BY mes ORDER BY mes DESC LIMIT 12''')
        por_mes = [{"mes": r[0], "total": r[1]} for r in c.fetchall()]
        c.execute('SELECT status, COUNT(*) FROM mensagens GROUP BY status')
        por_status = [{"status": r[0], "total": r[1]} for r in c.fetchall()]
        c.execute('''SELECT telefone, nome, mensagem_original, area, urgencia, status,
                            TO_CHAR(criado_em - INTERVAL '3 hours','YYYY-MM-DD HH24:MI'), fora_horario
                     FROM mensagens ORDER BY id DESC LIMIT 50''')
        ultimas = [{"telefone": r[0], "nome": r[1], "mensagem": r[2], "area": r[3],
                    "urgencia": r[4], "status": r[5], "data": r[6], "fora_horario": bool(r[7])}
                   for r in c.fetchall()]
        c.close()
        conn.close()
        return {"total": total, "por_area": por_area, "por_mes": por_mes,
                "por_status": por_status, "ultimas": ultimas}
    except Exception as e:
        print(f"Erro ao buscar relatórios: {e}")
        return {"total": 0, "por_area": [], "por_mes": [], "por_status": [], "ultimas": []}

# ─── CONFIGURAÇÕES ─────────────────────────────────────────────────────────────

def get_config(chave, padrao=""):
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('SELECT valor FROM configuracoes WHERE chave=%s', (chave,))
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row else padrao
    except:
        return padrao

def set_config(chave, valor):
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('''INSERT INTO configuracoes (chave, valor) VALUES (%s,%s)
                     ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor''', (chave, valor))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        print(f"Erro ao salvar config: {e}")

# ─── CONTROLE DE CUSTO CLAUDE ─────────────────────────────────────────────────

def registrar_uso_claude(tokens_entrada, tokens_saida, custo_usd, msg_id=None):
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('''INSERT INTO uso_claude (msg_id, tokens_entrada, tokens_saida, custo_usd)
                     VALUES (%s,%s,%s,%s)''',
                  (msg_id, tokens_entrada, tokens_saida, round(custo_usd, 6)))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        print(f"Erro ao registrar uso Claude: {e}")

def buscar_custo():
    try:
        conn = get_conn()
        c = conn.cursor()
        brasilia = datetime.timezone(datetime.timedelta(hours=-3))
        agora    = datetime.datetime.now(brasilia)
        hoje     = agora.date()
        mes      = agora.strftime('%Y-%m')

        c.execute("""SELECT COALESCE(SUM(custo_usd),0), COUNT(*)
                     FROM uso_claude
                     WHERE DATE(criado_em - INTERVAL '3 hours') = %s""", (hoje,))
        r = c.fetchone()
        custo_hoje, chamadas_hoje = float(r[0]), int(r[1])

        c.execute("""SELECT COALESCE(SUM(custo_usd),0), COUNT(*)
                     FROM uso_claude
                     WHERE TO_CHAR(criado_em - INTERVAL '3 hours','YYYY-MM') = %s""", (mes,))
        r = c.fetchone()
        custo_mes, chamadas_mes = float(r[0]), int(r[1])

        c.execute("SELECT COALESCE(SUM(custo_usd),0), COUNT(*) FROM uso_claude")
        r = c.fetchone()
        custo_total, chamadas_total = float(r[0]), int(r[1])

        c.close()
        conn.close()
        limite = float(get_config("LIMITE_DIARIO_USD", "0"))
        return {
            "custo_hoje":     round(custo_hoje,    4),
            "custo_mes":      round(custo_mes,     4),
            "custo_total":    round(custo_total,   4),
            "chamadas_hoje":  chamadas_hoje,
            "chamadas_mes":   chamadas_mes,
            "chamadas_total": chamadas_total,
            "limite_diario":  limite,
            "limite_ativo":   limite > 0,
            "limite_atingido": limite > 0 and custo_hoje >= limite,
        }
    except Exception as e:
        print(f"Erro ao buscar custo: {e}")
        return {"custo_hoje":0,"custo_mes":0,"custo_total":0,
                "chamadas_hoje":0,"chamadas_mes":0,"chamadas_total":0,
                "limite_diario":0,"limite_ativo":False,"limite_atingido":False}

def dentro_do_limite():
    """Retorna False se o limite diário foi atingido."""
    limite = float(get_config("LIMITE_DIARIO_USD", "0"))
    if limite <= 0:
        return True
    dados = buscar_custo()
    return dados["custo_hoje"] < limite

# Inicializa banco ao subir
try:
    init_db()
    carregar_pendentes_do_db()
except Exception as e:
    print(f"AVISO: Banco não inicializado — {e}")

# ─── HORÁRIO DE ATENDIMENTO ────────────────────────────────────────────────────

def dentro_do_horario():
    if os.environ.get("MODO_TESTE") == "1":
        return True
    brasilia = datetime.timezone(datetime.timedelta(hours=-3))
    agora    = datetime.datetime.now(brasilia)
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

    if fora_horario:                        tag = "FORA DO HORARIO"
    elif categoria == "cliente_fora_area":  tag = "FORA DA AREA"
    elif urgencia == "alta":                tag = "URGENTE"
    elif urgencia == "media":               tag = "NORMAL"
    else:                                   tag = "BAIXA PRIORIDADE"

    aviso = "\n(Auto-resposta enviada — aguarda aprovacao no painel)" if fora_horario else ""
    texto = f"""{tag} - Nova mensagem!

Cliente: {nome}
Mensagem: "{mensagem}"
Area: {area.upper()}{aviso}

Painel:
{API_BASE}/painel"""

    for numero in [IGOR, LETICIA]:
        try:
            requests.post(url, json={"phone": numero, "message": texto}, headers=headers, timeout=10)
        except Exception as e:
            print(f"Erro ao notificar {numero}: {e}")

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

    tokens_in  = resposta.usage.input_tokens
    tokens_out = resposta.usage.output_tokens
    custo      = tokens_in * PRECO_INPUT + tokens_out * PRECO_OUTPUT

    texto_resposta = resposta.content[0].text.strip()
    match = re.search(r'\{.*\}', texto_resposta, re.DOTALL)
    analise = json.loads(match.group()) if match else \
              {"categoria": "irrelevante", "urgencia": "baixa", "area": "geral", "perfil": "simples", "resposta": ""}
    return analise, tokens_in, tokens_out, custo

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
    try:
        if not dentro_do_limite():
            print("Limite diário Claude atingido — mensagem enfileirada sem análise.")
            analise = {"categoria": "cliente_nossa_area", "urgencia": "media",
                       "area": "geral", "perfil": "simples", "resposta": ""}
            tokens_in, tokens_out, custo = 0, 0, 0.0
        else:
            historico = buscar_historico_conversa(telefone)
            analise, tokens_in, tokens_out, custo = analisar_mensagem(texto, historico=historico)

        categoria = analise.get("categoria", "irrelevante")
        if categoria in ["conversa_pessoal", "irrelevante"]:
            return
        msg = {
            "telefone": telefone, "nome": nome, "foto": foto or "",
            "mensagem_original": texto, "analise": analise,
            "fora_horario": True, "status": "pendente"
        }
        db_id, retorno = salvar_mensagem_db(msg, fora_horario=True)
        if db_id:
            if custo > 0:
                registrar_uso_claude(tokens_in, tokens_out, custo, msg_id=db_id)
            brasilia  = datetime.timezone(datetime.timedelta(hours=-3))
            agora_str = datetime.datetime.now(brasilia).strftime('%Y-%m-%d %H:%M:%S')
            chave = str(db_id)
            msg["id"]              = chave
            msg["retorno_cliente"] = retorno
            msg["funil_status"]    = "novo"
            msg["criado_em"]       = agora_str
            mensagens_pendentes[chave] = msg
        notificar_equipe(nome, texto, analise["urgencia"], analise["area"], categoria, fora_horario=True)
    except Exception as e:
        print(f"Erro ao processar fora do horário: {e}")

# ─── TRIAGEM AUTOMÁTICA ────────────────────────────────────────────────────────

def limpar_triagens_expiradas():
    """Remove estados de triagem com mais de 2 horas (evita memória infinita)."""
    brasilia  = datetime.timezone(datetime.timedelta(hours=-3))
    agora     = datetime.datetime.now(brasilia)
    expirados = [tel for tel, d in list(triagem_pendente.items())
                 if (agora - d["criado_em"]).total_seconds() > 7200]
    for tel in expirados:
        del triagem_pendente[tel]
        print(f"Triagem expirada removida: {tel}")

def is_primeiro_contato(telefone):
    """True se o telefone nunca enviou mensagem ao escritório antes."""
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM mensagens WHERE telefone=%s', (telefone,))
        count = c.fetchone()[0]
        c.close()
        conn.close()
        return count == 0
    except Exception as e:
        print(f"Erro is_primeiro_contato: {e}")
        return False

# ─── ROTAS ────────────────────────────────────────────────────────────────────

@app.post("/login")
async def login(request: Request):
    data    = await request.json()
    senha   = data.get("senha", "")
    usuario = data.get("usuario", "")
    senha_correta = os.environ.get("PAINEL_SENHA", "Afra1988")
    if senha == senha_correta:
        return {"ok": True, "usuario": usuario or "Equipe"}
    raise HTTPException(status_code=401, detail="Senha incorreta")

@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    try:
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

        if not texto:                        return {"status": "sem texto"}
        if telefone in [IGOR, LETICIA]:     return {"status": "ignorado - equipe"}

        if not dentro_do_horario():
            enviar_whatsapp(telefone, get_msg_fora_horario())
            background_tasks.add_task(processar_mensagem_fora_horario, telefone, texto, nome, foto)
            return {"status": "fora do horario - auto-resposta enviada"}

        # ── TRIAGEM AUTOMÁTICA (somente dentro do horário) ─────────────────────
        triagem_ativa = get_config("TRIAGEM_ATIVA", "1") == "1"
        if triagem_ativa:
            limpar_triagens_expiradas()

            if telefone in triagem_pendente:
                estado = triagem_pendente[telefone]

                if estado["stage"] == "nome":
                    nome_informado = texto.strip()
                    if len(nome_informado) < 2 or len(nome_informado) > 80:
                        enviar_whatsapp(telefone,
                            "Não consegui identificar. Pode digitar apenas seu nome, por favor?")
                        return {"status": "triagem - aguardando nome novamente"}
                    triagem_pendente[telefone]["nome"]  = nome_informado
                    triagem_pendente[telefone]["stage"] = "situacao"
                    msg_sit = get_config("TRIAGEM_MSG_SITUACAO",
                        "Obrigado! Pode descrever brevemente sua situação ou dúvida jurídica?")
                    enviar_whatsapp(telefone, msg_sit)
                    return {"status": "triagem - aguardando situacao"}

                elif estado["stage"] == "situacao":
                    nome_informado = estado.get("nome", nome)
                    foto_salva     = estado.get("foto",  foto)
                    primeiro_texto = estado.get("primeiro_texto", "")
                    del triagem_pendente[telefone]
                    # Atualiza as variáveis locais para o processamento normal abaixo
                    nome  = nome_informado
                    foto  = foto_salva
                    texto = f"{primeiro_texto}\n{texto}".strip() if primeiro_texto else texto
                    # → continua para processamento normal sem return

            elif is_primeiro_contato(telefone):
                brasilia = datetime.timezone(datetime.timedelta(hours=-3))
                agora    = datetime.datetime.now(brasilia)
                triagem_pendente[telefone] = {
                    "stage":          "nome",
                    "foto":           foto,
                    "criado_em":      agora,
                    "primeiro_texto": texto,
                }
                msg_nome = get_config("TRIAGEM_MSG_NOME",
                    "Olá! Bem-vindo ao escritório Letícia Marques Advocacia. "
                    "Para que possamos atendê-lo melhor, poderia nos informar seu nome?")
                enviar_whatsapp(telefone, msg_nome)
                return {"status": "triagem - aguardando nome"}
        # ── FIM TRIAGEM ─────────────────────────────────────────────────────────

        if not dentro_do_limite():
            print("Limite diário Claude atingido — mensagem enfileirada sem análise.")
            analise = {"categoria": "cliente_nossa_area", "urgencia": "media",
                       "area": "geral", "perfil": "simples", "resposta": ""}
            tokens_in, tokens_out, custo = 0, 0, 0.0
        else:
            historico = buscar_historico_conversa(telefone)
            analise, tokens_in, tokens_out, custo = analisar_mensagem(texto, historico=historico)

        categoria = analise.get("categoria", "irrelevante")

        if categoria in ["conversa_pessoal", "irrelevante"]:
            return {"status": f"ignorado - {categoria}"}

        msg = {
            "telefone": telefone, "nome": nome, "foto": foto,
            "mensagem_original": texto, "analise": analise,
            "fora_horario": False, "status": "pendente"
        }
        db_id, retorno = salvar_mensagem_db(msg)
        if db_id:
            if custo > 0:
                registrar_uso_claude(tokens_in, tokens_out, custo, msg_id=db_id)
            brasilia  = datetime.timezone(datetime.timedelta(hours=-3))
            agora_str = datetime.datetime.now(brasilia).strftime('%Y-%m-%d %H:%M:%S')
            chave = str(db_id)
            msg["id"]              = chave
            msg["retorno_cliente"] = retorno
            msg["funil_status"]    = "novo"
            msg["criado_em"]       = agora_str
            mensagens_pendentes[chave] = msg

        try:
            notificar_equipe(nome, texto, analise.get("urgencia","media"),
                             analise.get("area","geral"), categoria)
        except Exception as e:
            print(f"Erro notificar equipe (não crítico): {e}")

        return {"status": "recebido"}

    except Exception as e:
        import traceback
        print(f"ERRO WEBHOOK: {e}\n{traceback.format_exc()}")
        return {"status": "erro", "detalhe": str(e)}

@app.get("/pendentes")
async def listar_pendentes():
    return [m for m in mensagens_pendentes.values() if m["status"] == "pendente"]

@app.post("/aprovar/{msg_id}")
async def aprovar(msg_id: str, request: Request):
    data = await request.json()
    if msg_id not in mensagens_pendentes:
        return {"erro": "Mensagem não encontrada — pode já ter sido aprovada por outro usuário."}
    telefone       = mensagens_pendentes[msg_id]["telefone"]
    mensagem_final = data.get("mensagem", "")
    usuario        = data.get("usuario", "")
    enviar_whatsapp(telefone, mensagem_final)
    del mensagens_pendentes[msg_id]
    atualizar_status_db(int(msg_id), "enviado", mensagem_final, usuario)
    return {"status": "enviado"}

@app.post("/rejeitar/{msg_id}")
async def rejeitar(msg_id: str, request: Request):
    data = await request.json()
    if msg_id not in mensagens_pendentes:
        return {"erro": "mensagem não encontrada"}
    mensagem_original = mensagens_pendentes[msg_id]["mensagem_original"]
    nova_analise, tokens_in, tokens_out, custo = analisar_mensagem(
        mensagem_original, feedback=data.get("feedback", ""))
    if custo > 0:
        registrar_uso_claude(tokens_in, tokens_out, custo, msg_id=int(msg_id))
    mensagens_pendentes[msg_id]["analise"] = nova_analise
    return {"status": "novas_opcoes", "analise": nova_analise}

@app.post("/contratar/{msg_id}")
async def contratar(msg_id: str, request: Request):
    if msg_id not in mensagens_pendentes:
        return {"erro": "mensagem não encontrada"}
    data    = await request.json()
    usuario = data.get("usuario", "")
    atualizar_status_db(int(msg_id), "contrato", aprovado_por=usuario)
    del mensagens_pendentes[msg_id]
    return {"status": "contrato registrado"}

@app.post("/funil/{msg_id}")
async def atualizar_funil(msg_id: str, request: Request):
    data  = await request.json()
    funil = data.get("funil_status", "novo")
    if msg_id in mensagens_pendentes:
        mensagens_pendentes[msg_id]["funil_status"] = funil
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('UPDATE mensagens SET funil_status=%s WHERE id=%s', (funil, int(msg_id)))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        print(f"Erro ao atualizar funil: {e}")
    return {"ok": True}

@app.get("/historico/{telefone}")
async def historico_cliente(telefone: str):
    return buscar_historico_completo(telefone)

@app.get("/nota/{telefone}")
async def get_nota(telefone: str):
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('SELECT texto FROM notas WHERE telefone=%s', (telefone,))
        row = c.fetchone()
        c.close()
        conn.close()
        return {"texto": row[0] if row else ""}
    except:
        return {"texto": ""}

@app.post("/nota/{telefone}")
async def salvar_nota(telefone: str, request: Request):
    data  = await request.json()
    texto = data.get("texto", "")
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('''INSERT INTO notas (telefone, texto) VALUES (%s,%s)
                     ON CONFLICT (telefone) DO UPDATE SET texto=EXCLUDED.texto, atualizado_em=NOW()''',
                  (telefone, texto))
        conn.commit()
        c.close()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"erro": str(e)}

# ─── MODELOS DE RESPOSTA ───────────────────────────────────────────────────────

@app.get("/modelos")
async def listar_modelos():
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('SELECT id, titulo, texto FROM modelos ORDER BY id ASC')
        rows = c.fetchall()
        c.close()
        conn.close()
        return [{"id": r[0], "titulo": r[1], "texto": r[2]} for r in rows]
    except:
        return []

@app.post("/modelos")
async def criar_modelo(request: Request):
    data = await request.json()
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('INSERT INTO modelos (titulo, texto) VALUES (%s,%s) RETURNING id',
                  (data.get("titulo",""), data.get("texto","")))
        new_id = c.fetchone()[0]
        conn.commit()
        c.close()
        conn.close()
        return {"ok": True, "id": new_id}
    except Exception as e:
        return {"erro": str(e)}

@app.delete("/modelos/{modelo_id}")
async def deletar_modelo(modelo_id: int):
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('DELETE FROM modelos WHERE id=%s', (modelo_id,))
        conn.commit()
        c.close()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"erro": str(e)}

# ─── LEMBRETES ────────────────────────────────────────────────────────────────

@app.get("/lembretes")
async def listar_lembretes():
    try:
        conn = get_conn()
        c = conn.cursor()
        brasilia = datetime.timezone(datetime.timedelta(hours=-3))
        hoje = datetime.datetime.now(brasilia).date()
        c.execute('''SELECT id, msg_id, telefone, nome, texto, data_lembrete::text
                     FROM lembretes WHERE ativo=TRUE AND data_lembrete <= %s
                     ORDER BY data_lembrete ASC''', (hoje,))
        rows = c.fetchall()
        c.close()
        conn.close()
        return [{"id": r[0], "msg_id": r[1], "telefone": r[2], "nome": r[3],
                 "texto": r[4], "data": r[5]} for r in rows]
    except:
        return []

@app.post("/lembrete")
async def criar_lembrete(request: Request):
    data = await request.json()
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('''INSERT INTO lembretes (msg_id, telefone, nome, texto, data_lembrete)
                     VALUES (%s,%s,%s,%s,%s) RETURNING id''',
                  (data.get("msg_id"), data.get("telefone"), data.get("nome"),
                   data.get("texto",""), data.get("data_lembrete")))
        new_id = c.fetchone()[0]
        conn.commit()
        c.close()
        conn.close()
        return {"ok": True, "id": new_id}
    except Exception as e:
        return {"erro": str(e)}

@app.delete("/lembrete/{lembrete_id}")
async def deletar_lembrete(lembrete_id: int):
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('UPDATE lembretes SET ativo=FALSE WHERE id=%s', (lembrete_id,))
        conn.commit()
        c.close()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"erro": str(e)}

# ─── PRAZOS JUDICIAIS ─────────────────────────────────────────────────────────

@app.get("/prazos-lista")
async def listar_prazos():
    try:
        brasilia = datetime.timezone(datetime.timedelta(hours=-3))
        hoje     = datetime.datetime.now(brasilia).date()
        conn = get_conn()
        c = conn.cursor()
        c.execute('''
            SELECT id, processo, cliente, tipo, descricao,
                   data_prazo::text, responsavel,
                   TO_CHAR(criado_em - INTERVAL '3 hours','DD/MM/YYYY')
            FROM prazos
            WHERE ativo = TRUE
            ORDER BY data_prazo ASC
        ''')
        rows = c.fetchall()
        c.close()
        conn.close()
        result = []
        for r in rows:
            data_prazo = datetime.date.fromisoformat(r[5])
            dias = (data_prazo - hoje).days
            result.append({
                "id": r[0], "processo": r[1] or "", "cliente": r[2],
                "tipo": r[3] or "", "descricao": r[4] or "",
                "data_prazo": r[5], "responsavel": r[6] or "Equipe",
                "criado_em": r[7], "dias_restantes": dias
            })
        return result
    except Exception as e:
        print(f"Erro ao listar prazos: {e}")
        return []

@app.post("/prazos")
async def criar_prazo(request: Request):
    data = await request.json()
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('''
            INSERT INTO prazos (processo, cliente, tipo, descricao, data_prazo, responsavel)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING id
        ''', (data.get("processo",""), data.get("cliente",""), data.get("tipo",""),
              data.get("descricao",""), data.get("data_prazo"), data.get("responsavel","Equipe")))
        new_id = c.fetchone()[0]
        conn.commit()
        c.close()
        conn.close()
        return {"ok": True, "id": new_id}
    except Exception as e:
        return {"erro": str(e)}

@app.delete("/prazos/{prazo_id}")
async def deletar_prazo(prazo_id: int):
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('UPDATE prazos SET ativo=FALSE WHERE id=%s', (prazo_id,))
        conn.commit()
        c.close()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"erro": str(e)}

@app.post("/prazos/testar-alertas")
async def testar_alertas_prazos():
    """Dispara verificação manual de prazos (para teste)."""
    verificar_prazos()
    return {"ok": True, "msg": "Verificação executada"}

# ─── EXPORTAR CSV ─────────────────────────────────────────────────────────────

@app.get("/exportar-csv")
async def exportar_csv():
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute('''SELECT telefone, nome, mensagem_original, area, urgencia, status,
                            funil_status,
                            TO_CHAR(criado_em - INTERVAL '3 hours','DD/MM/YYYY HH24:MI'),
                            aprovado_por
                     FROM mensagens ORDER BY id DESC''')
        rows = c.fetchall()
        c.close()
        conn.close()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Telefone","Nome","Mensagem","Área","Urgência","Status","Funil","Data","Aprovado Por"])
        for row in rows:
            writer.writerow(row)

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": "attachment; filename=clientes.csv"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─── RELATÓRIO SEMANAL ────────────────────────────────────────────────────────

def _gerar_e_enviar_relatorio():
    """Gera e envia o relatório semanal via WhatsApp. Retorna dict com resultado."""
    try:
        conn = get_conn()
        c = conn.cursor()
        brasilia     = datetime.timezone(datetime.timedelta(hours=-3))
        agora        = datetime.datetime.now(brasilia)
        semana_atras = agora - datetime.timedelta(days=7)

        c.execute('SELECT COUNT(*) FROM mensagens WHERE criado_em >= %s', (semana_atras,))
        total = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM mensagens WHERE criado_em >= %s AND status='enviado'", (semana_atras,))
        enviados = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM mensagens WHERE criado_em >= %s AND status='contrato'", (semana_atras,))
        contratos = c.fetchone()[0]
        c.execute('''SELECT area, COUNT(*) FROM mensagens WHERE criado_em >= %s
                     GROUP BY area ORDER BY 2 DESC LIMIT 3''', (semana_atras,))
        por_area = c.fetchall()
        c.close()
        conn.close()

        areas_texto = "\n".join([f"  - {r[0].upper()}: {r[1]}" for r in por_area]) if por_area else "  Nenhum"
        data_inicio = semana_atras.strftime("%d/%m")
        data_fim    = agora.strftime("%d/%m/%Y")

        texto = f"""RELATORIO SEMANAL — {data_inicio} a {data_fim}

Contatos recebidos: {total}
Respostas enviadas: {enviados}
Contratos fechados: {contratos}

Por area:
{areas_texto}

Painel completo:
{API_BASE}/relatorios"""

        instance     = os.environ["ZAPI_INSTANCE"]
        token        = os.environ["ZAPI_TOKEN"]
        client_token = os.environ["ZAPI_CLIENT_TOKEN"]
        url     = f"https://api.z-api.io/instances/{instance}/token/{token}/send-text"
        headers = {"Client-Token": client_token}
        for numero in [IGOR, LETICIA]:
            try:
                requests.post(url, json={"phone": numero, "message": texto}, headers=headers, timeout=10)
            except:
                pass

        # Registra data do envio para não duplicar
        set_config("ULTIMO_RELATORIO_SEMANAL", agora.strftime("%Y-%m-%d"))
        print(f"Relatório semanal enviado: {data_inicio} a {data_fim}")
        return {"ok": True, "periodo": f"{data_inicio} a {data_fim}", "total": total}
    except Exception as e:
        print(f"Erro ao enviar relatório semanal: {e}")
        return {"erro": str(e)}

@app.post("/relatorio-semanal")
async def enviar_relatorio_semanal():
    return _gerar_e_enviar_relatorio()

# ─── FOLLOW-UP AUTOMÁTICO ─────────────────────────────────────────────────────

def verificar_follow_ups():
    """Envia follow-up para clientes que não responderam após X horas."""
    try:
        horas = int(get_config("FOLLOWUP_HORAS", "48"))
        ativo = get_config("FOLLOWUP_ATIVO", "1")
        if ativo != "1" or horas <= 0:
            return

        msg_followup = get_config("FOLLOWUP_MENSAGEM",
            "Olá! Passando para saber se ficou alguma dúvida sobre o que conversamos. "
            "Estamos à disposição para ajudar quando precisar.")

        conn = get_conn()
        c = conn.cursor()

        # Busca mensagens enviadas há mais de X horas, sem follow-up ainda,
        # e cujo cliente não enviou nova mensagem depois da resposta
        c.execute(f'''
            SELECT DISTINCT ON (m.telefone)
                   m.id, m.telefone, m.nome
            FROM mensagens m
            WHERE m.status = 'enviado'
              AND m.follow_up_enviado = FALSE
              AND m.respondido_em IS NOT NULL
              AND m.respondido_em < NOW() - INTERVAL '{horas} hours'
              AND NOT EXISTS (
                  SELECT 1 FROM mensagens m2
                  WHERE m2.telefone = m.telefone
                    AND m2.criado_em > m.respondido_em
              )
            ORDER BY m.telefone, m.respondido_em DESC
        ''')
        pendentes = c.fetchall()

        for msg_id, telefone, nome in pendentes:
            try:
                enviar_whatsapp(telefone, msg_followup)
                c.execute('UPDATE mensagens SET follow_up_enviado=TRUE WHERE id=%s', (msg_id,))
                print(f"Follow-up enviado para {nome} ({telefone})")
            except Exception as e:
                print(f"Erro ao enviar follow-up para {telefone}: {e}")

        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        print(f"Erro no verificar_follow_ups: {e}")

# ─── ALERTAS DE PRAZO JUDICIAL ────────────────────────────────────────────────

def verificar_prazos():
    """Envia alertas de prazo judicial por WhatsApp (7d, 3d, 1d e no dia)."""
    try:
        brasilia = datetime.timezone(datetime.timedelta(hours=-3))
        hoje     = datetime.datetime.now(brasilia).date()

        conn = get_conn()
        c = conn.cursor()
        c.execute('''
            SELECT id, processo, cliente, tipo, descricao, data_prazo, responsavel,
                   alerta_7d_enviado, alerta_3d_enviado, alerta_1d_enviado, alerta_dia_enviado
            FROM prazos
            WHERE ativo = TRUE AND data_prazo >= %s
            ORDER BY data_prazo ASC
        ''', (hoje,))
        prazos = c.fetchall()

        instance     = os.environ.get("ZAPI_INSTANCE", "")
        token        = os.environ.get("ZAPI_TOKEN", "")
        client_token = os.environ.get("ZAPI_CLIENT_TOKEN", "")
        url     = f"https://api.z-api.io/instances/{instance}/token/{token}/send-text"
        headers = {"Client-Token": client_token}

        for row in prazos:
            (pid, processo, cliente, tipo, descricao,
             data_prazo, responsavel, a7, a3, a1, adia) = row
            dias = (data_prazo - hoje).days

            alerta = None
            campo  = None

            if dias == 0 and not adia:
                alerta = f"PRAZO HOJE - {tipo or 'Judicial'}\n\nProcesso: {processo or 'N/I'}\nCliente: {cliente}\nDescrição: {descricao or ''}\nResponsável: {responsavel}"
                campo  = "alerta_dia_enviado"
            elif dias == 1 and not a1:
                alerta = f"PRAZO AMANHA - {tipo or 'Judicial'}\n\nProcesso: {processo or 'N/I'}\nCliente: {cliente}\nDescrição: {descricao or ''}\nResponsável: {responsavel}"
                campo  = "alerta_1d_enviado"
            elif dias == 3 and not a3:
                alerta = f"Prazo em 3 dias - {tipo or 'Judicial'}\n\nProcesso: {processo or 'N/I'}\nCliente: {cliente}\nDescrição: {descricao or ''}\nResponsável: {responsavel}"
                campo  = "alerta_3d_enviado"
            elif dias == 7 and not a7:
                alerta = f"Prazo em 7 dias - {tipo or 'Judicial'}\n\nProcesso: {processo or 'N/I'}\nCliente: {cliente}\nDescrição: {descricao or ''}\nResponsável: {responsavel}"
                campo  = "alerta_7d_enviado"

            if alerta and campo:
                for numero in [IGOR, LETICIA]:
                    try:
                        requests.post(url, json={"phone": numero, "message": alerta},
                                      headers=headers, timeout=10)
                    except Exception as e:
                        print(f"Erro ao enviar alerta prazo para {numero}: {e}")
                c.execute(f'UPDATE prazos SET {campo}=TRUE WHERE id=%s', (pid,))
                print(f"Alerta de prazo enviado: {cliente} — {dias} dia(s)")

        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        print(f"Erro em verificar_prazos: {e}")

# ─── AGENDADOR AUTOMÁTICO ─────────────────────────────────────────────────────

async def agendador():
    """Roda em background: relatório semanal + follow-ups automáticos."""
    print("Agendador iniciado.")
    while True:
        try:
            brasilia = datetime.timezone(datetime.timedelta(hours=-3))
            agora    = datetime.datetime.now(brasilia)

            # Relatório semanal — segunda-feira entre 8h e 8h30
            if agora.weekday() == 0 and agora.hour == 8 and agora.minute < 30:
                hoje_str = agora.strftime("%Y-%m-%d")
                if get_config("ULTIMO_RELATORIO_SEMANAL", "") != hoje_str:
                    print("Segunda-feira 8h — enviando relatório semanal automático...")
                    _gerar_e_enviar_relatorio()

            # Follow-ups — verifica sempre (só envia dentro do horário comercial)
            if agora.weekday() < 5 and 8 <= agora.hour < 18:
                verificar_follow_ups()

            # Alertas de prazo judicial — uma vez ao dia, às 8h
            if agora.weekday() < 5 and 8 <= agora.hour < 9:
                hoje_str = agora.strftime("%Y-%m-%d")
                if get_config("ULTIMA_VERIFICACAO_PRAZOS", "") != hoje_str:
                    print("Verificando alertas de prazo judicial...")
                    verificar_prazos()
                    set_config("ULTIMA_VERIFICACAO_PRAZOS", hoje_str)

        except Exception as e:
            print(f"Erro no agendador: {e}")
        await asyncio.sleep(1800)  # verifica a cada 30 minutos

@app.on_event("startup")
async def startup():
    asyncio.create_task(agendador())

# ─── CONFIGURAÇÕES ─────────────────────────────────────────────────────────────

@app.get("/custo")
async def get_custo():
    return buscar_custo()

@app.get("/config")
async def get_configuracoes():
    return {
        "MSG_FORA_HORARIO":    get_msg_fora_horario(),
        "HORA_INICIO":         get_config("HORA_INICIO",        "8"),
        "HORA_FIM":            get_config("HORA_FIM",           "18"),
        "LIMITE_DIARIO_USD":   get_config("LIMITE_DIARIO_USD",  "0"),
        "FOLLOWUP_ATIVO":      get_config("FOLLOWUP_ATIVO",     "0"),
        "FOLLOWUP_HORAS":      get_config("FOLLOWUP_HORAS",     "48"),
        "FOLLOWUP_MENSAGEM":   get_config("FOLLOWUP_MENSAGEM",
            "Olá! Passando para saber se ficou alguma dúvida sobre o que conversamos. "
            "Estamos à disposição para ajudar quando precisar."),
        "TRIAGEM_ATIVA":       get_config("TRIAGEM_ATIVA",      "1"),
        "TRIAGEM_MSG_NOME":    get_config("TRIAGEM_MSG_NOME",
            "Olá! Bem-vindo ao escritório Letícia Marques Advocacia. "
            "Para que possamos atendê-lo melhor, poderia nos informar seu nome?"),
        "TRIAGEM_MSG_SITUACAO": get_config("TRIAGEM_MSG_SITUACAO",
            "Obrigado! Pode descrever brevemente sua situação ou dúvida jurídica?"),
    }

@app.post("/config")
async def salvar_configuracoes(request: Request):
    data = await request.json()
    for chave in ["MSG_FORA_HORARIO", "HORA_INICIO", "HORA_FIM",
                  "LIMITE_DIARIO_USD", "FOLLOWUP_ATIVO", "FOLLOWUP_HORAS", "FOLLOWUP_MENSAGEM",
                  "TRIAGEM_ATIVA", "TRIAGEM_MSG_NOME", "TRIAGEM_MSG_SITUACAO"]:
        if chave in data:
            set_config(chave, str(data[chave]))
    return {"ok": True}

@app.get("/relatorios-dados")
async def relatorios_dados():
    return buscar_relatorios()

# ─── PÁGINAS ───────────────────────────────────────────────────────────────────

@app.get("/painel")
async def painel():
    return FileResponse("painel.html")

@app.get("/relatorios")
async def relatorios_page():
    return FileResponse("relatorios.html")

@app.get("/prazos")
async def prazos_page():
    return FileResponse("prazos.html")

@app.get("/configuracoes")
async def configuracoes_page():
    return FileResponse("configuracoes.html")

@app.get("/db-status")
async def db_status():
    try:
        db_url = os.environ.get("DATABASE_URL", "NAO_DEFINIDA")
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM mensagens")
        count = c.fetchone()[0]
        c.close()
        conn.close()
        return {"status": "conectado", "mensagens": count, "url_prefixo": db_url[:30]}
    except Exception as e:
        return {"status": "erro", "erro": str(e),
                "DATABASE_URL": os.environ.get("DATABASE_URL", "NAO_DEFINIDA")[:30]}

@app.get("/diagnostico")
async def diagnostico():
    resultado = {}
    try:
        resultado["db"] = "ok"
        conn = get_conn(); conn.close()
    except Exception as e:
        resultado["db"] = str(e)
    try:
        resultado["anthropic_key"] = "ok" if os.environ.get("ANTHROPIC_API_KEY") else "FALTANDO"
    except:
        resultado["anthropic_key"] = "erro"
    try:
        resultado["zapi_instance"] = "ok" if os.environ.get("ZAPI_INSTANCE") else "FALTANDO"
        resultado["zapi_token"]    = "ok" if os.environ.get("ZAPI_TOKEN")    else "FALTANDO"
        resultado["zapi_client"]   = "ok" if os.environ.get("ZAPI_CLIENT_TOKEN") else "FALTANDO"
    except:
        resultado["zapi"] = "erro"
    try:
        resultado["horario"] = dentro_do_horario()
        resultado["limite"]  = dentro_do_limite()
    except Exception as e:
        resultado["horario_erro"] = str(e)
    try:
        client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        r = client.messages.create(
            model="claude-sonnet-4-6", max_tokens=10,
            messages=[{"role":"user","content":"Responda apenas: ok"}])
        resultado["claude"] = "ok - " + r.content[0].text.strip()
        resultado["tokens"] = {"in": r.usage.input_tokens, "out": r.usage.output_tokens}
    except Exception as e:
        resultado["claude"] = "ERRO: " + str(e)
    return resultado

@app.get("/")
async def root():
    return {"status": "servidor rodando"}
