#!/usr/bin/env python3
"""
Organizador de Documentos Juridicos - AB Group
App web mobile-first para organizar documentos em ordem cronologica via IA.
Separa automaticamente PDFs com multiplos documentos em arquivos individuais.
"""

import os
import io
import re
import json
import uuid
import time
import base64
import shutil
import zipfile
import tempfile
import unicodedata
from pathlib import Path
from datetime import datetime

from flask import Flask, render_template, request, jsonify, send_file

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024  # 100MB max total

# Diretorio compartilhado entre workers para ZIPs temporarios
SHARED_ZIP_DIR = Path(tempfile.gettempdir()) / "organizador-zips"
SHARED_ZIP_DIR.mkdir(exist_ok=True)

MODELO_IA = "claude-haiku-4-5-20251001"
EXTENSOES_ACEITAS = {".pdf", ".jpg", ".jpeg", ".png", ".docx"}
MAX_TEXTO_POR_PAGINA = 2000  # contexto maior para IA classificar melhor
MAX_PAGINAS_PDF = 100  # suporta PDFs grandes (antes era 30 = perdia paginas)

# Limite de tamanho por tipo de processo (em bytes)
LIMITES_TAMANHO = {
    "inss_admin": 5 * 1024 * 1024,   # 5MB
    "judicial": 10 * 1024 * 1024,    # 10MB
    "consumidor": 10 * 1024 * 1024,  # 10MB
    "trabalhista": 10 * 1024 * 1024, # 10MB
    "civel": 10 * 1024 * 1024,       # 10MB
}
MAX_FILE_SIZE_BYTES = 25 * 1024 * 1024  # fallback geral

TIPOS_PROCESSO = {
    "inss_admin": "INSS Administrativo",
    "judicial": "Judicial",
    "consumidor": "Direito do Consumidor",
    "trabalhista": "Trabalhista",
    "civel": "Cível",
}

USUARIOS = [
    "Emily", "Karol", "Alan", "Henrique", "Caique", "Jaíne",
    "Camila", "Luana", "Claudio", "Meire", "André", "Vitória",
]

# Webhook para registrar uso (configurado via env var)
LOG_WEBHOOK_URL = os.environ.get("LOG_WEBHOOK_URL", "")
# Webhook para registrar erros (pode ser o mesmo, diferenciado pelo campo 'tipo')
ERRO_WEBHOOK_URL = os.environ.get("ERRO_WEBHOOK_URL", LOG_WEBHOOK_URL)

PROMPT_MAPEAMENTO = """Analise o texto de cada pagina deste PDF juridico brasileiro.
Identifique TODOS os documentos separados que existem dentro deste arquivo.

IMPORTANTE: Responda SEMPRE em portugues brasileiro. NUNCA use ingles.
Use EXATAMENTE um destes tipos (escreva identico, sem traduzir):
- Procuracao
- Substabelecimento
- Declaracao de Hipossuficiencia
- Declaracao (outras declaracoes sem ser hipossuficiencia)
- Contrato de Honorarios
- Termo de Responsabilidade
- Termo de Representacao
- Protocolo de Assinatura (gerado por DocuSign, Clicksign, ZapSign, D4Sign - vem APOS documento assinado)
- RG
- CPF
- CNH
- Documento de Identidade
- CNIS
- CTPS
- Carta Indeferimento INSS
- Decisao INSS
- Atestado Medico
- Relatorio Medico
- Receita Medica
- Exame Medico
- Laudo Medico
- Laudo MeuINSS
- Certidao de Casamento
- Certidao de Nascimento
- Certidao de Obito
- Termo de Homologacao Atividade Rural
- Documentos Rurais
- Folha V7
- Declaracao Tempo de Servico
- Ficha Funcionario
- Certidao Tempo de Servico
- Ficha Financeira
- PPP
- LTCAT
- GPS
- Comprovante de Residencia
- Comprovante de Gasto
- Foto de Residencia
- Avaliacao Social
- Pericia Medica
- Contagem de Tempo
- Calculo Renda Mensal
- Copia Processo Administrativo
- Certidao Negativa Justica Estadual

REGRAS:
- Cada documento pode ter 1 ou mais paginas
- Uma procuracao de 3 paginas NAO e 3 procuracoes - e UMA procuracao (pagina_inicio=1, pagina_fim=3)
- "Protocolo de Assinatura" e um documento separado que vem APOS o documento assinado
- Identifique onde cada documento COMECA e TERMINA
- A data deve ser a data de emissao/expedicao do documento (no texto), NAO a data de hoje

Responda APENAS em JSON valido, sem markdown:
{"documentos": [
  {"tipo": "Procuracao", "pagina_inicio": 1, "pagina_fim": 3, "data": "2023-03-15"},
  {"tipo": "Protocolo de Assinatura", "pagina_inicio": 4, "pagina_fim": 4, "data": null},
  {"tipo": "RG", "pagina_inicio": 5, "pagina_fim": 5, "data": null}
]}

Se nao encontrar data, use "data": null.
"""

PROMPT_EXTRACAO_SIMPLES = """Analise este documento juridico brasileiro.

IMPORTANTE: Responda SEMPRE em portugues brasileiro, nunca em ingles.

Extraia:
1. Tipo do documento (use EXATAMENTE: Procuracao, Substabelecimento, Declaracao de Hipossuficiencia, Declaracao, Contrato de Honorarios, Termo de Responsabilidade, Termo de Representacao, Protocolo de Assinatura, RG, CPF, CNH, CNIS, CTPS, Carta Indeferimento INSS, Atestado Medico, Relatorio Medico, Receita Medica, Exame Medico, Laudo Medico, Laudo MeuINSS, Certidao de Casamento, Certidao de Nascimento, Certidao de Obito, Documentos Rurais, Folha V7, Declaracao Tempo Servico, Ficha Funcionario, Certidao Tempo Servico, Ficha Financeira, PPP, LTCAT, GPS, Comprovante Residencia, Comprovante Gasto, Foto Residencia)
2. Data de emissao/expedicao DO DOCUMENTO (lida no texto, NAO a data atual)

Responda APENAS em JSON valido, sem markdown:
{"tipo": "...", "data": "YYYY-MM-DD"}

Se nao encontrar data, use "data": null.
"""

PROMPT_MAPEAMENTO_CURTO = """Identifique os documentos nestas paginas de PDF juridico brasileiro.

IMPORTANTE: Responda SEMPRE em portugues brasileiro. Uma procuracao de 3 paginas e UMA procuracao (nao 3).
Tipos permitidos: Procuracao, Substabelecimento, Declaracao de Hipossuficiencia, Declaracao, Contrato de Honorarios, Termo de Responsabilidade, Termo de Representacao, Protocolo de Assinatura, RG, CPF, CNH, CNIS, CTPS, Atestado Medico, Relatorio Medico, Receita Medica, Exame Medico, Laudo, Certidao, PPP, LTCAT, GPS, Comprovante Residencia, Comprovante Gasto.

Para cada documento, indique: tipo, pagina de inicio, pagina de fim, data de emissao (do texto, nao hoje).

Responda APENAS em JSON valido:
{"documentos": [{"tipo": "...", "pagina_inicio": 1, "pagina_fim": 3, "data": "YYYY-MM-DD"}]}
Se nao encontrar data, use "data": null.
"""

CHUNK_SIZE_PAGINAS = 8  # paginas por chunk para PDFs grandes

# Sequencia para INSS Administrativo
# IMPORTANTE:
# - grupo_procuracao_termos = procuracoes + substabelecimentos + termos + declaracoes + protocolos juntos
# - contrato_de_honorarios FICA SEMPRE SEPARADO (documento sigiloso entre advogado e cliente,
#   nao compoe os documentos do processo)
SEQUENCIA_INSS_ADMIN = [
    ("grupo_procuracao_termos", "Procuracao_Termos_Declaracoes"),
    ("contrato_de_honorarios", "Contrato_de_Honorarios_SIGILOSO"),
    ("documento_pessoal", None),  # usa tipo real
]

# Sequencia detalhada para Processos Judiciais
SEQUENCIA_JUDICIAL = [
    ("procuracao", "Procuracao"),
    ("substabelecimento", "Substabelecimento"),
    ("declaracao_hipossuficiencia", "Declaracao_de_Hipossuficiencia"),
    ("documento_pessoal", None),
    ("carta_indeferimento", "Carta_Indeferimento_INSS"),
    ("cnis", "CNIS"),
    ("ctps", "CTPS"),
    ("atestados_relatorios_receitas", "Atestados_Relatorios_Receitas_Medicas"),
    ("exames_medicos", "Exames_Medicos"),
    ("laudo_meuinss", "Laudo_MeuINSS"),
    ("documentos_rurais", "Documentos_Rurais"),
    ("folha_v7", "Folha_V7"),
    ("certidoes", "Certidoes"),
    ("termo_homologacao_rural", "Termo_Homologacao_Atividade_Rural"),
    ("declaracao_tempo_servico", "Declaracao_Tempo_Servico_Ficha_Funcionario"),
    ("certidao_tempo_servico", "Certidao_Tempo_Servico"),
    ("ficha_financeira", "Ficha_Financeira"),
    ("ppp", "PPP"),
    ("ltcat", "LTCAT"),
    ("gps", "GPS"),
    ("comprovante_gasto", "Comprovantes_Gastos"),
    ("foto_residencia", "Fotos_Residencia"),
    ("avaliacao_social_pericia", "Avaliacao_Social_Pericia_Medica"),
    ("contagem_tempo", "Contagem_Tempo"),
    ("calculo_regras_transicao", "Calculo_Regras_Transicao"),
    ("calculo_rmi", "Calculo_RMI"),
    ("copia_processo_administrativo", "Copia_Processo_Administrativo"),
    ("certidao_negativa_estadual", "Certidao_Negativa_Justica_Estadual"),
    ("comprovante_residencia", "Comprovante_Residencia"),
]

# Categorias que devem ser MERGED em um unico PDF (em ordem cronologica)
CATEGORIAS_MERGE = {
    "atestados_relatorios_receitas",
    "exames_medicos",
    "documentos_rurais",
    "comprovante_gasto",
    "ctps",
    "certidoes",
    "declaracao_tempo_servico",
    "ficha_financeira",
    "gps",
    "grupo_procuracao_termos",  # admin: procuracao+substabelecimento+termo juntos
}

# Categorias que devem receber o "protocolo de assinatura" anexado
CATEGORIAS_ASSINADAS = {
    "procuracao",
    "substabelecimento",
    "declaracao",
    "declaracao_hipossuficiencia",
    "contrato_de_honorarios",
    "termo_de_responsabilidade",
}

# Para INSS admin: agrupar estas categorias em UM UNICO arquivo
# (procuracoes + substabelecimentos + termos + declaracoes + protocolos de assinatura)
# NOTA: contrato_de_honorarios NAO entra aqui (fica sempre separado, e sigiloso)
GRUPO_MERGE_ADMIN = {
    "grupo_procuracao_termos": [
        "procuracao",
        "substabelecimento",
        "termo_de_responsabilidade",
        "declaracao",
        "declaracao_hipossuficiencia",
        "protocolo_assinatura",
    ],
}


def limpar_nome(texto):
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = texto.lower().replace(" ", "_")
    texto = "".join(c for c in texto if c.isalnum() or c in ("_", "-"))
    return texto


def contar_paginas_pdf(caminho):
    """Conta total de paginas de um PDF sem extrair texto."""
    try:
        from pypdf import PdfReader
        return len(PdfReader(caminho).pages)
    except Exception:
        return 0


def extrair_textos_todas_paginas(caminho):
    """Extrai texto de todas as paginas de um PDF.
    Retorna dict com paginas + aviso caso tenha truncado."""
    import pdfplumber
    resultado = {"paginas": [], "total_real": 0, "truncado": False}
    try:
        with pdfplumber.open(caminho) as pdf:
            total_real = len(pdf.pages)
            resultado["total_real"] = total_real
            total = min(total_real, MAX_PAGINAS_PDF)
            if total_real > MAX_PAGINAS_PDF:
                resultado["truncado"] = True
            # PDFs grandes: menos texto por pagina para economizar memoria e tokens
            if total > CHUNK_SIZE_PAGINAS:
                max_linhas = 15
                max_chars = 1000
            else:
                max_linhas = 20
                max_chars = MAX_TEXTO_POR_PAGINA
            for i in range(total):
                try:
                    texto = pdf.pages[i].extract_text() or ""
                except Exception:
                    texto = ""
                linhas = texto.split("\n")[:max_linhas]
                resultado["paginas"].append({"pagina": i + 1, "texto": "\n".join(linhas)[:max_chars]})
        return resultado
    except Exception:
        return resultado


def dividir_pdf_por_tamanho(caminho, tmp_dir, max_bytes=MAX_FILE_SIZE_BYTES):
    """Divide um PDF grande em partes menores de no maximo max_bytes cada."""
    from pypdf import PdfReader, PdfWriter

    tamanho = os.path.getsize(caminho)
    if tamanho <= max_bytes:
        return [caminho]  # nao precisa dividir

    try:
        reader = PdfReader(caminho)
        total_paginas = len(reader.pages)
        if total_paginas <= 1:
            return [caminho]  # nao da pra dividir mais

        # Estima paginas por parte baseado no tamanho
        bytes_por_pagina = tamanho / total_paginas
        paginas_por_parte = max(1, int(max_bytes / bytes_por_pagina))

        partes = []
        inicio = 0
        parte_num = 1

        while inicio < total_paginas:
            fim = min(inicio + paginas_por_parte, total_paginas)
            writer = PdfWriter()
            for i in range(inicio, fim):
                writer.add_page(reader.pages[i])

            nome_base = Path(caminho).stem
            parte_path = os.path.join(tmp_dir, f"{nome_base}_parte{parte_num}.pdf")
            with open(parte_path, "wb") as f:
                writer.write(f)

            # Verifica se a parte ainda e grande demais (pode acontecer com paginas pesadas)
            if os.path.getsize(parte_path) > max_bytes and (fim - inicio) > 1:
                # Tenta com menos paginas
                os.remove(parte_path)
                paginas_por_parte = max(1, paginas_por_parte // 2)
                continue

            partes.append(parte_path)
            inicio = fim
            parte_num += 1

        return partes if partes else [caminho]
    except Exception:
        return [caminho]


def pagina_para_imagem_b64(caminho, pagina_num):
    """Converte uma pagina especifica do PDF em imagem base64."""
    from pdf2image import convert_from_path
    try:
        imagens = convert_from_path(caminho, first_page=pagina_num, last_page=pagina_num, dpi=150)
        if imagens:
            buffer = io.BytesIO()
            imagens[0].save(buffer, format="JPEG", quality=80)
            return base64.b64encode(buffer.getvalue()).decode()
        return None
    except Exception:
        return None


def separar_pdf(caminho_original, pagina_inicio, pagina_fim, caminho_destino):
    """Extrai paginas de um PDF e salva em novo arquivo."""
    import pdfplumber
    from pypdf import PdfReader, PdfWriter
    try:
        reader = PdfReader(caminho_original)
        writer = PdfWriter()
        for i in range(pagina_inicio - 1, min(pagina_fim, len(reader.pages))):
            writer.add_page(reader.pages[i])
        with open(caminho_destino, "wb") as f:
            writer.write(f)
        return True
    except Exception:
        return False


def _parse_json_response(resposta):
    """Parse JSON da resposta da IA, lidando com markdown code blocks."""
    resposta = resposta.strip()
    if resposta.startswith("{"):
        return json.loads(resposta)
    if "```" in resposta:
        json_str = resposta.split("```")[1]
        if json_str.startswith("json"):
            json_str = json_str[4:]
        return json.loads(json_str.strip())
    return json.loads(resposta)


def _mapear_chunk(client, caminho, nome_arquivo, chunk_paginas):
    """Mapeia documentos em um chunk de paginas (max CHUNK_SIZE_PAGINAS)."""
    texto_chunk = ""
    for p in chunk_paginas:
        texto_chunk += f"\n--- PAGINA {p['pagina']} ---\n{p['texto']}\n"

    # Para paginas sem texto no chunk, tenta imagem (max 1 por chunk para economizar)
    imagens_content = []
    paginas_sem_texto = [p for p in chunk_paginas if not p["texto"].strip()]
    if paginas_sem_texto and len(paginas_sem_texto) <= 2:
        p = paginas_sem_texto[0]
        img_b64 = pagina_para_imagem_b64(caminho, p["pagina"])
        if img_b64:
            imagens_content.append({"type": "text", "text": f"--- PAGINA {p['pagina']} (imagem) ---"})
            imagens_content.append({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64},
            })
            img_b64 = None

    pag_inicio = chunk_paginas[0]["pagina"]
    pag_fim = chunk_paginas[-1]["pagina"]

    messages_content = [{"type": "text",
        "text": f"Arquivo: {nome_arquivo} (paginas {pag_inicio}-{pag_fim})\n\n{texto_chunk}"}]
    messages_content.extend(imagens_content)

    try:
        response = client.messages.create(
            model=MODELO_IA,
            max_tokens=800,
            messages=[{"role": "user", "content": messages_content + [
                {"type": "text", "text": PROMPT_MAPEAMENTO_CURTO}
            ]}],
        )
        dados = _parse_json_response(response.content[0].text)
        docs = dados.get("documentos", [])
        if docs:
            return docs
    except Exception:
        pass

    # Fallback: chunk inteiro como documento unico
    return [{"tipo": "desconhecido", "pagina_inicio": pag_inicio, "pagina_fim": pag_fim, "data": None}]


def _merge_boundary_docs(docs):
    """Junta documentos do mesmo tipo que ficaram separados na fronteira entre chunks."""
    if len(docs) <= 1:
        return docs
    merged = [docs[0]]
    for doc in docs[1:]:
        prev = merged[-1]
        # Mesmo tipo e paginas consecutivas? Provavelmente o mesmo documento
        if (limpar_nome(doc.get("tipo", "")) == limpar_nome(prev.get("tipo", "")) and
                doc.get("pagina_inicio", 0) == prev.get("pagina_fim", 0) + 1):
            prev["pagina_fim"] = doc["pagina_fim"]
            if doc.get("data") and not prev.get("data"):
                prev["data"] = doc["data"]
        else:
            merged.append(doc)
    return merged


def mapear_documentos_pdf(client, caminho, nome_arquivo):
    """Analisa PDF com multiplas paginas e identifica cada documento separado."""
    dados_extracao = extrair_textos_todas_paginas(caminho)
    paginas = dados_extracao["paginas"]
    truncado = dados_extracao.get("truncado", False)
    total_real = dados_extracao.get("total_real", 0)

    if not paginas:
        # PDF escaneado - tenta via imagem da primeira pagina
        img_b64 = pagina_para_imagem_b64(caminho, 1)
        if img_b64:
            resultado = analisar_imagem_simples(client, img_b64, nome_arquivo)
            return [{"tipo": resultado.get("tipo", "desconhecido"), "pagina_inicio": 1,
                     "pagina_fim": 1, "data": resultado.get("data")}]
        return [{"tipo": "desconhecido", "pagina_inicio": 1, "pagina_fim": 1, "data": None}]

    total_paginas = len(paginas)

    # Se PDF foi truncado, avisa nos resultados (pagina extra invisivel que o usuario pode ver no relatorio)
    aviso_truncado = None
    if truncado:
        aviso_truncado = f"ATENCAO: PDF tem {total_real} paginas, so foram analisadas as primeiras {MAX_PAGINAS_PDF}. Divida o arquivo em partes menores."

    if total_paginas == 1:
        # PDF de 1 pagina - analise simples
        texto = paginas[0]["texto"]
        if texto.strip():
            resultado = analisar_texto_simples(client, texto, nome_arquivo)
        else:
            img_b64 = pagina_para_imagem_b64(caminho, 1)
            resultado = analisar_imagem_simples(client, img_b64, nome_arquivo) if img_b64 else {"tipo": "desconhecido", "data": None}
        doc = {"tipo": resultado.get("tipo", "desconhecido"), "pagina_inicio": 1,
                 "pagina_fim": 1, "data": resultado.get("data")}
        if aviso_truncado:
            doc["_aviso"] = aviso_truncado
        return [doc]

    # === PDF pequeno (ate CHUNK_SIZE paginas): chamada unica (comportamento original) ===
    if total_paginas <= CHUNK_SIZE_PAGINAS:
        texto_completo = ""
        for p in paginas:
            texto_completo += f"\n--- PAGINA {p['pagina']} ---\n{p['texto']}\n"

        paginas_sem_texto = [p for p in paginas if not p["texto"].strip()]
        imagens_content = []
        if paginas_sem_texto and len(paginas_sem_texto) <= 3:
            for p in paginas_sem_texto[:2]:
                img_b64 = pagina_para_imagem_b64(caminho, p["pagina"])
                if img_b64:
                    imagens_content.append({"type": "text", "text": f"--- PAGINA {p['pagina']} (imagem) ---"})
                    imagens_content.append({
                        "type": "image",
                        "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64},
                    })
                    img_b64 = None

        messages_content = [{"type": "text",
            "text": f"Arquivo: {nome_arquivo} ({total_paginas} paginas)\n\n{texto_completo}"}]
        messages_content.extend(imagens_content)

        try:
            response = client.messages.create(
                model=MODELO_IA,
                max_tokens=1000,
                messages=[
                    {"role": "user", "content": messages_content},
                    {"role": "user", "content": PROMPT_MAPEAMENTO},
                ],
            )
            dados = _parse_json_response(response.content[0].text)
            docs = dados.get("documentos", [])
            if docs:
                if aviso_truncado:
                    docs[0]["_aviso"] = aviso_truncado
                return docs
        except Exception:
            pass

        doc = {"tipo": "desconhecido", "pagina_inicio": 1, "pagina_fim": total_paginas, "data": None}
        if aviso_truncado:
            doc["_aviso"] = aviso_truncado
        return [doc]

    # === PDF grande: processa em chunks de CHUNK_SIZE_PAGINAS paginas ===
    todos_docs = []
    for i in range(0, total_paginas, CHUNK_SIZE_PAGINAS):
        chunk = paginas[i:i + CHUNK_SIZE_PAGINAS]
        chunk_docs = _mapear_chunk(client, caminho, nome_arquivo, chunk)
        todos_docs.extend(chunk_docs)

    # Junta documentos que ficaram divididos na fronteira entre chunks
    todos_docs = _merge_boundary_docs(todos_docs)

    if not todos_docs:
        todos_docs = [{"tipo": "desconhecido", "pagina_inicio": 1, "pagina_fim": total_paginas, "data": None}]

    if aviso_truncado:
        todos_docs[0]["_aviso"] = aviso_truncado

    return todos_docs


def analisar_texto_simples(client, texto, nome_arquivo):
    """Analise simples de texto para extrair tipo e data."""
    try:
        response = client.messages.create(
            model=MODELO_IA,
            max_tokens=200,
            messages=[
                {"role": "user", "content": f"Nome do arquivo: {nome_arquivo}\n\nConteudo:\n{texto}"},
                {"role": "user", "content": PROMPT_EXTRACAO_SIMPLES},
            ],
        )
        resposta = response.content[0].text.strip()
        if resposta.startswith("{"):
            return json.loads(resposta)
        if "```" in resposta:
            json_str = resposta.split("```")[1]
            if json_str.startswith("json"):
                json_str = json_str[4:]
            return json.loads(json_str.strip())
        return json.loads(resposta)
    except Exception:
        return {"tipo": "desconhecido", "data": None}


def analisar_imagem_simples(client, imagem_b64, nome_arquivo):
    """Analise simples de imagem para extrair tipo e data."""
    try:
        response = client.messages.create(
            model=MODELO_IA,
            max_tokens=200,
            messages=[
                {"role": "user", "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": imagem_b64}},
                    {"type": "text", "text": f"Nome do arquivo: {nome_arquivo}"},
                ]},
                {"role": "user", "content": PROMPT_EXTRACAO_SIMPLES},
            ],
        )
        resposta = response.content[0].text.strip()
        if resposta.startswith("{"):
            return json.loads(resposta)
        return json.loads(resposta)
    except Exception:
        return {"tipo": "desconhecido", "data": None}


# ============ Extracao de data com fallbacks em cascata (#3.3) ============

# Regex para datas comuns em nomes de arquivo
# Aceita: 2023-03-15, 2023_03_15, 15-03-2023, 15/03/2023, 15.03.2023, 20230315
_RE_DATAS_FILENAME = [
    # YYYY-MM-DD ou YYYY_MM_DD ou YYYY.MM.DD
    (re.compile(r"(20\d{2})[-_./](\d{1,2})[-_./](\d{1,2})"), "ymd"),
    # DD-MM-YYYY ou DD/MM/YYYY
    (re.compile(r"(\d{1,2})[-_./](\d{1,2})[-_./](20\d{2})"), "dmy"),
    # YYYYMMDD compactado
    (re.compile(r"(20\d{2})(\d{2})(\d{2})"), "ymd"),
]


def extrair_data_pdf_metadata(caminho):
    """Tenta ler a data de criacao do PDF dos metadados. Retorna YYYY-MM-DD ou None."""
    try:
        from pypdf import PdfReader
        reader = PdfReader(caminho)
        meta = reader.metadata
        if not meta:
            return None
        # Pega a data mais antiga entre CreationDate e ModDate (geralmente mais confiavel)
        candidatos = []
        for campo in ["/CreationDate", "/ModDate"]:
            valor = meta.get(campo)
            if valor:
                candidatos.append(str(valor))
        for raw in candidatos:
            # Formato pdf: "D:20230315143022-03'00'" ou "D:20230315143022Z"
            m = re.search(r"(20\d{2})(\d{2})(\d{2})", raw)
            if m:
                ano, mes, dia = m.groups()
                # Valida mes/dia basicamente
                if 1 <= int(mes) <= 12 and 1 <= int(dia) <= 31:
                    return f"{ano}-{mes}-{dia}"
        return None
    except Exception:
        return None


def extrair_data_filename(nome_arquivo):
    """Tenta extrair uma data do nome do arquivo. Retorna YYYY-MM-DD ou None."""
    if not nome_arquivo:
        return None
    nome = Path(nome_arquivo).stem  # remove extensao
    for regex, formato in _RE_DATAS_FILENAME:
        m = regex.search(nome)
        if not m:
            continue
        g1, g2, g3 = m.groups()
        if formato == "ymd":
            ano, mes, dia = g1, g2.zfill(2), g3.zfill(2)
        else:  # dmy
            dia, mes, ano = g1.zfill(2), g2.zfill(2), g3
        # Validacao basica
        try:
            if 1 <= int(mes) <= 12 and 1 <= int(dia) <= 31 and 2000 <= int(ano) <= 2100:
                return f"{ano}-{mes}-{dia}"
        except ValueError:
            continue
    return None


def resolver_data_fallback(resultado, caminho_original, nome_original):
    """Aplica fallbacks em cascata pra tentar encontrar uma data quando a IA nao achou.
    Modifica o dicionario 'resultado' in-place adicionando:
    - data: YYYY-MM-DD ou None
    - _data_fonte: "ia" | "pdf_metadata" | "filename" | None
    """
    # Se IA ja achou a data, marca como tal e retorna
    if resultado.get("data"):
        resultado["_data_fonte"] = "ia"
        return

    # Fallback 1: metadata do PDF
    if caminho_original and Path(caminho_original).suffix.lower() == ".pdf":
        data_meta = extrair_data_pdf_metadata(caminho_original)
        if data_meta:
            resultado["data"] = data_meta
            resultado["_data_fonte"] = "pdf_metadata"
            return

    # Fallback 2: nome do arquivo original
    data_nome = extrair_data_filename(nome_original)
    if data_nome:
        resultado["data"] = data_nome
        resultado["_data_fonte"] = "filename"
        return

    # Sem data
    resultado["_data_fonte"] = None


def processar_arquivo_completo(client, caminho, nome_original, tmp_dir):
    """Processa um arquivo, separando-o em documentos individuais se necessario.
    Retorna lista de resultados (1 por documento encontrado)."""
    ext = Path(nome_original).suffix.lower()
    resultados = []

    if ext == ".pdf":
        # Mapeia todos os documentos dentro do PDF
        docs_encontrados = mapear_documentos_pdf(client, caminho, nome_original)

        import pdfplumber
        try:
            with pdfplumber.open(caminho) as pdf:
                total_paginas = len(pdf.pages)
        except Exception:
            total_paginas = 1

        if len(docs_encontrados) == 1 and total_paginas <= 2:
            # Documento unico, usa o arquivo original
            doc = docs_encontrados[0]
            resultados.append({
                "tipo": doc.get("tipo", "desconhecido"),
                "data": doc.get("data"),
                "arquivo_tmp": caminho,
                "nome_original": nome_original,
                "extensao": ".pdf",
                "_aviso": doc.get("_aviso"),
            })
        else:
            # Multiplos documentos - separa em arquivos individuais
            for i, doc in enumerate(docs_encontrados):
                p_inicio = doc.get("pagina_inicio", 1)
                p_fim = doc.get("pagina_fim", p_inicio)
                tipo = doc.get("tipo", "desconhecido")
                tipo_limpo = limpar_nome(tipo)[:30]

                novo_caminho = os.path.join(tmp_dir, f"split_{i}_{tipo_limpo}.pdf")
                if separar_pdf(caminho, p_inicio, p_fim, novo_caminho):
                    resultados.append({
                        "tipo": tipo,
                        "data": doc.get("data"),
                        "arquivo_tmp": novo_caminho,
                        "nome_original": f"{nome_original} (pag {p_inicio}-{p_fim})",
                        "extensao": ".pdf",
                        "_aviso": doc.get("_aviso") if i == 0 else None,
                    })

    elif ext in (".jpg", ".jpeg", ".png"):
        with open(caminho, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode()
        resultado = analisar_imagem_simples(client, img_b64, nome_original)
        resultados.append({
            "tipo": resultado.get("tipo", "desconhecido"),
            "data": resultado.get("data"),
            "arquivo_tmp": caminho,
            "nome_original": nome_original,
            "extensao": ext,
        })

    elif ext == ".docx":
        from docx import Document
        try:
            doc = Document(caminho)
            texto = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
            if texto:
                resultado = analisar_texto_simples(client, texto[:4000], nome_original)
            else:
                resultado = {"tipo": "desconhecido", "data": None}
        except Exception:
            resultado = {"tipo": "desconhecido", "data": None}

        resultados.append({
            "tipo": resultado.get("tipo", "desconhecido"),
            "data": resultado.get("data"),
            "arquivo_tmp": caminho,
            "nome_original": nome_original,
            "extensao": ext,
        })

    if not resultados:
        resultados.append({
            "tipo": "desconhecido",
            "data": None,
            "arquivo_tmp": caminho,
            "nome_original": nome_original,
            "extensao": ext,
        })

    return resultados


def eh_protocolo_assinatura(r):
    """Verifica se um documento e protocolo de assinatura."""
    tipo = limpar_nome(r.get("tipo", ""))
    return "protocolo" in tipo and "assinatura" in tipo


def classificar_doc(r, tipo_processo):
    """Retorna a categoria do doc para ordenacao baseada no tipo de processo."""
    tipo = limpar_nome(r.get("tipo", ""))

    # Protocolo de assinatura digital
    if "protocolo" in tipo and "assinatura" in tipo:
        return "protocolo_assinatura"
    # Substabelecimento (separado de procuracao)
    if "substabelecimento" in tipo:
        return "substabelecimento"
    # Procuracao
    if "procuracao" in tipo:
        return "procuracao"
    # Declaracao de hipossuficiencia (separada das demais)
    if "declaracao" in tipo and ("hipossuficiencia" in tipo or "pobreza" in tipo):
        return "declaracao_hipossuficiencia"
    # Outras declaracoes
    if "declaracao" in tipo and "tempo" in tipo and "servico" in tipo:
        return "declaracao_tempo_servico"
    if "declaracao" in tipo:
        return "declaracao"
    # Contrato de honorarios
    if "contrato" in tipo and "honorario" in tipo:
        return "contrato_de_honorarios"
    # Termo de responsabilidade
    if "termo" in tipo and "responsabilidade" in tipo:
        return "termo_de_responsabilidade"
    # Carta de indeferimento INSS
    if "carta" in tipo and "indeferimento" in tipo:
        return "carta_indeferimento"
    if "decisao" in tipo and "inss" in tipo:
        return "carta_indeferimento"
    # CNIS
    if "cnis" in tipo:
        return "cnis"
    # CTPS
    if "ctps" in tipo or ("carteira" in tipo and "trabalho" in tipo):
        return "ctps"
    # Documentos medicos
    if "laudo" in tipo and ("meuinss" in tipo or "meu_inss" in tipo):
        return "laudo_meuinss"
    if "exame" in tipo and "medico" in tipo:
        return "exames_medicos"
    if "exame" in tipo:
        return "exames_medicos"
    if any(k in tipo for k in ["atestado", "relatorio_medico", "receita_medica"]):
        return "atestados_relatorios_receitas"
    if "laudo" in tipo:
        return "atestados_relatorios_receitas"
    # Documentos rurais
    if "rural" in tipo and "homologacao" in tipo:
        return "termo_homologacao_rural"
    if "rural" in tipo or "atividade_rural" in tipo:
        return "documentos_rurais"
    # Folha V7
    if "folha_v7" in tipo or tipo == "folha_v7" or "v7" in tipo:
        return "folha_v7"
    # Certidoes
    if "certidao" in tipo and "tempo" in tipo and "servico" in tipo:
        return "certidao_tempo_servico"
    if "certidao" in tipo and "negativa" in tipo:
        return "certidao_negativa_estadual"
    if "certidao" in tipo and any(k in tipo for k in ["casamento", "nascimento", "obito"]):
        return "certidoes"
    if "certidao" in tipo:
        return "certidoes"
    # Ficha financeira / funcionario
    if "ficha" in tipo and "financeira" in tipo:
        return "ficha_financeira"
    if "ficha" in tipo and "funcionario" in tipo:
        return "declaracao_tempo_servico"
    # PPP / LTCAT
    if "ppp" in tipo or "perfil_profissiografico" in tipo:
        return "ppp"
    if "ltcat" in tipo or ("laudo" in tipo and "tecnico" in tipo):
        return "ltcat"
    # GPS
    if "gps" in tipo or ("guia" in tipo and "previdencia" in tipo):
        return "gps"
    # Comprovantes
    if "comprovante" in tipo and ("gasto" in tipo or "despesa" in tipo or "pagamento" in tipo):
        return "comprovante_gasto"
    if "comprovante" in tipo and "residencia" in tipo:
        return "comprovante_residencia"
    # Fotos
    if "foto" in tipo and "residencia" in tipo:
        return "foto_residencia"
    # Avaliacao social / pericia
    if ("avaliacao" in tipo and "social" in tipo) or "pericia_medica" in tipo or "pericia" in tipo:
        return "avaliacao_social_pericia"
    # Calculos
    if "contagem" in tipo and "tempo" in tipo:
        return "contagem_tempo"
    if "calculo" in tipo and ("transicao" in tipo or "regra" in tipo):
        return "calculo_regras_transicao"
    if "calculo" in tipo and ("rmi" in tipo or "renda_mensal" in tipo):
        return "calculo_rmi"
    # Copia processo
    if "copia" in tipo and "processo" in tipo:
        return "copia_processo_administrativo"
    if "processo_administrativo" in tipo:
        return "copia_processo_administrativo"
    # Documentos pessoais
    if tipo in ("rg", "cpf", "cnh") or \
       any(k in tipo for k in ["identidade", "cnh", "carteira_nacional"]):
        return "documento_pessoal"

    return None


def merge_pdfs(caminhos, destino):
    """Junta varios PDFs em um unico arquivo."""
    from pypdf import PdfReader, PdfWriter
    try:
        writer = PdfWriter()
        for caminho in caminhos:
            try:
                reader = PdfReader(caminho)
                for page in reader.pages:
                    writer.add_page(page)
            except Exception:
                continue
        with open(destino, "wb") as f:
            writer.write(f)
        return True
    except Exception:
        return False


# === ROTAS ===

@app.route("/")
def index():
    return render_template("index.html", tipos=TIPOS_PROCESSO, usuarios=USUARIOS)


def registrar_uso(usuario, nome_cliente, tipo_processo, total_docs, status):
    """Envia log de uso para webhook (n8n -> Google Sheets aba 'Uso')."""
    if not LOG_WEBHOOK_URL:
        return
    try:
        import urllib.request
        payload = json.dumps({
            "tipo": "uso",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "usuario": usuario,
            "cliente": nome_cliente,
            "tipo_processo": TIPOS_PROCESSO.get(tipo_processo, tipo_processo),
            "total_documentos": str(total_docs),
            "status": status,
        }).encode("utf-8")
        req = urllib.request.Request(
            LOG_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # nao quebra o app se o log falhar


def registrar_erro(usuario, nome_cliente, tipo_processo, arquivo, tipo_erro, mensagem):
    """Envia log de erro para webhook (n8n -> Google Sheets aba 'Erros').
    Usado quando algo da errado: exception, classificacao falhou, PDF truncado, etc.
    """
    if not ERRO_WEBHOOK_URL:
        return
    try:
        import urllib.request
        payload = json.dumps({
            "tipo": "erro",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "usuario": usuario or "desconhecido",
            "cliente": nome_cliente or "",
            "tipo_processo": TIPOS_PROCESSO.get(tipo_processo, tipo_processo or ""),
            "arquivo": arquivo or "",
            "tipo_erro": tipo_erro,  # ex: "excecao_processamento", "pdf_truncado", "api_anthropic"
            "mensagem": str(mensagem)[:500],  # trunca mensagens longas
        }).encode("utf-8")
        req = urllib.request.Request(
            ERRO_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # nao quebra o app se o log falhar


@app.route("/processar", methods=["POST"])
def processar():
    import anthropic

    # Limpa ZIPs antigos (>1h) a cada request para nao entupir o disco
    limpar_zips_antigos()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"erro": "API key nao configurada no servidor"}), 500

    usuario = request.form.get("usuario", "").strip()
    nome_cliente = request.form.get("nome_cliente", "").strip()
    tipo_processo = request.form.get("tipo_processo", "").strip()

    if not usuario or usuario not in USUARIOS:
        return jsonify({"erro": "Selecione o usuario que esta processando"}), 400
    if not nome_cliente:
        return jsonify({"erro": "Nome do cliente e obrigatorio"}), 400
    if tipo_processo not in TIPOS_PROCESSO:
        return jsonify({"erro": "Tipo de processo invalido"}), 400

    arquivos = request.files.getlist("documentos")
    if not arquivos or all(f.filename == "" for f in arquivos):
        return jsonify({"erro": "Nenhum documento enviado"}), 400

    arquivos_validos = [
        f for f in arquivos
        if f.filename and Path(f.filename).suffix.lower() in EXTENSOES_ACEITAS
    ]
    if not arquivos_validos:
        return jsonify({"erro": f"Nenhum arquivo valido. Aceitos: {', '.join(EXTENSOES_ACEITAS)}"}), 400

    tmp_dir = tempfile.mkdtemp()

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resultados = []

        for arquivo in arquivos_validos:
            nome_original = arquivo.filename
            ext = Path(nome_original).suffix.lower()
            tmp_path = os.path.join(tmp_dir, nome_original)
            arquivo.save(tmp_path)

            try:
                # Processa e separa documentos automaticamente
                docs_separados = processar_arquivo_completo(client, tmp_path, nome_original, tmp_dir)
                # Aplica fallback de data em cada documento (metadata PDF / nome do arquivo)
                for doc in docs_separados:
                    resolver_data_fallback(doc, tmp_path, nome_original)
                resultados.extend(docs_separados)
            except Exception as e:
                # Nao falha o request inteiro — adiciona como documento desconhecido
                resultados.append({
                    "tipo": "desconhecido",
                    "data": None,
                    "arquivo_tmp": tmp_path,
                    "nome_original": nome_original,
                    "extensao": ext,
                    "_data_fonte": None,
                })
                registrar_erro(usuario, nome_cliente, tipo_processo, nome_original,
                              "excecao_processamento_arquivo", e)

        # ===== ETAPA 1: Anexar protocolos de assinatura ao documento anterior =====
        # Para JUDICIAL/consumidor/trabalhista/civel: protocolo anexa ao doc assinado anterior
        # Para INSS ADMIN: protocolo vai direto pro grupo_procuracao_termos (nao anexa)
        resultados_processados = []
        protocolos_pendentes = []

        for r in resultados:
            if eh_protocolo_assinatura(r) and tipo_processo != "inss_admin":
                # Tenta anexar ao ultimo documento assinavel (so para judicial etc)
                if resultados_processados:
                    ultimo = resultados_processados[-1]
                    cat_ultimo = classificar_doc(ultimo, tipo_processo)
                    if cat_ultimo in CATEGORIAS_ASSINADAS:
                        # Merge protocolo com o documento anterior
                        merged_path = os.path.join(tmp_dir, f"merged_{len(resultados_processados)}.pdf")
                        if merge_pdfs([ultimo["arquivo_tmp"], r["arquivo_tmp"]], merged_path):
                            ultimo["arquivo_tmp"] = merged_path
                            ultimo["nome_original"] += " + protocolo"
                            continue
                # Se nao conseguiu anexar, guarda para tentar depois
                protocolos_pendentes.append(r)
            else:
                resultados_processados.append(r)

        # ===== ETAPA 2: Classificar documentos =====
        sequencia = SEQUENCIA_JUDICIAL if tipo_processo != "inss_admin" else SEQUENCIA_INSS_ADMIN
        categorias_validas = [cat for cat, _ in sequencia]

        # Para admin, tambem inclui categorias individuais que serao reagrupadas
        if tipo_processo == "inss_admin":
            for grupo, componentes in GRUPO_MERGE_ADMIN.items():
                categorias_validas.extend(componentes)

        docs_por_categoria = {cat: [] for cat in categorias_validas}
        docs_cronologicos = []

        for r in resultados_processados:
            cat = classificar_doc(r, tipo_processo)
            if cat and cat in docs_por_categoria:
                docs_por_categoria[cat].append(r)
            else:
                docs_cronologicos.append(r)

        # ===== ETAPA 2.5: Para admin, agrupar procuracao+substabelecimento+termo =====
        if tipo_processo == "inss_admin":
            for grupo, componentes in GRUPO_MERGE_ADMIN.items():
                grupo_docs = []
                for comp in componentes:
                    grupo_docs.extend(docs_por_categoria.pop(comp, []))
                if grupo_docs:
                    # Ordena na sequencia: procuracao primeiro, depois substabelecimento, depois termo
                    ordem_componentes = {c: i for i, c in enumerate(componentes)}
                    grupo_docs.sort(key=lambda r: ordem_componentes.get(
                        classificar_doc(r, tipo_processo), 99))
                    docs_por_categoria[grupo] = grupo_docs

        # Ordena cronologicamente os demais
        docs_cronologicos.sort(key=lambda r: r.get("data") or "9999-99-99")

        # Ordena cronologicamente dentro das categorias MERGE (CTPS, atestados, etc.)
        for cat in CATEGORIAS_MERGE:
            if cat in docs_por_categoria and cat not in GRUPO_MERGE_ADMIN:
                docs_por_categoria[cat].sort(key=lambda r: r.get("data") or "9999-99-99")

        # Monta ZIP
        zip_buffer = io.BytesIO()
        nome_limpo = limpar_nome(nome_cliente)
        # UUID curto (8 chars) para evitar race condition entre usuarios simultaneos
        uid = uuid.uuid4().hex[:8]
        nome_pasta = f"{nome_limpo}_{tipo_processo}_{uid}"
        # Nome "amigavel" para a pasta dentro do ZIP (sem o UUID, mais limpo)
        nome_pasta_zip = f"{nome_limpo}_{tipo_processo}"
        limite_mb = LIMITES_TAMANHO.get(tipo_processo, MAX_FILE_SIZE_BYTES) // (1024 * 1024)
        relatorio_linhas = [
            f"Relatorio de Organizacao - {nome_cliente}",
            f"Tipo: {TIPOS_PROCESSO[tipo_processo]}",
            f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"Total de documentos separados: {len(resultados_processados)}",
            f"Limite por arquivo: {limite_mb}MB",
            "-" * 50,
            "",
        ]

        # Adiciona avisos no topo do relatorio (ex: PDFs truncados)
        avisos_relatorio = list({r.get("_aviso") for r in resultados if r.get("_aviso")})
        if avisos_relatorio:
            relatorio_linhas.insert(5, "")
            relatorio_linhas.insert(5, "AVISOS:")
            for a in avisos_relatorio:
                relatorio_linhas.insert(6, f"  - {a}")

        lista_docs = []
        ordem = 1
        limite_arquivo = LIMITES_TAMANHO.get(tipo_processo, MAX_FILE_SIZE_BYTES)

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            # ===== ETAPA 3: Adicionar documentos na sequencia correta =====
            for cat, label in sequencia:
                docs_cat = docs_por_categoria.get(cat, [])
                if not docs_cat:
                    continue

                # Se categoria e MERGE, junta todos em um unico PDF
                if cat in CATEGORIAS_MERGE and len(docs_cat) > 1:
                    merged_path = os.path.join(tmp_dir, f"merged_{cat}.pdf")
                    caminhos = [r["arquivo_tmp"] for r in docs_cat]
                    if merge_pdfs(caminhos, merged_path):
                        nome_label = label or limpar_nome(docs_cat[0].get("tipo", "documento"))[:30].upper()
                        partes = dividir_pdf_por_tamanho(merged_path, tmp_dir, max_bytes=limite_arquivo)
                        for idx, parte_path in enumerate(partes):
                            sufixo = f"_parte{idx+1}" if len(partes) > 1 else ""
                            novo_nome = f"{ordem:02d}_{nome_limpo}_{nome_label}{sufixo}.pdf"
                            zf.write(parte_path, f"{nome_pasta_zip}/{novo_nome}")
                            relatorio_linhas.append(
                                f"{novo_nome} | {len(docs_cat)} docs merged em ordem cronologica"
                            )
                            lista_docs.append({
                                "ordem": ordem, "nome": novo_nome,
                                "original": f"{len(docs_cat)} documentos merged",
                                "tipo": label or cat, "data": None,
                            })
                            ordem += 1
                        continue

                # Sem merge: adiciona cada doc individualmente
                for r in docs_cat:
                    nome_label = label or limpar_nome(r.get("tipo", "documento"))[:30].upper()
                    # Verifica tamanho e divide se necessario
                    if r["extensao"] == ".pdf" and os.path.getsize(r["arquivo_tmp"]) > limite_arquivo:
                        partes = dividir_pdf_por_tamanho(r["arquivo_tmp"], tmp_dir, max_bytes=limite_arquivo)
                        for idx, parte_path in enumerate(partes):
                            sufixo = f"_parte{idx+1}" if len(partes) > 1 else ""
                            novo_nome = f"{ordem:02d}_{nome_limpo}_{nome_label}{sufixo}{r['extensao']}"
                            zf.write(parte_path, f"{nome_pasta_zip}/{novo_nome}")
                            relatorio_linhas.append(
                                f"{novo_nome} | Original: {r['nome_original']} | Data: {r.get('data', 'N/A')}"
                            )
                            lista_docs.append({
                                "ordem": ordem, "nome": novo_nome,
                                "original": r["nome_original"],
                                "tipo": r.get("tipo", "?"), "data": r.get("data"),
                            })
                            ordem += 1
                    else:
                        novo_nome = f"{ordem:02d}_{nome_limpo}_{nome_label}{r['extensao']}"
                        zf.write(r["arquivo_tmp"], f"{nome_pasta_zip}/{novo_nome}")
                        relatorio_linhas.append(
                            f"{novo_nome} | Original: {r['nome_original']} | Tipo: {r.get('tipo', '?')} | Data: {r.get('data', 'N/A')}"
                        )
                        lista_docs.append({
                            "ordem": ordem, "nome": novo_nome,
                            "original": r["nome_original"],
                            "tipo": r.get("tipo", "?"), "data": r.get("data"),
                        })
                        ordem += 1

            # ===== ETAPA 4: Demais documentos em ordem cronologica =====
            for r in docs_cronologicos:
                tipo_limpo = limpar_nome(r.get("tipo", "documento"))[:30]
                data_str = r.get("data") or "sem_data"
                if r["extensao"] == ".pdf" and os.path.getsize(r["arquivo_tmp"]) > limite_arquivo:
                    partes = dividir_pdf_por_tamanho(r["arquivo_tmp"], tmp_dir, max_bytes=limite_arquivo)
                    for idx, parte_path in enumerate(partes):
                        sufixo = f"_parte{idx+1}" if len(partes) > 1 else ""
                        novo_nome = f"{ordem:02d}_{nome_limpo}_{data_str}_{tipo_limpo}{sufixo}{r['extensao']}"
                        zf.write(parte_path, f"{nome_pasta_zip}/{novo_nome}")
                        fonte = r.get("_data_fonte")
                        fonte_txt = f" [fonte: {fonte}]" if fonte and fonte != "ia" else ""
                        relatorio_linhas.append(
                            f"{novo_nome} | Original: {r['nome_original']} | Data: {r.get('data', 'NAO ENCONTRADA')}{fonte_txt}"
                        )
                        lista_docs.append({
                            "ordem": ordem, "nome": novo_nome,
                            "original": r["nome_original"],
                            "tipo": r.get("tipo", "?"), "data": r.get("data"),
                            "data_fonte": r.get("_data_fonte"),
                        })
                        ordem += 1
                else:
                    novo_nome = f"{ordem:02d}_{nome_limpo}_{data_str}_{tipo_limpo}{r['extensao']}"
                    zf.write(r["arquivo_tmp"], f"{nome_pasta_zip}/{novo_nome}")
                    fonte = r.get("_data_fonte")
                    fonte_txt = f" [fonte: {fonte}]" if fonte and fonte != "ia" else ""
                    relatorio_linhas.append(
                        f"{novo_nome} | Original: {r['nome_original']} | Tipo: {r.get('tipo', '?')} | Data: {r.get('data', 'NAO ENCONTRADA')}{fonte_txt}"
                    )
                    lista_docs.append({
                        "ordem": ordem, "nome": novo_nome,
                        "original": r["nome_original"],
                        "tipo": r.get("tipo", "?"), "data": r.get("data"),
                        "data_fonte": r.get("_data_fonte"),
                    })
                    ordem += 1

            # Protocolos orfaos (caso nao tenham sido anexados)
            for r in protocolos_pendentes:
                novo_nome = f"{ordem:02d}_{nome_limpo}_Protocolo_Assinatura{r['extensao']}"
                zf.write(r["arquivo_tmp"], f"{nome_pasta_zip}/{novo_nome}")
                relatorio_linhas.append(
                    f"{novo_nome} | Original: {r['nome_original']} | PROTOCOLO ORFAO"
                )
                lista_docs.append({
                    "ordem": ordem, "nome": novo_nome,
                    "original": r["nome_original"],
                    "tipo": "Protocolo Assinatura", "data": None,
                })
                ordem += 1

            relatorio_linhas.append("")
            relatorio_linhas.append("Gerado por: Organizador Juridico AB Group")
            zf.writestr(f"{nome_pasta_zip}/_relatorio.txt", "\n".join(relatorio_linhas))

        zip_buffer.seek(0)

        shared_zip = SHARED_ZIP_DIR / f"{nome_pasta}.zip"
        with open(shared_zip, "wb") as f:
            f.write(zip_buffer.getvalue())

        shutil.rmtree(tmp_dir, ignore_errors=True)

        # Registra o uso na planilha (via webhook n8n)
        registrar_uso(usuario, nome_cliente, tipo_processo, len(resultados), "sucesso")

        # Coleta avisos (ex: PDF truncado por ter mais que MAX_PAGINAS_PDF)
        avisos = list({r.get("_aviso") for r in resultados if r.get("_aviso")})

        # Registra avisos como eventos de erro menores (pra monitoramento)
        for aviso in avisos:
            registrar_erro(usuario, nome_cliente, tipo_processo, "",
                          "pdf_truncado", aviso)

        # Registra se muitos documentos sairam como desconhecido (qualidade ruim)
        total_desconhecidos = sum(1 for r in resultados if r.get("tipo", "").lower() == "desconhecido")
        if total_desconhecidos > 0 and total_desconhecidos >= len(resultados) * 0.3:
            registrar_erro(usuario, nome_cliente, tipo_processo, "",
                          "muitos_desconhecidos",
                          f"{total_desconhecidos} de {len(resultados)} documentos nao foram classificados")

        return jsonify({
            "sucesso": True,
            "nome_pasta": nome_pasta,
            "documentos": lista_docs,
            "total": len(resultados),
            "com_data": sum(1 for r in resultados if r.get("data")),
            "sem_data": sum(1 for r in resultados if not r.get("data")),
            "avisos": avisos,
        })

    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        registrar_uso(usuario, nome_cliente, tipo_processo, 0, f"erro: {str(e)[:100]}")
        registrar_erro(usuario, nome_cliente, tipo_processo, "",
                      "excecao_geral_processar", e)
        return jsonify({"erro": str(e)}), 500


@app.route("/download/<nome_pasta>")
def download(nome_pasta):
    zip_path = SHARED_ZIP_DIR / f"{nome_pasta}.zip"
    if not zip_path.exists():
        return "Arquivo nao encontrado. Processe novamente.", 404

    # Remove UUID do nome do arquivo baixado (fica amigavel pro usuario)
    # nome_pasta = "joao_silva_inss_admin_abc12345" -> download "joao_silva_inss_admin_organizado.zip"
    nome_amigavel = nome_pasta.rsplit("_", 1)[0] if "_" in nome_pasta else nome_pasta
    return send_file(
        zip_path,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"{nome_amigavel}_organizado.zip",
    )


def limpar_zips_antigos(max_idade_segundos=3600):
    """Remove ZIPs com mais de 1 hora do diretorio compartilhado."""
    try:
        agora = time.time()
        for zip_file in SHARED_ZIP_DIR.glob("*.zip"):
            if agora - zip_file.stat().st_mtime > max_idade_segundos:
                try:
                    zip_file.unlink()
                except Exception:
                    pass
    except Exception:
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
