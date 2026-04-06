#!/usr/bin/env python3
"""
Organizador de Documentos Juridicos - AB Group
App web mobile-first para organizar documentos em ordem cronologica via IA.
Separa automaticamente PDFs com multiplos documentos em arquivos individuais.
"""

import os
import io
import json
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
MAX_TEXTO_POR_PAGINA = 800  # menor para economizar memoria
MAX_PAGINAS_PDF = 30  # limite de paginas para evitar OOM

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

PROMPT_MAPEAMENTO = """Analise o texto de cada pagina deste PDF juridico brasileiro.
Identifique TODOS os documentos separados que existem dentro deste arquivo.

Tipos comuns de documentos:
- Procuracao, Substabelecimento
- Declaracao de Hipossuficiencia/Pobreza, Declaracao
- Contrato de Honorarios
- Termo de Responsabilidade, Termo de Representacao
- Protocolo de Assinatura (gerado por DocuSign, Clicksign, ZapSign, D4Sign, etc. - geralmente vem APOS um documento assinado)
- RG, CPF, CNH, Documento de Identidade
- CNIS, CTPS, Carteira de Trabalho
- Carta Indeferimento INSS, Decisao INSS
- Atestado Medico, Relatorio Medico, Receita Medica
- Exame Medico, Laudo Medico, Laudo MeuINSS
- Certidao de Casamento, Certidao de Nascimento, Certidao de Obito
- Termo de Homologacao Atividade Rural, Documentos Rurais
- Folha V7, Declaracao de Tempo de Servico, Ficha do Funcionario
- Certidao de Tempo de Servico, Ficha Financeira
- PPP - Perfil Profissiografico Previdenciario
- LTCAT - Laudo Tecnico das Condicoes de Trabalho
- GPS - Guia Pagamento Previdencia Social
- Comprovante de Residencia, Comprovante de Gasto
- Foto de Residencia
- Avaliacao Social, Pericia Medica
- Contagem de Tempo, Calculo Renda Mensal
- Copia Processo Administrativo
- Certidao Negativa Justica Estadual

IMPORTANTE:
- Cada documento pode ter 1 ou mais paginas
- "Protocolo de Assinatura" e um documento separado mas geralmente segue o documento assinado (procuracao, declaracao, etc.)
- Identifique onde cada documento COMECA e TERMINA

Responda APENAS em JSON valido, sem markdown:
{"documentos": [
  {"tipo": "Procuracao", "pagina_inicio": 1, "pagina_fim": 2, "data": "YYYY-MM-DD"},
  {"tipo": "Protocolo de Assinatura", "pagina_inicio": 3, "pagina_fim": 3, "data": null},
  {"tipo": "RG", "pagina_inicio": 4, "pagina_fim": 4, "data": null}
]}

Se nao encontrar data, use "data": null.
"""

PROMPT_EXTRACAO_SIMPLES = """Analise este documento juridico brasileiro.
Extraia:
1. Tipo do documento (use os tipos: Procuracao, Substabelecimento, Declaracao de Hipossuficiencia, Contrato de Honorarios, Termo de Responsabilidade, Protocolo de Assinatura, RG, CPF, CNH, CNIS, CTPS, Carta Indeferimento INSS, Atestado Medico, Relatorio Medico, Receita Medica, Exame Medico, Laudo Medico, Laudo MeuINSS, Certidao de Casamento, Certidao de Nascimento, Certidao de Obito, Documentos Rurais, Folha V7, Declaracao Tempo Servico, Ficha Funcionario, Certidao Tempo Servico, Ficha Financeira, PPP, LTCAT, GPS, Comprovante Residencia, Comprovante Gasto, Foto Residencia, etc.)
2. Data de emissao/expedicao do documento

Responda APENAS em JSON valido, sem markdown:
{"tipo": "...", "data": "YYYY-MM-DD"}

Se nao encontrar data, use "data": null.
"""

# Sequencia para INSS Administrativo (simples)
SEQUENCIA_INSS_ADMIN = [
    ("procuracao", "Procuracao"),
    ("substabelecimento", "Substabelecimento"),
    ("declaracao", "Declaracao"),
    ("contrato_de_honorarios", "Contrato_de_Honorarios"),
    ("termo_de_responsabilidade", "Termo_de_Responsabilidade"),
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


def limpar_nome(texto):
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = texto.lower().replace(" ", "_")
    texto = "".join(c for c in texto if c.isalnum() or c in ("_", "-"))
    return texto


def extrair_textos_todas_paginas(caminho):
    """Extrai texto de todas as paginas de um PDF (com limite para economizar memoria)."""
    import pdfplumber
    try:
        paginas = []
        with pdfplumber.open(caminho) as pdf:
            total = min(len(pdf.pages), MAX_PAGINAS_PDF)
            for i in range(total):
                try:
                    texto = pdf.pages[i].extract_text() or ""
                except Exception:
                    texto = ""
                # Pega so as primeiras linhas de cada pagina (suficiente para identificar tipo)
                linhas = texto.split("\n")[:15]
                paginas.append({"pagina": i + 1, "texto": "\n".join(linhas)[:MAX_TEXTO_POR_PAGINA]})
        return paginas
    except Exception:
        return []


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


def mapear_documentos_pdf(client, caminho, nome_arquivo):
    """Analisa PDF com multiplas paginas e identifica cada documento separado."""
    paginas = extrair_textos_todas_paginas(caminho)

    if not paginas:
        # PDF escaneado - tenta via imagem da primeira pagina
        img_b64 = pagina_para_imagem_b64(caminho, 1)
        if img_b64:
            resultado = analisar_imagem_simples(client, img_b64, nome_arquivo)
            return [{"tipo": resultado.get("tipo", "desconhecido"), "pagina_inicio": 1,
                     "pagina_fim": 1, "data": resultado.get("data")}]
        return [{"tipo": "desconhecido", "pagina_inicio": 1, "pagina_fim": 1, "data": None}]

    total_paginas = len(paginas)

    if total_paginas == 1:
        # PDF de 1 pagina - analise simples
        texto = paginas[0]["texto"]
        if texto.strip():
            resultado = analisar_texto_simples(client, texto, nome_arquivo)
        else:
            img_b64 = pagina_para_imagem_b64(caminho, 1)
            resultado = analisar_imagem_simples(client, img_b64, nome_arquivo) if img_b64 else {"tipo": "desconhecido", "data": None}
        return [{"tipo": resultado.get("tipo", "desconhecido"), "pagina_inicio": 1,
                 "pagina_fim": 1, "data": resultado.get("data")}]

    # PDF com multiplas paginas - pede para IA mapear todos os documentos
    texto_completo = ""
    for p in paginas:
        texto_completo += f"\n--- PAGINA {p['pagina']} ---\n{p['texto']}\n"

    # Para paginas sem texto (escaneadas), envia imagem apenas da primeira pagina sem texto
    paginas_sem_texto = [p for p in paginas if not p["texto"].strip()]
    imagens_content = []
    if paginas_sem_texto and len(paginas_sem_texto) <= 3:
        for p in paginas_sem_texto[:2]:  # max 2 imagens para nao estourar memoria
            img_b64 = pagina_para_imagem_b64(caminho, p["pagina"])
            if img_b64:
                imagens_content.append({
                    "type": "text", "text": f"--- PAGINA {p['pagina']} (imagem) ---"
                })
                imagens_content.append({
                    "type": "image",
                    "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64},
                })
                img_b64 = None  # libera memoria

    messages_content = []
    messages_content.append({
        "type": "text",
        "text": f"Arquivo: {nome_arquivo} ({total_paginas} paginas)\n\n{texto_completo}"
    })
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
        resposta = response.content[0].text.strip()
        if resposta.startswith("{"):
            dados = json.loads(resposta)
        elif "```" in resposta:
            json_str = resposta.split("```")[1]
            if json_str.startswith("json"):
                json_str = json_str[4:]
            dados = json.loads(json_str.strip())
        else:
            dados = json.loads(resposta)

        docs = dados.get("documentos", [])
        if docs:
            return docs
    except Exception:
        pass

    # Fallback: trata como documento unico
    return [{"tipo": "desconhecido", "pagina_inicio": 1, "pagina_fim": total_paginas, "data": None}]


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
    return render_template("index.html", tipos=TIPOS_PROCESSO)


@app.route("/processar", methods=["POST"])
def processar():
    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"erro": "API key nao configurada no servidor"}), 500

    nome_cliente = request.form.get("nome_cliente", "").strip()
    tipo_processo = request.form.get("tipo_processo", "").strip()

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
            tmp_path = os.path.join(tmp_dir, nome_original)
            arquivo.save(tmp_path)

            # Processa e separa documentos automaticamente
            # (a divisao por tamanho acontece DEPOIS, ao montar o ZIP final)
            docs_separados = processar_arquivo_completo(client, tmp_path, nome_original, tmp_dir)
            resultados.extend(docs_separados)

        # ===== ETAPA 1: Anexar protocolos de assinatura ao documento anterior =====
        # Protocolos vem logo apos o documento assinado no PDF original
        resultados_processados = []
        protocolos_pendentes = []

        for r in resultados:
            if eh_protocolo_assinatura(r):
                # Tenta anexar ao ultimo documento assinavel
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
        sequencia = SEQUENCIA_JUDICIAL if tipo_processo == "judicial" else SEQUENCIA_INSS_ADMIN
        categorias_validas = [cat for cat, _ in sequencia]

        docs_por_categoria = {cat: [] for cat in categorias_validas}
        docs_cronologicos = []

        for r in resultados_processados:
            cat = classificar_doc(r, tipo_processo)
            if cat and cat in docs_por_categoria:
                docs_por_categoria[cat].append(r)
            else:
                docs_cronologicos.append(r)

        # Ordena cronologicamente os demais
        docs_cronologicos.sort(key=lambda r: r.get("data") or "9999-99-99")

        # Ordena cronologicamente dentro das categorias que precisam (CTPS, atestados, etc.)
        for cat in CATEGORIAS_MERGE:
            if cat in docs_por_categoria:
                docs_por_categoria[cat].sort(key=lambda r: r.get("data") or "9999-99-99")

        # Monta ZIP
        zip_buffer = io.BytesIO()
        nome_limpo = limpar_nome(nome_cliente)
        nome_pasta = f"{nome_limpo}_{tipo_processo}"
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
                            zf.write(parte_path, f"{nome_pasta}/{novo_nome}")
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
                            zf.write(parte_path, f"{nome_pasta}/{novo_nome}")
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
                        zf.write(r["arquivo_tmp"], f"{nome_pasta}/{novo_nome}")
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
                        zf.write(parte_path, f"{nome_pasta}/{novo_nome}")
                        relatorio_linhas.append(
                            f"{novo_nome} | Original: {r['nome_original']} | Data: {r.get('data', 'NAO ENCONTRADA')}"
                        )
                        lista_docs.append({
                            "ordem": ordem, "nome": novo_nome,
                            "original": r["nome_original"],
                            "tipo": r.get("tipo", "?"), "data": r.get("data"),
                        })
                        ordem += 1
                else:
                    novo_nome = f"{ordem:02d}_{nome_limpo}_{data_str}_{tipo_limpo}{r['extensao']}"
                    zf.write(r["arquivo_tmp"], f"{nome_pasta}/{novo_nome}")
                    relatorio_linhas.append(
                        f"{novo_nome} | Original: {r['nome_original']} | Tipo: {r.get('tipo', '?')} | Data: {r.get('data', 'NAO ENCONTRADA')}"
                    )
                    lista_docs.append({
                        "ordem": ordem, "nome": novo_nome,
                        "original": r["nome_original"],
                        "tipo": r.get("tipo", "?"), "data": r.get("data"),
                    })
                    ordem += 1

            # Protocolos orfaos (caso nao tenham sido anexados)
            for r in protocolos_pendentes:
                novo_nome = f"{ordem:02d}_{nome_limpo}_Protocolo_Assinatura{r['extensao']}"
                zf.write(r["arquivo_tmp"], f"{nome_pasta}/{novo_nome}")
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
            zf.writestr(f"{nome_pasta}/_relatorio.txt", "\n".join(relatorio_linhas))

        zip_buffer.seek(0)

        shared_zip = SHARED_ZIP_DIR / f"{nome_pasta}.zip"
        with open(shared_zip, "wb") as f:
            f.write(zip_buffer.getvalue())

        shutil.rmtree(tmp_dir, ignore_errors=True)

        return jsonify({
            "sucesso": True,
            "nome_pasta": nome_pasta,
            "documentos": lista_docs,
            "total": len(resultados),
            "com_data": sum(1 for r in resultados if r.get("data")),
            "sem_data": sum(1 for r in resultados if not r.get("data")),
        })

    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return jsonify({"erro": str(e)}), 500


@app.route("/download/<nome_pasta>")
def download(nome_pasta):
    zip_path = SHARED_ZIP_DIR / f"{nome_pasta}.zip"
    if not zip_path.exists():
        return "Arquivo nao encontrado. Processe novamente.", 404

    return send_file(
        zip_path,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"{nome_pasta}_organizado.zip",
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
