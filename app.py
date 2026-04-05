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
MAX_TEXTO_POR_PAGINA = 1500

TIPOS_PROCESSO = {
    "inss_admin": "INSS Administrativo",
    "judicial": "Judicial",
    "consumidor": "Direito do Consumidor",
    "trabalhista": "Trabalhista",
    "civel": "Cível",
}

PROMPT_MAPEAMENTO = """Analise o texto de cada pagina deste PDF juridico brasileiro.
Identifique TODOS os documentos separados que existem dentro deste arquivo.

Documentos comuns: Procuracao, Declaracao (hipossuficiencia, residencia, etc.), Contrato de Honorarios, Termo de Responsabilidade, RG, CPF, CNH, CNIS, CTPS, Laudo Medico, Atestado, Comprovante de Residencia, Certidao, Decisao INSS, Contrato, Holerite, etc.

IMPORTANTE:
- Cada documento pode ter 1 ou mais paginas
- Um documento de identidade (RG, CPF, CNH) geralmente e 1 pagina
- Uma procuracao pode ter 1-3 paginas
- Identifique onde cada documento COMECA e TERMINA

Responda APENAS em JSON valido, sem markdown:
{"documentos": [
  {"tipo": "Procuracao", "pagina_inicio": 1, "pagina_fim": 2, "data": "YYYY-MM-DD"},
  {"tipo": "RG", "pagina_inicio": 3, "pagina_fim": 3, "data": null},
  ...
]}

Se o PDF tem apenas 1 documento, retorne 1 item na lista.
Se nao encontrar data de emissao, use "data": null.
"""

PROMPT_EXTRACAO_SIMPLES = """Analise este documento juridico brasileiro.
Extraia:
1. Tipo do documento (Procuracao, Declaracao, Contrato de Honorarios, Termo de Responsabilidade, RG, CPF, CNH, CNIS, Laudo Medico, etc.)
2. Data de emissao/expedicao do documento

Responda APENAS em JSON valido, sem markdown:
{"tipo": "...", "data": "YYYY-MM-DD"}

Se nao encontrar data, use "data": null.
"""

# Documentos com ordem fixa (sempre vem primeiro, nesta ordem)
DOCS_ORDEM_FIXA = [
    "procuracao",
    "declaracao",
    "contrato_de_honorarios",
    "termo_de_responsabilidade",
    "documento_pessoal",
]


def limpar_nome(texto):
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = texto.lower().replace(" ", "_")
    texto = "".join(c for c in texto if c.isalnum() or c in ("_", "-"))
    return texto


def extrair_textos_todas_paginas(caminho):
    """Extrai texto de TODAS as paginas de um PDF."""
    import pdfplumber
    try:
        paginas = []
        with pdfplumber.open(caminho) as pdf:
            for i, page in enumerate(pdf.pages):
                texto = page.extract_text() or ""
                paginas.append({"pagina": i + 1, "texto": texto[:MAX_TEXTO_POR_PAGINA]})
        return paginas
    except Exception:
        return []


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

    # Se alguma pagina nao tem texto (escaneada), usa imagem
    paginas_sem_texto = [p for p in paginas if not p["texto"].strip()]
    imagens_content = []
    for p in paginas_sem_texto[:5]:  # max 5 imagens para nao estourar tokens
        img_b64 = pagina_para_imagem_b64(caminho, p["pagina"])
        if img_b64:
            imagens_content.append({
                "type": "text", "text": f"--- PAGINA {p['pagina']} (imagem) ---"
            })
            imagens_content.append({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64},
            })

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


def classificar_doc(r):
    """Retorna a categoria do doc para ordenacao fixa."""
    tipo = limpar_nome(r.get("tipo", ""))
    if "procuracao" in tipo or "substabelecimento" in tipo:
        return "procuracao"
    elif "declaracao" in tipo:
        return "declaracao"
    elif "contrato" in tipo and "honorario" in tipo:
        return "contrato_de_honorarios"
    elif "termo" in tipo and "responsabilidade" in tipo:
        return "termo_de_responsabilidade"
    elif tipo in ("rg", "cpf", "cnh", "identidade", "carteira_de_identidade",
                   "documento_de_identidade", "carteira_nacional_de_habilitacao") or \
         "identidade" in tipo or "cnh" in tipo:
        return "documento_pessoal"
    return None


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
            docs_separados = processar_arquivo_completo(client, tmp_path, nome_original, tmp_dir)
            resultados.extend(docs_separados)

        # Classifica: ordem fixa vs demais (cronologicos)
        docs_fixos = {cat: [] for cat in DOCS_ORDEM_FIXA}
        docs_cronologicos = []

        for r in resultados:
            cat = classificar_doc(r)
            if cat:
                docs_fixos[cat].append(r)
            else:
                docs_cronologicos.append(r)

        docs_cronologicos.sort(key=lambda r: r.get("data") or "9999-99-99")

        # Monta ZIP
        zip_buffer = io.BytesIO()
        nome_limpo = limpar_nome(nome_cliente)
        nome_pasta = f"{nome_limpo}_{tipo_processo}"
        relatorio_linhas = [
            f"Relatorio de Organizacao - {nome_cliente}",
            f"Tipo: {TIPOS_PROCESSO[tipo_processo]}",
            f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"Total de documentos separados: {len(resultados)}",
            "-" * 50,
            "",
        ]

        lista_docs = []
        ordem = 1

        nomes_fixos = {
            "procuracao": "Procuracao",
            "declaracao": "Declaracao",
            "contrato_de_honorarios": "Contrato_de_Honorarios",
            "termo_de_responsabilidade": "Termo_de_Responsabilidade",
            "documento_pessoal": None,  # usa o tipo real (RG, CPF, CNH)
        }

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            # 1) Documentos com ordem fixa
            for cat in DOCS_ORDEM_FIXA:
                for r in docs_fixos[cat]:
                    label = nomes_fixos[cat]
                    if label is None:
                        label = limpar_nome(r.get("tipo", "documento"))[:30]
                        label = label.upper()
                    novo_nome = f"{ordem:02d}_{nome_limpo}_{label}{r['extensao']}"
                    zf.write(r["arquivo_tmp"], f"{nome_pasta}/{novo_nome}")
                    relatorio_linhas.append(
                        f"{novo_nome} | Original: {r['nome_original']} | Tipo: {r.get('tipo', '?')} | Data: {r.get('data', 'N/A')}"
                    )
                    lista_docs.append({
                        "ordem": ordem,
                        "nome": novo_nome,
                        "original": r["nome_original"],
                        "tipo": r.get("tipo", "?"),
                        "data": r.get("data"),
                    })
                    ordem += 1

            # 2) Demais documentos em ordem cronologica
            for r in docs_cronologicos:
                tipo_limpo = limpar_nome(r.get("tipo", "documento"))[:30]
                data_str = r.get("data") or "sem_data"
                novo_nome = f"{ordem:02d}_{nome_limpo}_{data_str}_{tipo_limpo}{r['extensao']}"
                zf.write(r["arquivo_tmp"], f"{nome_pasta}/{novo_nome}")
                relatorio_linhas.append(
                    f"{novo_nome} | Original: {r['nome_original']} | Tipo: {r.get('tipo', '?')} | Data: {r.get('data', 'NAO ENCONTRADA')}"
                )
                lista_docs.append({
                    "ordem": ordem,
                    "nome": novo_nome,
                    "original": r["nome_original"],
                    "tipo": r.get("tipo", "?"),
                    "data": r.get("data"),
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
