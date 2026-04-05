#!/usr/bin/env python3
"""
Organizador de Documentos Juridicos - AB Group
App web mobile-first para organizar documentos em ordem cronologica via IA.
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
MAX_TEXTO_CHARS = 4000

TIPOS_PROCESSO = {
    "inss_admin": "INSS Administrativo",
    "judicial": "Judicial",
    "consumidor": "Direito do Consumidor",
    "trabalhista": "Trabalhista",
    "civel": "Cível",
}

PROMPT_EXTRACAO = """Analise este documento juridico brasileiro.
Extraia:
1. Tipo do documento. Use EXATAMENTE uma destas categorias se o documento se encaixar:
   - "Procuracao" (procuracao ad judicia, substabelecimento, etc.)
   - "Declaracao" (declaracao de hipossuficiencia, declaracao de residencia, etc.)
   - "Contrato de Honorarios" (contrato de honorarios advocaticios, contrato de prestacao de servicos juridicos, etc.)
   - Se nao for nenhum dos tres acima, descreva o tipo em poucas palavras (ex: "RG", "CNIS", "Laudo Medico", "Decisao INSS", etc.)
2. Data de emissao/expedicao do documento (a data em que o documento foi emitido, nao datas mencionadas no texto)

Responda APENAS em JSON valido, sem markdown:
{"tipo": "...", "data": "YYYY-MM-DD"}

Se nao encontrar data de emissao, use "data": null.
"""

# Documentos com ordem fixa (sempre vem primeiro, nesta ordem)
DOCS_ORDEM_FIXA = ["procuracao", "declaracao", "contrato_de_honorarios"]


def limpar_nome(texto):
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = texto.lower().replace(" ", "_")
    texto = "".join(c for c in texto if c.isalnum() or c in ("_", "-"))
    return texto


def extrair_texto_pdf(caminho):
    import pdfplumber
    try:
        with pdfplumber.open(caminho) as pdf:
            if not pdf.pages:
                return None
            texto = pdf.pages[0].extract_text()
            if texto and len(texto.strip()) > 20:
                return texto[:MAX_TEXTO_CHARS]
            return None
    except Exception:
        return None


def pdf_para_imagem_b64(caminho):
    from pdf2image import convert_from_path
    try:
        imagens = convert_from_path(caminho, first_page=1, last_page=1, dpi=150)
        if imagens:
            buffer = io.BytesIO()
            imagens[0].save(buffer, format="JPEG", quality=80)
            return base64.b64encode(buffer.getvalue()).decode()
        return None
    except Exception:
        return None


def extrair_texto_docx(caminho):
    from docx import Document
    try:
        doc = Document(caminho)
        texto = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
        return texto[:MAX_TEXTO_CHARS] if texto else None
    except Exception:
        return None


def analisar_com_ia(client, texto=None, imagem_b64=None, nome_arquivo=""):
    messages_content = []

    if texto:
        messages_content.append(
            {"type": "text", "text": f"Nome do arquivo: {nome_arquivo}\n\nConteudo:\n{texto}"}
        )
    elif imagem_b64:
        ext = nome_arquivo.rsplit(".", 1)[-1].lower()
        media_type = "image/jpeg" if ext in ("jpg", "jpeg") else "image/png"
        messages_content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": media_type, "data": imagem_b64},
        })
        messages_content.append({"type": "text", "text": f"Nome do arquivo: {nome_arquivo}"})
    else:
        return {"tipo": "desconhecido", "data": None}

    try:
        response = client.messages.create(
            model=MODELO_IA,
            max_tokens=200,
            messages=[
                {"role": "user", "content": messages_content},
                {"role": "user", "content": PROMPT_EXTRACAO},
            ],
        )
        resposta_texto = response.content[0].text.strip()
        if resposta_texto.startswith("{"):
            return json.loads(resposta_texto)
        if "```" in resposta_texto:
            json_str = resposta_texto.split("```")[1]
            if json_str.startswith("json"):
                json_str = json_str[4:]
            return json.loads(json_str.strip())
        return json.loads(resposta_texto)
    except (json.JSONDecodeError, Exception):
        return {"tipo": "desconhecido", "data": None}


def processar_arquivo(client, caminho, nome_original):
    ext = Path(nome_original).suffix.lower()

    if ext == ".pdf":
        texto = extrair_texto_pdf(caminho)
        if texto:
            return analisar_com_ia(client, texto=texto, nome_arquivo=nome_original)
        img_b64 = pdf_para_imagem_b64(caminho)
        if img_b64:
            return analisar_com_ia(client, imagem_b64=img_b64, nome_arquivo=nome_original)
    elif ext in (".jpg", ".jpeg", ".png"):
        with open(caminho, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode()
        return analisar_com_ia(client, imagem_b64=img_b64, nome_arquivo=nome_original)
    elif ext == ".docx":
        texto = extrair_texto_docx(caminho)
        if texto:
            return analisar_com_ia(client, texto=texto, nome_arquivo=nome_original)

    return {"tipo": "desconhecido", "data": None}


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

    # Filtra extensoes validas
    arquivos_validos = [
        f for f in arquivos
        if f.filename and Path(f.filename).suffix.lower() in EXTENSOES_ACEITAS
    ]
    if not arquivos_validos:
        return jsonify({"erro": f"Nenhum arquivo valido. Aceitos: {', '.join(EXTENSOES_ACEITAS)}"}), 400

    # Cria pasta temporaria
    tmp_dir = tempfile.mkdtemp()

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resultados = []

        for arquivo in arquivos_validos:
            # Salva arquivo temporario
            nome_original = arquivo.filename
            tmp_path = os.path.join(tmp_dir, nome_original)
            arquivo.save(tmp_path)

            # Processa com IA
            resultado = processar_arquivo(client, tmp_path, nome_original)
            resultado["arquivo_tmp"] = tmp_path
            resultado["nome_original"] = nome_original
            resultado["extensao"] = Path(nome_original).suffix.lower()
            resultados.append(resultado)

        # Classifica documentos: ordem fixa vs demais (cronologicos)
        def classificar_doc(r):
            """Retorna a categoria do doc para ordenacao fixa."""
            tipo = limpar_nome(r.get("tipo", ""))
            if "procuracao" in tipo or "substabelecimento" in tipo:
                return "procuracao"
            elif "declaracao" in tipo:
                return "declaracao"
            elif "contrato" in tipo and "honorario" in tipo:
                return "contrato_de_honorarios"
            return None

        docs_fixos = {cat: [] for cat in DOCS_ORDEM_FIXA}
        docs_cronologicos = []

        for r in resultados:
            cat = classificar_doc(r)
            if cat:
                docs_fixos[cat].append(r)
            else:
                docs_cronologicos.append(r)

        # Ordena cronologicos por data (sem data vai pro final)
        docs_cronologicos.sort(key=lambda r: r.get("data") or "9999-99-99")

        # Monta ZIP
        zip_buffer = io.BytesIO()
        nome_limpo = limpar_nome(nome_cliente)
        nome_pasta = f"{nome_limpo}_{tipo_processo}"
        relatorio_linhas = [
            f"Relatorio de Organizacao - {nome_cliente}",
            f"Tipo: {TIPOS_PROCESSO[tipo_processo]}",
            f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"Total: {len(resultados)}",
            "-" * 50,
            "",
        ]

        lista_docs = []
        ordem = 1

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            # 1) Documentos com ordem fixa: Procuracao, Declaracao, Contrato
            nomes_fixos = {
                "procuracao": "Procuracao",
                "declaracao": "Declaracao",
                "contrato_de_honorarios": "Contrato_de_Honorarios",
            }
            for cat in DOCS_ORDEM_FIXA:
                for r in docs_fixos[cat]:
                    tipo_label = nomes_fixos[cat]
                    novo_nome = f"{ordem:02d}_{nome_limpo}_{tipo_label}{r['extensao']}"
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

        # Salva ZIP no diretorio compartilhado (acessivel por todos os workers)
        shared_zip = SHARED_ZIP_DIR / f"{nome_pasta}.zip"
        with open(shared_zip, "wb") as f:
            f.write(zip_buffer.getvalue())

        # Limpa arquivos temporarios de processamento
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
    # Busca no diretorio compartilhado (funciona com multiplos workers)
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
