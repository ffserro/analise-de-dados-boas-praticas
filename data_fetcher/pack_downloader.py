import os
import re
import requests
from zipfile import ZipFile
from bs4 import BeautifulSoup
from pathlib import Path
from io import BytesIO

MESES = [
    "Janeiro", "Fevereiro", "Marco", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
]

BASE_URL = "https://in.gov.br/acesso-a-informacao/dados-abertos/base-de-dados"

OUTPUT_FILE = Path("./documentos/dou_zips.txt")
XML_DIR = Path("./redownload")

EXPRESSAO_ALVO = "Ministério da Defesa"


def extrair_links_zip(ano, mes, session=None):
    session = session or requests.Session()

    resp = session.get(BASE_URL, params={"ano": ano, "mes": mes}, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    links = {
        a["href"]
        for a in soup.find_all("a", href=True)
        if ".zip" in a["href"].lower()
    }
    return sorted(links)


def carregar_links_existentes(path: Path):
    if not path.exists():
        return set()

    with open(path, "r", encoding="utf-8") as f:
        return {linha.strip() for linha in f if linha.strip()}


def salvar_link_processado(path: Path, link: str):
    with open(path, "a", encoding="utf-8") as f:
        f.write(link + "\n")


def extrair_metadados_zip(link: str):
    padroes = [
        # S01012024.zip.part001
        (
            r"(S\d{8}\.zip)\.part(\d{3})",
            lambda m: {
                "nome_zip": m.group(1),
                "grupo": m.group(1),
                "parte": int(m.group(2)),
            },
        ),
        # S01012024.zip.001
        (
            r"(S\d{8}\.zip)\.(\d{3})",
            lambda m: {
                "nome_zip": m.group(1),
                "grupo": m.group(1),
                "parte": int(m.group(2)),
            },
        ),
        # S01012024_parte01de03.zip  ou S01012024_parte1.zip
        (
            r"(S\d{8})_parte(\d{1,3})(?:de\d{1,3})?\.zip",
            lambda m: {
                "nome_zip": f"{m.group(1)}.zip",
                "grupo": f"{m.group(1)}.zip",
                "parte": int(m.group(2)),
            },
        ),
        # S01012024.zip
        (
            r"(S\d{8}\.zip)",
            lambda m: {
                "nome_zip": m.group(1),
                "grupo": m.group(1),
                "parte": None,
            },
        ),
    ]

    for padrao, parser in padroes:
        match = re.search(padrao, link, flags=re.IGNORECASE)
        if match:
            return parser(match)

    return None


def agrupar_links_por_zip(links):

    grupos = {}

    for link in links:
        meta = extrair_metadados_zip(link)
        if not meta:
            continue

        grupo = meta["grupo"]
        grupos.setdefault(grupo, []).append(
            {
                "link": link,
                "parte": meta["parte"],
                "nome_zip": meta["nome_zip"],
            }
        )

    for grupo, itens in grupos.items():
        grupos[grupo] = sorted(
            itens,
            key=lambda x: (x["parte"] is None, x["parte"] if x["parte"] is not None else 0)
        )

    return grupos


def decodificar_bytes_xml(data: bytes) -> str:
    for encoding in ("utf-8", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


def baixar_bytes(session, url):
    resp = session.get(url, timeout=120)
    resp.raise_for_status()
    return resp.content


def baixar_zip_ou_partes_em_memoria(session, itens_grupo):

    if len(itens_grupo) == 1 and itens_grupo[0]["parte"] is None:
        return BytesIO(baixar_bytes(session, itens_grupo[0]["link"]))

    buffer = BytesIO()
    for item in itens_grupo:
        parte_bytes = baixar_bytes(session, item["link"])
        buffer.write(parte_bytes)

    buffer.seek(0)
    return buffer


def processar_zip_em_memoria(zip_bytes, nome_zip: str, xml_base_dir: Path, expressao: str):
    pasta_saida = xml_base_dir / Path(nome_zip).stem
    pasta_saida.mkdir(parents=True, exist_ok=True)

    salvos = 0

    with ZipFile(zip_bytes) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            if not member.filename.lower().endswith(".xml"):
                continue

            with zf.open(member) as xml_file:
                xml_bytes = xml_file.read()

            texto = decodificar_bytes_xml(xml_bytes)

            if expressao in texto:
                destino_xml = pasta_saida / Path(member.filename).name
                with open(destino_xml, "wb") as f:
                    f.write(xml_bytes)
                salvos += 1

    if salvos == 0:
        try:
            pasta_saida.rmdir()
        except OSError:
            pass

    return salvos


def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    XML_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })

    links_existentes = carregar_links_existentes(OUTPUT_FILE)

    total_links_novos = 0
    total_xmls_salvos = 0

    for ano in range(2002, 2027):
        for mes in MESES:
            try:
                links = extrair_links_zip(ano, mes, session=session)
                novos = [link for link in links if link not in links_existentes]

                grupos = agrupar_links_por_zip(novos)
                xmls_mes = 0

                for grupo, itens_grupo in grupos.items():
                    nome_zip = itens_grupo[0]["nome_zip"]

                    try:
                        zip_bytes = baixar_zip_ou_partes_em_memoria(session, itens_grupo)

                        salvos = processar_zip_em_memoria(
                            zip_bytes=zip_bytes,
                            nome_zip=nome_zip,
                            xml_base_dir=XML_DIR,
                            expressao=EXPRESSAO_ALVO
                        )

                        for item in itens_grupo:
                            salvar_link_processado(OUTPUT_FILE, item["link"])
                            links_existentes.add(item["link"])
                            total_links_novos += 1

                        total_xmls_salvos += salvos
                        xmls_mes += salvos

                    except Exception as e:
                        partes = [x["link"] for x in itens_grupo]
                        print(f"Erro ao processar ZIP {nome_zip} ({len(partes)} parte(s)): {e}")

                print(
                    f"{ano} - {mes}: {len(links)} encontrados, "
                    f"{len(novos)} novos, {len(grupos)} grupo(s), {xmls_mes} XMLs salvos"
                )

            except Exception as e:
                print(f"Erro em {ano} - {mes}: {e}")

    print(f"\nTotal de novos links salvos: {total_links_novos}")
    print(f"Total de XMLs salvos: {total_xmls_salvos}")
    print(f"Arquivo de links: {OUTPUT_FILE}")
    print(f"Diretório dos XMLs: {XML_DIR}")


main()