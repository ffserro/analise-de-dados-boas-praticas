import re
from glob import iglob
from pathlib import Path

import polars as pl

XML_GLOB = "redownload/**/*.xml"
DEFAULT_BATCH_SIZE = 50_000
DATABASE_DIR = Path("./database_2")
METADATA_PARQUET = DATABASE_DIR / "metadata.parquet"
TEXT_PARQUET = DATABASE_DIR / "textos.parquet"
MAX_PARQUET_SIZE_MB = 50
MAX_PARQUET_SIZE_BYTES = MAX_PARQUET_SIZE_MB * 1024 * 1024

ARTICLE_RE = re.compile(r"<article\s+(.+?)>", flags=re.IGNORECASE | re.DOTALL)
ARTICLE_ATTR_RE = re.compile(r'(\w+?)="(.*?)"', flags=re.DOTALL)


def _read_xml_compact(file_path: str) -> str | None:
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as file:
            return "".join(line.strip() for line in file if line.strip())
    except OSError:
        return None


def _extract_tag_content(text: str, tag: str) -> str | None:
    if re.search(fr"<{tag}\b[^>]*/>", text, flags=re.IGNORECASE):
        return None

    match = re.search(
        fr"<{tag}\b[^>]*>(.*?)</{tag}>",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        return None

    content = match.group(1).strip()
    if not content:
        return None

    cdata_match = re.fullmatch(r"\s*<!\[CDATA\[(.*?)\]\]>\s*", content, flags=re.DOTALL)
    if cdata_match is not None:
        content = cdata_match.group(1)

    content = content.strip()
    return content or None


def _extract_texto(text: str) -> str | None:
    texto = _extract_tag_content(text, "Texto")
    if texto is None:
        return None

    paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", texto, flags=re.IGNORECASE | re.DOTALL)
    if not paragraphs:
        cleaned = re.sub(r"</?strong>", "", texto, flags=re.IGNORECASE).strip()
        return cleaned or None

    cleaned_paragraphs = []
    for paragraph in paragraphs:
        paragraph = re.sub(r"</?strong>", "", paragraph, flags=re.IGNORECASE).strip()
        if paragraph:
            cleaned_paragraphs.append(paragraph)

    if not cleaned_paragraphs:
        return None

    return "\n".join(cleaned_paragraphs)


def _extract_autores(text: str) -> str | None:
    autores_raw = _extract_tag_content(text, "Autores")
    if autores_raw is None:
        return None

    autores = []
    for autor in re.findall(r"<assina>(.*?)</assina>", autores_raw, flags=re.IGNORECASE | re.DOTALL):
        autor = re.sub(r"\s+", " ", autor).strip()
        if autor:
            autores.append(autor)

    if not autores:
        return None

    return str(autores)


def parse_xml_record(file_path: str) -> dict[str, str | None] | None:
    text = _read_xml_compact(file_path)
    if text is None:
        return None

    article_match = ARTICLE_RE.search(text)
    if article_match is None:
        return None

    record: dict[str, str | None] = {
        key: value.strip() for key, value in ARTICLE_ATTR_RE.findall(article_match.group(1))
    }
    record["id"] = Path(file_path).stem
    record["identifica"] = _extract_tag_content(text, "Identifica")
    record["data"] = _extract_tag_content(text, "Data")
    record["ementa"] = _extract_tag_content(text, "Ementa")
    record["titulo"] = _extract_tag_content(text, "Titulo")
    record["subtitulo"] = _extract_tag_content(text, "SubTitulo")
    record["texto"] = _extract_texto(text)
    record["autores"] = _extract_autores(text)
    return record


def extract_article_info(file_path: str) -> dict[str, str | None] | None:
    record = parse_xml_record(file_path)
    if record is None:
        return None
    record.pop("texto", None)
    return record


def extract_text(file_path: str) -> dict[str, str | None] | None:
    record = parse_xml_record(file_path)
    if record is None:
        return None
    return {"id": record["id"], "texto": record.get("texto")}


def _build_chunks(
    parser,
    xml_glob: str = XML_GLOB,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> pl.DataFrame:
    chunks: list[pl.DataFrame] = []
    rows: list[dict[str, str | None]] = []

    for file_path in iglob(xml_glob, recursive=True):
        info = parser(file_path)
        if info is None:
            continue

        rows.append(info)
        if len(rows) >= batch_size:
            chunks.append(pl.from_dicts(rows, infer_schema_length=None))
            rows.clear()

    if rows:
        chunks.append(pl.from_dicts(rows, infer_schema_length=None))

    if not chunks:
        return pl.DataFrame()

    if len(chunks) == 1:
        return chunks[0]

    return pl.concat(chunks, how="diagonal_relaxed", rechunk=True)


def build_metadata(xml_glob: str = XML_GLOB, batch_size: int = DEFAULT_BATCH_SIZE) -> pl.DataFrame:
    return _build_chunks(extract_article_info, xml_glob=xml_glob, batch_size=batch_size)


def build_dataframe(xml_glob: str = XML_GLOB, batch_size: int = DEFAULT_BATCH_SIZE) -> pl.DataFrame:
    return _build_chunks(extract_text, xml_glob=xml_glob, batch_size=batch_size)


def _part_path(base_path: Path, part_number: int) -> Path:
    return base_path.with_name(f"{base_path.stem}_part_{part_number:04d}.parquet")


def _cleanup_previous_parts(base_path: Path) -> None:
    if base_path.exists():
        base_path.unlink()

    for old_part in base_path.parent.glob(f"{base_path.stem}_part_*.parquet"):
        old_part.unlink()


def write_parquet_in_parts(
    df: pl.DataFrame,
    base_path: Path,
    max_size_bytes: int = MAX_PARQUET_SIZE_BYTES,
) -> list[Path]:
    _cleanup_previous_parts(base_path)

    if df.is_empty():
        output_path = _part_path(base_path, 1)
        df.write_parquet(output_path)
        return [output_path]

    estimated_size = int(df.estimated_size())
    if estimated_size > 0:
        initial_rows = max(1, int((max_size_bytes * df.height / estimated_size) * 2))
    else:
        initial_rows = min(df.height, 100_000)

    written_files: list[Path] = []
    start = 0
    part_number = 1

    while start < df.height:
        remaining = df.height - start
        rows_for_chunk = min(remaining, initial_rows)
        output_path = _part_path(base_path, part_number)

        while True:
            chunk = df.slice(start, rows_for_chunk)
            chunk.write_parquet(output_path)
            file_size = output_path.stat().st_size

            if file_size <= max_size_bytes or rows_for_chunk == 1:
                if file_size > max_size_bytes and rows_for_chunk == 1:
                    print(
                        f"Aviso: 1 linha excedeu {MAX_PARQUET_SIZE_MB}MB em {output_path} "
                        f"({file_size / (1024 * 1024):.2f} MB)."
                    )
                break

            new_rows = max(1, int(rows_for_chunk * max_size_bytes / file_size))
            if new_rows >= rows_for_chunk:
                new_rows = rows_for_chunk - 1
            rows_for_chunk = max(1, new_rows)

        written_files.append(output_path)
        start += rows_for_chunk
        part_number += 1

    return written_files


def main() -> None:
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)

    meta_df = build_metadata()
    text_df = build_dataframe()

    meta_files = write_parquet_in_parts(meta_df, METADATA_PARQUET, MAX_PARQUET_SIZE_BYTES)
    text_files = write_parquet_in_parts(text_df, TEXT_PARQUET, MAX_PARQUET_SIZE_BYTES)

    print(f"Metadata salvo em {len(meta_files)} arquivo(s), total de {meta_df.height} linhas.")
    print(f"Textos salvos em {len(text_files)} arquivo(s), total de {text_df.height} linhas.")
    print(f"Diretorio de saida: {DATABASE_DIR.resolve()}")


if __name__ == "__main__":
    main()
