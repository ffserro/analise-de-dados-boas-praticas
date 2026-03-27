import re 
from glob import iglob
from pathlib import Path
from xml.etree import ElementTree as ET

import polars as pl

XML_GLOB = "redownload/**/*.xml"
DEFAULT_BATCH_SIZE = 50_000
DATABASE_DIR = Path("./database")
METADATA_PARQUET = DATABASE_DIR / "metadata.parquet"
TEXT_PARQUET = DATABASE_DIR / "textos.parquet"
MAX_PARQUET_SIZE_MB = 50
MAX_PARQUET_SIZE_BYTES = MAX_PARQUET_SIZE_MB * 1024 * 1024


def _local_tag_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def extract_article_info(file_path: str) -> dict[str, str] | None:
    try:
        for _, element in ET.iterparse(file_path, events=("start",)):
            if _local_tag_name(element.tag) == "article":
                info = dict(element.attrib)
                info["id"] = Path(file_path).stem
                return info
    except ET.ParseError:
        return None

    return None

def extract_text(file_path: str) -> dict[str, str] | None:
    # try:
    #     for _, element in ET.iterparse(file_path, events=("start",)):
    #         if _local_tag_name(element.tag) == "Texto":
    #             info = {"texto": '\n'.join([x.strip() for x in re.findall(r"<p\sclass=\'corpo.+\'>(.+?)<\/p>", re.sub(r'<\/p>\s+<p', '</p>\n<p', element.text.replace('\n', ' ').replace('<br>', '')))]) if element.text else ""}
    #             info["id"] = Path(file_path).stem
    #             return info
    # except ET.ParseError:
    #     return None

    # return None
    
    content = ET.parse(file_path)
    content = re.sub(r'<\/p>\s+<p', '</p>\n<p', content.findtext('.//Texto').replace('\n', ' ').replace('<br>', ''))
    info = {'texto': '\n'.join([t.strip() for t in re.findall(r"<p.*?>(.+?)<\/p>", content)])}
    info['id'] = Path(file_path).stem
    
    return info


def build_metadata(xml_glob: str = XML_GLOB, batch_size: int = DEFAULT_BATCH_SIZE) -> pl.DataFrame:
    chunks: list[pl.DataFrame] = []
    rows: list[dict[str, str]] = []

    for file_path in iglob(xml_glob, recursive=True):
        info = extract_article_info(file_path)
        if info is None:
            continue

        rows.append(info)

        if len(rows) >= batch_size:
            chunks.append(pl.from_dicts(rows))
            rows.clear()

    if rows:
        chunks.append(pl.from_dicts(rows))

    if not chunks:
        return pl.DataFrame()

    if len(chunks) == 1:
        return chunks[0]

    return pl.concat(chunks, how="diagonal_relaxed", rechunk=True)

def build_dataframe(xml_glob: str = XML_GLOB, batch_size: int = DEFAULT_BATCH_SIZE) -> pl.DataFrame:
    chunks: list[pl.DataFrame] = []
    rows: list[dict[str, str]] = []

    for file_path in iglob(xml_glob, recursive=True):
        info = extract_text(file_path)
        if info is None:
            continue

        rows.append(info)

        if len(rows) >= batch_size:
            chunks.append(pl.from_dicts(rows))
            rows.clear()

    if rows:
        chunks.append(pl.from_dicts(rows))

    if not chunks:
        return pl.DataFrame()

    if len(chunks) == 1:
        return chunks[0]

    return pl.concat(chunks, how="diagonal_relaxed", rechunk=True)


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
