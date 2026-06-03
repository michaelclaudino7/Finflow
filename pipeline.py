"""Pipeline principal do FinFlow usando dlt.

Uso:
    python3 pipeline.py              # ingestão incremental
    python3 pipeline.py --full       # carga completa desde DATA_INICIO
    python3 pipeline.py --only precos
    python3 pipeline.py --only macro
    python3 pipeline.py --only ativos
"""

import argparse
import sys
from datetime import date

import dlt
from loguru import logger

from ingestion.config import (
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
    DATA_INICIO,
)
from ingestion.sources.yahoo import precos_acoes, ativos
from ingestion.sources.bcb import indicadores_macro


def build_pipeline() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="finflow",
        destination=dlt.destinations.postgres(
            f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        ),
        dataset_name="raw",
    )


def run(full: bool = False, only: str = None) -> None:
    inicio = DATA_INICIO if full else None
    fim = date.today().isoformat()

    pipeline = build_pipeline()

    logger.info(f"FinFlow pipeline — {'completo' if full else 'incremental'} — {date.today()}")

    resources = []

    if only is None or only == "ativos":
        resources.append(ativos())

    if only is None or only == "precos":
        resources.append(precos_acoes(inicio=inicio, fim=fim))

    if only is None or only == "macro":
        resources.append(indicadores_macro(inicio=inicio, fim=fim))

    load_info = pipeline.run(resources)

    logger.success(f"Pipeline concluída: {load_info}")

    schema = pipeline.default_schema
    if schema:
        logger.info(f"Tabelas gerenciadas pelo dlt: {list(schema.tables.keys())}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FinFlow dlt pipeline")
    parser.add_argument("--full", action="store_true", help="Carga completa desde DATA_INICIO")
    parser.add_argument("--only", choices=["precos", "macro", "ativos"], help="Roda apenas uma fonte")
    args = parser.parse_args()

    try:
        run(full=args.full, only=args.only)
    except Exception as exc:
        logger.error(f"Erro na pipeline: {exc}")
        sys.exit(1)
