"""Fonte de dados: Yahoo Finance via dlt."""

from datetime import date
from typing import Iterator

import dlt
import yfinance as yf
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.config import TICKERS, DATA_INICIO, ATIVOS_META


@dlt.resource(
    name="precos_acoes",
    write_disposition="merge",
    primary_key=["ticker", "data"],
)
def precos_acoes(
    inicio: str = None,
    fim: str = None,
) -> Iterator[dict]:
    """Busca preços históricos de ações via Yahoo Finance."""
    _tickers = TICKERS
    _inicio = inicio or DATA_INICIO
    _fim = fim or date.today().isoformat()

    for ticker in _tickers:
        try:
            registros = _fetch_ticker(ticker, _inicio, _fim)
            for rec in registros:
                yield rec
        except Exception as exc:
            logger.error(f"Falha ao buscar {ticker}: {exc}")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _fetch_ticker(ticker: str, inicio: str, fim: str) -> list:
    """Baixa dados de um ticker com retry automático."""
    logger.info(f"Buscando {ticker} de {inicio} até {fim}...")
    t = yf.Ticker(ticker)
    df = t.history(start=inicio, end=fim, auto_adjust=False)

    if df.empty:
        logger.warning(f"Nenhum dado retornado para {ticker}.")
        return []

    df = df.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    registros = []
    for _, row in df.iterrows():
        rec = {
            "ticker":        ticker,
            "data":          row["date"].date() if hasattr(row["date"], "date") else row["date"],
            "abertura":      float(row.get("open",  0)) or None,
            "maxima":        float(row.get("high",  0)) or None,
            "minima":        float(row.get("low",   0)) or None,
            "fechamento":    float(row.get("close", 0)) or None,
            "fechamento_aj": float(row.get("adj_close", row.get("close", 0))) or None,
            "volume":        int(row.get("volume", 0)) or None,
        }
        registros.append(rec)

    logger.success(f"{ticker}: {len(registros)} registros válidos.")
    return registros


@dlt.resource(
    name="ativos",
    write_disposition="merge",
    primary_key=["ticker"],
)
def ativos() -> Iterator[dict]:
    """Metadados dos ativos monitorados."""
    for ticker, meta in ATIVOS_META.items():
        try:
            t = yf.Ticker(ticker)
            info = t.info or {}
            yield {
                "ticker":   ticker,
                "nome":     meta.get("nome") or info.get("longName", ticker),
                "setor":    meta.get("setor") or info.get("sector"),
                "subsetor": info.get("industry"),
                "tipo":     meta.get("tipo", "ACAO"),
                "moeda":    info.get("currency", "BRL"),
                "pais":     "Brasil",
            }
            logger.info(f"Metadados carregados: {ticker}")
        except Exception as exc:
            logger.warning(f"Não foi possível obter metadados de {ticker}: {exc}")
            yield {"ticker": ticker, **meta}
