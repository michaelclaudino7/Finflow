"""Fonte de dados: Banco Central e IBGE via dlt."""

from datetime import date
from typing import Iterator

import dlt
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.config import BCB_BASE_URL, BCB_SERIES, DATA_INICIO


@dlt.resource(
    name="indicadores_macro",
    write_disposition="merge",
    primary_key=["indicador", "data"],
)
def indicadores_macro(
    inicio: str = None,
    fim: str = None,
) -> Iterator[dict]:
    """Busca indicadores macroeconômicos do BCB e IBGE."""
    inicio = inicio or DATA_INICIO
    fim = fim or date.today().isoformat()

    unidades = {
        "selic_meta":   "% a.a.",
        "selic_diaria": "% a.d.",
        "usd_brl":      "BRL",
        "eur_brl":      "BRL",
        "igpm":         "% mês",
        "cdi":          "% a.d.",
        "pib_mensal":   "índice",
    }

    grupos = {
        "selic_meta":   "juros",
        "selic_diaria": "juros",
        "cdi":          "juros",
        "usd_brl":      "cambio",
        "eur_brl":      "cambio",
        "igpm":         "inflacao",
        "pib_mensal":   "atividade",
    }

    for nome, codigo in BCB_SERIES.items():
        logger.info(f"Buscando série BCB: {nome} (código {codigo})...")
        try:
            dados = _get_serie_bcb(codigo, inicio, fim)
            for item in dados:
                try:
                    yield {
                        "data":      _parse_bcb_date(item["data"]),
                        "indicador": nome,
                        "valor":     float(item["valor"].replace(",", ".")) if isinstance(item["valor"], str) else float(item["valor"]),
                        "unidade":   unidades.get(nome, "%"),
                        "grupo":     grupos.get(nome, "outro"),
                        "fonte":     "bcb",
                    }
                except (ValueError, KeyError) as exc:
                    logger.warning(f"Registro inválido ({nome}): {exc}")

            logger.success(f"{nome}: {len(dados)} registros coletados.")
        except Exception as exc:
            logger.error(f"Falha ao buscar série {nome} ({codigo}): {exc}")

    yield from _fetch_ipca(inicio, fim)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _get_serie_bcb(codigo: int, inicio: str, fim: str) -> list:
    url = BCB_BASE_URL.format(codigo=codigo)
    params = {
        "formato":     "json",
        "dataInicial": _to_bcb_date(inicio),
        "dataFinal":   _to_bcb_date(fim),
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _to_bcb_date(d: str) -> str:
    parts = d.split("-")
    return f"{parts[2]}/{parts[1]}/{parts[0]}"


def _parse_bcb_date(d: str) -> date:
    parts = d.split("/")
    return date(int(parts[2]), int(parts[1]), int(parts[0]))


def _fetch_ipca(inicio: str, fim: str) -> Iterator[dict]:
    ano_ini, mes_ini, _ = inicio.split("-")
    ano_fim, mes_fim, _ = fim.split("-")
    periodo = f"{ano_ini}{mes_ini}|{ano_fim}{mes_fim}"

    url = f"https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/{periodo}/variaveis/2266"
    params = {"localidades": "N1[all]"}

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        count = 0
        for var in data:
            for resultado in var.get("resultados", []):
                for periodo_str, valor in resultado.get("series", [{}])[0].get("serie", {}).items():
                    try:
                        ano = int(periodo_str[:4])
                        mes = int(periodo_str[4:])
                        yield {
                            "data":      date(ano, mes, 1),
                            "indicador": "ipca",
                            "valor":     float(valor),
                            "unidade":   "% mês",
                            "grupo":     "inflacao",
                            "fonte":     "ibge",
                        }
                        count += 1
                    except Exception as exc:
                        logger.warning(f"IPCA registro inválido ({periodo_str}): {exc}")

        logger.success(f"IPCA: {count} registros coletados.")
    except Exception as exc:
        logger.error(f"Falha ao buscar IPCA: {exc}")
