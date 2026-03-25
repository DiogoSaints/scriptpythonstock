#!/usr/bin/env python3
"""
Sistema de verificacao de estoque Magento/Adobe Commerce.
Entrada: arquivo CSV com coluna SKU.
Saida: arquivo CSV (sku, data, estoque).
"""

import asyncio
import csv
import logging
import os
import sys
import time
from datetime import datetime
from typing import List, Optional
from urllib.parse import quote

import aiohttp
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Variavel de ambiente obrigatoria nao encontrada: {name}")
    return val


MG_RETRIABLE_EXC = (aiohttp.ClientError, asyncio.TimeoutError, OSError)


def _mg_retry():
    return retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(MG_RETRIABLE_EXC),
    )


def read_products_from_csv(csv_path: str) -> List[str]:
    if not os.path.exists(csv_path):
        raise RuntimeError(f"Arquivo CSV de entrada nao encontrado: {csv_path}")

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames:
            raise RuntimeError("CSV de entrada sem cabecalho.")

        field_map = {str(name).strip().lower(): name for name in reader.fieldnames if name}
        sku_field = field_map.get("sku")
        if not sku_field:
            raise RuntimeError("CSV de entrada precisa ter uma coluna chamada 'SKU'.")

        skus = []
        for row in reader:
            sku = str(row.get(sku_field, "")).strip()
            if sku:
                skus.append(sku)

    logger.info(f"SKUs lidos do CSV '{csv_path}': {len(skus)}")
    return skus


def write_stocks_to_csv(output_path: str, skus: List[str], stocks: List[int]) -> None:
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    data_hoje = datetime.utcnow().strftime("%Y-%m-%d")
    with open(output_path, "w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["sku", "data", "estoque"])
        for sku, saldo in zip(skus, stocks):
            writer.writerow([sku, data_hoje, saldo])

    logger.info(f"Arquivo CSV gerado com {len(skus)} linhas: {output_path}")


def normalize_proxy_line(line: str) -> Optional[str]:
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None

    if raw.startswith("http://") or raw.startswith("https://"):
        return raw

    parts = raw.split(":")
    if len(parts) == 4:
        host, port, user, password = parts
        return f"http://{quote(user)}:{quote(password)}@{host}:{port}"
    if len(parts) == 2:
        host, port = parts
        return f"http://{host}:{port}"
    return None


def load_proxies(proxies_file: str) -> List[str]:
    if not os.path.exists(proxies_file):
        raise RuntimeError(f"Arquivo de proxies nao encontrado: {proxies_file}")

    proxies: List[str] = []
    with open(proxies_file, "r", encoding="utf-8", newline="") as file:
        for line in file:
            proxy = normalize_proxy_line(line)
            if proxy:
                proxies.append(proxy)

    if not proxies:
        raise RuntimeError(f"Nenhum proxy valido encontrado em: {proxies_file}")
    return proxies


class AsyncMagentoStockChecker:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        rate_limit: float = 0.1,
        max_stock: int = 5000,
        max_workers: int = 20,
        proxies: Optional[List[str]] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Accept": "*/*",
            "Content-Type": "application/json",
            "X-Api-Key": api_key,
        }
        self.rate_limit = rate_limit
        self.max_stock = max_stock
        self._max_workers = max_workers
        self.proxies = proxies or []
        self._proxy_index = 0
        self.stats = {"processed": 0, "errors": 0, "start_time": None, "requests": 0}

    def _next_proxy(self) -> Optional[str]:
        if not self.proxies:
            return None
        proxy = self.proxies[self._proxy_index]
        self._proxy_index = (self._proxy_index + 1) % len(self.proxies)
        return proxy

    @_mg_retry()
    async def create_cart(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> Optional[str]:
        proxy = self._next_proxy()
        async with sem:
            async with session.post(f"{self.base_url}/rest/V1/guest-carts", proxy=proxy) as response:
                self.stats["requests"] += 1
                if response.status == 200:
                    return (await response.text()).strip('"')
                raise aiohttp.ClientError(f"Erro {response.status} em create_cart")

    @_mg_retry()
    async def add_item(self, session: aiohttp.ClientSession, cart_id: str, sku: str, qty: int):
        proxy = self._next_proxy()
        payload = {"cartItem": {"quoteId": cart_id, "sku": sku, "qty": qty}}
        async with session.post(
            f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items",
            json=payload,
            proxy=proxy,
        ) as response:
            self.stats["requests"] += 1
            if response.status == 200:
                data = await response.json()
                return True, data.get("item_id") or data.get("itemId")
            return False, None

    @_mg_retry()
    async def update_item(
        self,
        session: aiohttp.ClientSession,
        cart_id: str,
        item_id: str,
        sku: str,
        qty: int,
    ) -> bool:
        proxy = self._next_proxy()
        payload = {"cartItem": {"item_id": item_id, "quote_id": cart_id, "sku": sku, "qty": qty}}
        async with session.put(
            f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items/{item_id}",
            json=payload,
            proxy=proxy,
        ) as response:
            self.stats["requests"] += 1
            return response.status == 200

    async def delete_item(self, session: aiohttp.ClientSession, cart_id: str, item_id: str):
        proxy = self._next_proxy()
        try:
            async with session.delete(
                f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items/{item_id}",
                proxy=proxy,
            ):
                self.stats["requests"] += 1
        except Exception:
            pass

    async def check_stock(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore, sku: str) -> int:
        try:
            cart_id = await self.create_cart(session, sem)
            if not cart_id:
                return 0

            await asyncio.sleep(self.rate_limit)
            ok, item_id = await self.add_item(session, cart_id, sku, 1)
            if not ok or not item_id:
                return 0

            valid = 1
            candidate = 4
            while candidate <= self.max_stock:
                await asyncio.sleep(self.rate_limit)
                if await self.update_item(session, cart_id, item_id, sku, candidate):
                    valid = candidate
                    candidate *= 2
                else:
                    break

            low = valid
            high = min(candidate, self.max_stock)
            while low < high:
                mid = (low + high + 1) // 2
                await asyncio.sleep(self.rate_limit)
                if await self.update_item(session, cart_id, item_id, sku, mid):
                    low = mid
                else:
                    high = mid - 1

            await self.delete_item(session, cart_id, item_id)
            return low

        except Exception as exc:
            self.stats["errors"] += 1
            logger.error(f"Erro {sku}: {exc}")
            return 0
        finally:
            self.stats["processed"] += 1

    async def process_all(
        self,
        skus: List[str],
        sock_connect_timeout: float = 30,
        sock_read_timeout: float = 180,
    ) -> List[int]:
        self.stats["start_time"] = time.time()
        sem = asyncio.Semaphore(self._max_workers)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=sock_connect_timeout, sock_read=sock_read_timeout)
        connector = aiohttp.TCPConnector(limit=0)
        stocks: List[Optional[int]] = [None] * len(skus)

        async with aiohttp.ClientSession(headers=self.headers, timeout=timeout, connector=connector) as session:
            async def runner(idx: int, sku: str):
                stocks[idx] = await self.check_stock(session, sem, sku)

            tasks = [asyncio.create_task(runner(i, sku)) for i, sku in enumerate(skus)]
            await asyncio.gather(*tasks)

        return [stock if stock is not None else 0 for stock in stocks]


def main():
    input_csv = get_env("INPUT_CSV")
    output_csv = get_env("OUTPUT_CSV", required=False, default="estoque_saida.csv")
    magento_api_key = get_env("MAGENTO_API_KEY")
    magento_base_url = get_env("MAGENTO_BASE_URL")

    rate_limit = float(os.getenv("RATE_LIMIT", "0.1"))
    max_workers = int(os.getenv("MAX_WORKERS", "20"))
    max_stock = int(os.getenv("MAX_STOCK", "5000"))
    sock_connect_timeout = float(os.getenv("SOCK_CONNECT_TIMEOUT", "30"))
    sock_read_timeout = float(os.getenv("SOCK_READ_TIMEOUT", "180"))
    proxies_file = os.getenv("PROXIES_FILE", "").strip()
    proxies = load_proxies(proxies_file) if proxies_file else []

    logger.info("Iniciando sistema de verificacao de estoque Magento")
    logger.info(f"INPUT_CSV: {input_csv}")
    logger.info(f"OUTPUT_CSV: {output_csv}")
    if proxies_file:
        logger.info(f"PROXIES_FILE: {proxies_file} | proxies validos: {len(proxies)}")

    skus = read_products_from_csv(input_csv)
    if not skus:
        logger.error("Nenhum SKU encontrado no CSV de entrada.")
        sys.exit(1)

    checker = AsyncMagentoStockChecker(
        magento_base_url,
        magento_api_key,
        rate_limit=rate_limit,
        max_stock=max_stock,
        max_workers=max_workers,
        proxies=proxies,
    )

    stocks = asyncio.run(checker.process_all(skus, sock_connect_timeout, sock_read_timeout))
    write_stocks_to_csv(output_csv, skus, stocks)

    elapsed = time.time() - checker.stats["start_time"]
    speed = checker.stats["processed"] / elapsed if elapsed > 0 else 0
    logger.info("===================================================")
    logger.info(f"Concluido! {checker.stats['processed']} SKUs em {elapsed:.1f}s")
    logger.info(
        f"Velocidade media: {speed:.2f} it/s | Erros: {checker.stats['errors']} | Reqs: {checker.stats['requests']}"
    )
    logger.info("===================================================")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Falha na execucao principal")
        sys.exit(1)
