#!/usr/bin/env python3
"""
Sistema de verificacao de estoque Magento/Adobe Commerce.
Entrada: arquivo CSV com coluna SKU.
Saida: arquivo CSV (sku, data, estoque).

Inclui rotacao inteligente de proxies:
- remove proxy com 407
- aplica cooldown em proxies com timeout/broken pipe
- prioriza proxies com melhor taxa de sucesso
"""

import asyncio
import csv
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
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

    logger.info("SKUs lidos do CSV '%s': %s", csv_path, len(skus))
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

    logger.info("Arquivo CSV gerado com %s linhas: %s", len(skus), output_path)


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


@dataclass
class ProxyState:
    proxy: str
    success_count: int = 0
    fail_count: int = 0
    auth_failures: int = 0
    timeout_failures: int = 0
    broken_pipe_failures: int = 0
    cooldown_until: float = 0.0
    disabled: bool = False

    @property
    def score(self) -> float:
        return (self.success_count * 3) - (self.fail_count * 2)


class SmartProxyPool:
    def __init__(self, proxies: List[str]):
        self._states = [ProxyState(proxy=proxy) for proxy in proxies]
        self._index = 0

    def stats(self) -> Dict[str, int]:
        return {
            "total": len(self._states),
            "active": sum(1 for state in self._states if not state.disabled),
            "disabled": sum(1 for state in self._states if state.disabled),
            "cooldown": sum(1 for state in self._states if (not state.disabled) and state.cooldown_until > time.time()),
        }

    def _active_candidates(self) -> List[ProxyState]:
        now = time.time()
        ready = [state for state in self._states if not state.disabled and state.cooldown_until <= now]
        if ready:
            return ready
        fallback = [state for state in self._states if not state.disabled]
        return fallback

    def acquire(self) -> Optional[ProxyState]:
        if not self._states:
            return None

        candidates = self._active_candidates()
        if not candidates:
            return None

        candidates.sort(key=lambda state: (state.score, state.success_count), reverse=True)
        choice = candidates[self._index % len(candidates)]
        self._index = (self._index + 1) % max(len(candidates), 1)
        return choice

    def report_success(self, state: Optional[ProxyState]) -> None:
        if state is None:
            return
        state.success_count += 1
        state.cooldown_until = 0.0

    def report_failure(self, state: Optional[ProxyState], exc: Exception = None, status: int = 0) -> None:
        if state is None:
            return

        state.fail_count += 1

        if status == 407:
            state.auth_failures += 1
            state.disabled = True
            logger.warning("Proxy removido por 407: %s", state.proxy)
            return

        text = str(exc or "").lower()
        if "broken pipe" in text:
            state.broken_pipe_failures += 1
        if "timeout" in text:
            state.timeout_failures += 1

        penalty = min(300, 15 * max(1, state.fail_count))
        if state.timeout_failures >= 3 or state.broken_pipe_failures >= 3:
            penalty = max(penalty, 180)
        state.cooldown_until = time.time() + penalty

        if state.fail_count >= 8:
            state.disabled = True
            logger.warning("Proxy removido por excesso de falhas: %s", state.proxy)


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
        self.proxy_pool = SmartProxyPool(proxies or [])
        self.stats = {"processed": 0, "errors": 0, "start_time": None, "requests": 0}

    def _acquire_proxy(self) -> Optional[ProxyState]:
        return self.proxy_pool.acquire()

    @_mg_retry()
    async def create_cart(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> Optional[str]:
        proxy_state = self._acquire_proxy()
        proxy = proxy_state.proxy if proxy_state else None
        accounted = False
        try:
            async with sem:
                async with session.post(f"{self.base_url}/rest/V1/guest-carts", proxy=proxy) as response:
                    self.stats["requests"] += 1
                    if response.status == 407:
                        self.proxy_pool.report_failure(proxy_state, status=407)
                        accounted = True
                        raise aiohttp.ClientError("Proxy Authentication Required")
                    if response.status < 500:
                        self.proxy_pool.report_success(proxy_state)
                    else:
                        self.proxy_pool.report_failure(proxy_state, status=response.status)
                        accounted = True
                    if response.status == 200:
                        return (await response.text()).strip('"')
                    raise aiohttp.ClientError(f"Erro {response.status} em create_cart")
        except Exception as exc:
            if not accounted:
                self.proxy_pool.report_failure(proxy_state, exc=exc)
            raise exc

    @_mg_retry()
    async def add_item(self, session: aiohttp.ClientSession, cart_id: str, sku: str, qty: int):
        proxy_state = self._acquire_proxy()
        proxy = proxy_state.proxy if proxy_state else None
        accounted = False
        payload = {"cartItem": {"quoteId": cart_id, "sku": sku, "qty": qty}}
        try:
            async with session.post(
                f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items",
                json=payload,
                proxy=proxy,
            ) as response:
                self.stats["requests"] += 1
                if response.status == 407:
                    self.proxy_pool.report_failure(proxy_state, status=407)
                    accounted = True
                    raise aiohttp.ClientError("Proxy Authentication Required")
                if response.status < 500:
                    self.proxy_pool.report_success(proxy_state)
                else:
                    self.proxy_pool.report_failure(proxy_state, status=response.status)
                    accounted = True
                if response.status == 200:
                    data = await response.json()
                    return True, data.get("item_id") or data.get("itemId")
                return False, None
        except Exception as exc:
            if not accounted:
                self.proxy_pool.report_failure(proxy_state, exc=exc)
            raise exc

    @_mg_retry()
    async def update_item(
        self,
        session: aiohttp.ClientSession,
        cart_id: str,
        item_id: str,
        sku: str,
        qty: int,
    ) -> bool:
        proxy_state = self._acquire_proxy()
        proxy = proxy_state.proxy if proxy_state else None
        accounted = False
        payload = {"cartItem": {"item_id": item_id, "quote_id": cart_id, "sku": sku, "qty": qty}}
        try:
            async with session.put(
                f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items/{item_id}",
                json=payload,
                proxy=proxy,
            ) as response:
                self.stats["requests"] += 1
                if response.status == 407:
                    self.proxy_pool.report_failure(proxy_state, status=407)
                    accounted = True
                    raise aiohttp.ClientError("Proxy Authentication Required")
                if response.status < 500:
                    self.proxy_pool.report_success(proxy_state)
                else:
                    self.proxy_pool.report_failure(proxy_state, status=response.status)
                    accounted = True
                return response.status == 200
        except Exception as exc:
            if not accounted:
                self.proxy_pool.report_failure(proxy_state, exc=exc)
            raise exc

    async def delete_item(self, session: aiohttp.ClientSession, cart_id: str, item_id: str):
        proxy_state = self._acquire_proxy()
        proxy = proxy_state.proxy if proxy_state else None
        try:
            async with session.delete(
                f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items/{item_id}",
                proxy=proxy,
            ) as response:
                self.stats["requests"] += 1
                if response.status == 407:
                    self.proxy_pool.report_failure(proxy_state, status=407)
                    raise aiohttp.ClientError("Proxy Authentication Required")
                if response.status < 500:
                    self.proxy_pool.report_success(proxy_state)
                else:
                    self.proxy_pool.report_failure(proxy_state, status=response.status)
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
            logger.error("Erro %s: %s", sku, exc)
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

        logger.info("Proxy pool: %s", self.proxy_pool.stats())

        async with aiohttp.ClientSession(headers=self.headers, timeout=timeout, connector=connector) as session:
            tasks = [self.check_stock(session, sem, sku) for sku in skus]
            results = await asyncio.gather(*tasks)
            for idx, stock in enumerate(results):
                stocks[idx] = stock

        logger.info("Proxy pool final: %s", self.proxy_pool.stats())
        return [int(stock or 0) for stock in stocks]


async def async_main() -> None:
    input_csv = get_env("INPUT_CSV")
    output_csv = get_env("OUTPUT_CSV")
    base_url = get_env("MAGENTO_BASE_URL")
    api_key = get_env("MAGENTO_API_KEY")
    proxies_file = os.getenv("PROXIES_FILE", "").strip()
    rate_limit = float(os.getenv("RATE_LIMIT", "0.1"))
    max_workers = int(os.getenv("MAX_WORKERS", "20"))
    max_stock = int(os.getenv("MAX_STOCK", "5000"))
    sock_connect_timeout = float(os.getenv("SOCK_CONNECT_TIMEOUT", "30"))
    sock_read_timeout = float(os.getenv("SOCK_READ_TIMEOUT", "180"))

    proxies = load_proxies(proxies_file) if proxies_file else []
    skus = read_products_from_csv(input_csv)
    checker = AsyncMagentoStockChecker(
        base_url=base_url,
        api_key=api_key,
        rate_limit=rate_limit,
        max_stock=max_stock,
        max_workers=max_workers,
        proxies=proxies,
    )
    stocks = await checker.process_all(skus, sock_connect_timeout, sock_read_timeout)
    write_stocks_to_csv(output_csv, skus, stocks)


def main() -> int:
    asyncio.run(async_main())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
