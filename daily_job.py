#!/usr/bin/env python3
"""
Atualiza saldo diario diretamente no PostgreSQL com fallback por variantes.
"""

import asyncio
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

import psycopg

from main import AsyncMagentoStockChecker, load_proxies


def env(name: str, default: str = "", required: bool = False) -> str:
    value = os.getenv(name, default).strip()
    if required and not value:
        raise RuntimeError(f"Variavel obrigatoria ausente: {name}")
    return value


def today_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def utc_now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def unique_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        raw = str(value or "").strip()
        if not raw or raw in seen:
            continue
        seen.add(raw)
        result.append(raw)
    return result


def build_pg_conn() -> psycopg.Connection:
    return psycopg.connect(
        host=env("PGHOST", required=True),
        port=int(env("PGPORT", "5432")),
        user=env("PGUSER", required=True),
        password=env("PGPASSWORD", required=True),
        dbname=env("PGDATABASE", required=True),
    )


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                codigo_completo TEXT PRIMARY KEY,
                codigo_simples TEXT NOT NULL,
                url TEXT NOT NULL,
                product_name TEXT NOT NULL,
                preco DOUBLE PRECISION NOT NULL DEFAULT 0,
                sku_filho_para_saldo TEXT,
                variant_skus TEXT,
                first_seen_on DATE NOT NULL,
                last_seen_on DATE NOT NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_stock (
                codigo_completo TEXT NOT NULL REFERENCES products(codigo_completo),
                data_consulta DATE NOT NULL,
                saldo INTEGER NOT NULL DEFAULT 0,
                preco DOUBLE PRECISION NOT NULL DEFAULT 0,
                sku_filho TEXT,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                PRIMARY KEY (codigo_completo, data_consulta)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_runs (
                id BIGINT PRIMARY KEY,
                run_type TEXT NOT NULL,
                run_date DATE NOT NULL,
                started_at TIMESTAMP NOT NULL,
                finished_at TIMESTAMP,
                rows_read INTEGER NOT NULL DEFAULT 0,
                rows_inserted INTEGER NOT NULL DEFAULT 0,
                rows_updated INTEGER NOT NULL DEFAULT 0,
                rows_deactivated INTEGER NOT NULL DEFAULT 0,
                notes TEXT
            )
            """
        )
        cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS variant_skus TEXT")
    conn.commit()


def begin_run(conn: psycopg.Connection, run_type: str, run_date: str) -> int:
    run_id = int(time.time() * 1000)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sync_runs (id, run_type, run_date, started_at)
            VALUES (%s, %s, %s, %s)
            """,
            (run_id, run_type, run_date, utc_now_iso()),
        )
    conn.commit()
    return run_id


def finish_run(
    conn: psycopg.Connection,
    run_id: int,
    rows_read: int,
    rows_inserted: int,
    rows_updated: int,
    rows_deactivated: int,
    notes: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sync_runs
            SET finished_at = %s,
                rows_read = %s,
                rows_inserted = %s,
                rows_updated = %s,
                rows_deactivated = %s,
                notes = %s
            WHERE id = %s
            """,
            (utc_now_iso(), rows_read, rows_inserted, rows_updated, rows_deactivated, notes, run_id),
        )
    conn.commit()


def load_products(conn: psycopg.Connection) -> List[Dict[str, object]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                codigo_completo,
                codigo_simples,
                url,
                product_name,
                preco,
                COALESCE(sku_filho_para_saldo, ''),
                COALESCE(variant_skus, '')
            FROM products
            WHERE is_active = TRUE
            ORDER BY codigo_simples, codigo_completo
            """
        )
        rows = cur.fetchall()

    items: List[Dict[str, object]] = []
    for row in rows:
        primary_sku = str(row[5] or "").strip()
        variant_skus = unique_preserve_order(str(row[6] or "").split("|"))
        items.append(
            {
                "codigo_completo": row[0],
                "codigo_simples": row[1],
                "url": row[2],
                "product_name": row[3],
                "preco": float(row[4] or 0),
                "primary_sku": primary_sku,
                "candidate_skus": unique_preserve_order([primary_sku] + variant_skus),
                "saldo": 0,
                "sku_escolhido": primary_sku,
                "saldo_source": "not_tried",
            }
        )
    return items


async def fetch_stock_pairs(
    sku_candidates: List[List[str]],
    base_url: str,
    api_key: str,
    proxies_file: str,
    rate_limit: float,
    max_workers: int,
    max_stock: int,
    sock_connect_timeout: float,
    sock_read_timeout: float,
) -> Dict[str, int]:
    flat_skus = unique_preserve_order([sku for group in sku_candidates for sku in group])
    if not flat_skus:
        return {}

    proxies = load_proxies(proxies_file) if proxies_file else []
    checker = AsyncMagentoStockChecker(
        base_url=base_url,
        api_key=api_key,
        rate_limit=rate_limit,
        max_stock=max_stock,
        max_workers=max_workers,
        proxies=proxies,
    )
    stocks = await checker.process_all(flat_skus, sock_connect_timeout, sock_read_timeout)
    return dict(zip(flat_skus, stocks))


def group_by_simple_code(items: List[Dict[str, object]]) -> Dict[str, List[Dict[str, object]]]:
    groups: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for item in items:
        groups[str(item["codigo_simples"])].append(item)
    return groups


def upsert_daily_stock(conn: psycopg.Connection, items: List[Dict[str, object]], run_date: str) -> Dict[str, int]:
    inserted = 0
    updated = 0
    now = utc_now_iso()

    with conn.cursor() as cur:
        for item in items:
            cur.execute(
                """
                SELECT 1
                FROM daily_stock
                WHERE codigo_completo = %s AND data_consulta = %s
                """,
                (str(item["codigo_completo"]).strip(), run_date),
            )
            exists = cur.fetchone() is not None

            cur.execute(
                """
                INSERT INTO daily_stock (
                    codigo_completo, data_consulta, saldo, preco, sku_filho, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (codigo_completo, data_consulta) DO UPDATE SET
                    saldo = EXCLUDED.saldo,
                    preco = EXCLUDED.preco,
                    sku_filho = EXCLUDED.sku_filho,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    str(item["codigo_completo"]).strip(),
                    run_date,
                    int(item["saldo"]),
                    float(item["preco"]),
                    str(item["sku_escolhido"]).strip(),
                    now,
                    now,
                ),
            )
            if exists:
                updated += 1
            else:
                inserted += 1

            chosen_sku = str(item["sku_escolhido"]).strip()
            if chosen_sku:
                cur.execute(
                    """
                    UPDATE products
                    SET sku_filho_para_saldo = %s,
                        updated_at = %s
                    WHERE codigo_completo = %s
                    """,
                    (chosen_sku, now, str(item["codigo_completo"]).strip()),
                )

    conn.commit()
    return {"inserted": inserted, "updated": updated}


async def run() -> int:
    run_date = env("RUN_DATE", today_iso())
    base_url = env("MAGENTO_BASE_URL", "https://www.bringit.com.br")
    api_key = env("MAGENTO_API_KEY", required=True)
    proxies_file = env("PROXIES_FILE")
    rate_limit = float(env("RATE_LIMIT", "0.02"))
    max_workers = int(env("MAX_WORKERS", "80"))
    max_stock = int(env("MAX_STOCK", "5000"))
    sock_connect_timeout = float(env("SOCK_CONNECT_TIMEOUT", "30"))
    sock_read_timeout = float(env("SOCK_READ_TIMEOUT", "180"))

    conn = build_pg_conn()
    ensure_schema(conn)
    run_id = begin_run(conn, "daily_stock_refresh", run_date)
    items = load_products(conn)
    groups = group_by_simple_code(items)

    primary_candidates: List[List[str]] = []
    for group_items in groups.values():
        probe_skus = [str(item["primary_sku"]).strip() for item in group_items if str(item["primary_sku"]).strip()]
        if probe_skus:
            primary_candidates.append(probe_skus)

    primary_stock_map = await fetch_stock_pairs(
        primary_candidates,
        base_url,
        api_key,
        proxies_file,
        rate_limit,
        max_workers,
        max_stock,
        sock_connect_timeout,
        sock_read_timeout,
    )

    replicated_primary = 0
    fallback_candidates: List[List[str]] = []

    for group_items in groups.values():
        positive_item = None
        for item in group_items:
            primary_sku = str(item["primary_sku"]).strip()
            if not primary_sku:
                item["saldo_source"] = "missing_primary_sku"
                continue
            saldo = int(primary_stock_map.get(primary_sku, 0))
            item["saldo"] = saldo
            item["sku_escolhido"] = primary_sku
            item["saldo_source"] = "primary_sku" if saldo > 0 else "primary_sku_zero"
            if saldo > 0:
                positive_item = item
                break

        if positive_item is not None:
            source_saldo = int(positive_item["saldo"])
            source_sku = str(positive_item["sku_escolhido"])
            for item in group_items:
                if item is positive_item:
                    continue
                item["saldo"] = source_saldo
                item["sku_escolhido"] = source_sku
                item["saldo_source"] = "replicated_primary_group_hit"
                replicated_primary += 1
            continue

        for item in group_items:
            candidates = [sku for sku in list(item["candidate_skus"]) if sku]
            if candidates:
                fallback_candidates.append(candidates)

    fallback_stock_map = await fetch_stock_pairs(
        fallback_candidates,
        base_url,
        api_key,
        proxies_file,
        rate_limit,
        max_workers,
        max_stock,
        sock_connect_timeout,
        sock_read_timeout,
    )

    fallback_hits = 0
    replicated_fallback = 0

    for group_items in groups.values():
        if any(int(item["saldo"]) > 0 for item in group_items):
            continue

        positive_item = None
        for item in group_items:
            best_sku = ""
            best_saldo = 0
            for sku in [sku for sku in list(item["candidate_skus"]) if sku]:
                saldo = int(fallback_stock_map.get(sku, 0))
                if saldo > best_saldo:
                    best_saldo = saldo
                    best_sku = sku
            if best_saldo > 0:
                item["saldo"] = best_saldo
                item["sku_escolhido"] = best_sku
                item["saldo_source"] = "fallback_variant"
                positive_item = item
                fallback_hits += 1
                break

        if positive_item is not None:
            source_saldo = int(positive_item["saldo"])
            source_sku = str(positive_item["sku_escolhido"])
            for item in group_items:
                if item is positive_item:
                    continue
                item["saldo"] = source_saldo
                item["sku_escolhido"] = source_sku
                item["saldo_source"] = "replicated_fallback_group_hit"
                replicated_fallback += 1

    counts = upsert_daily_stock(conn, items, run_date)
    finish_run(
        conn,
        run_id,
        rows_read=len(items),
        rows_inserted=counts["inserted"],
        rows_updated=counts["updated"],
        rows_deactivated=0,
        notes=(
            f"positivos={sum(1 for item in items if int(item['saldo']) > 0)} "
            f"replicated_primary={replicated_primary} fallback_hits={fallback_hits} "
            f"replicated_fallback={replicated_fallback}"
        ),
    )
    print(
        f"[OK] daily_job itens={len(items)} inserted={counts['inserted']} updated={counts['updated']} "
        f"positivos={sum(1 for item in items if int(item['saldo']) > 0)} "
        f"replicated_primary={replicated_primary} fallback_hits={fallback_hits} "
        f"replicated_fallback={replicated_fallback}"
    )
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
