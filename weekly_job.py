#!/usr/bin/env python3
"""
Atualiza o catalogo diretamente no PostgreSQL a partir das URLs ja cadastradas.
"""

import os
import sys
import time
from datetime import datetime
from typing import Dict, Iterable, List
from urllib.parse import urlparse

import psycopg
import requests


GRAPHQL_ENDPOINT = "https://www.bringit.com.br/graphql"

QUERY_PRODUCTS_BY_URL_KEY = """
query($keys: [String!]!) {
  products(filter: { url_key: { in: $keys } }) {
    items {
      __typename
      url_key
      sku
      name
      stock_status
      price_range {
        minimum_price {
          regular_price {
            value
          }
        }
      }
      ... on ConfigurableProduct {
        variants {
          product {
            sku
            stock_status
          }
        }
      }
    }
  }
}
"""


def env(name: str, default: str = "", required: bool = False) -> str:
    value = os.getenv(name, default).strip()
    if required and not value:
        raise RuntimeError(f"Variavel obrigatoria ausente: {name}")
    return value


def today_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def utc_now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


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
        cur.execute("DROP VIEW IF EXISTS current_stock")
        cur.execute(
            """
            CREATE VIEW current_stock AS
            SELECT
                p.codigo_completo,
                p.codigo_simples,
                p.url,
                p.product_name,
                p.preco AS preco_catalogo,
                p.sku_filho_para_saldo,
                p.variant_skus,
                ds.data_consulta,
                ds.saldo,
                ds.preco AS preco_diario
            FROM products p
            LEFT JOIN daily_stock ds
              ON ds.codigo_completo = p.codigo_completo
             AND ds.data_consulta = (
                 SELECT MAX(ds2.data_consulta)
                 FROM daily_stock ds2
                 WHERE ds2.codigo_completo = p.codigo_completo
             )
            """
        )
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


def extract_url_key(url_value: str) -> str:
    raw = str(url_value or "").strip()
    if not raw:
        return ""
    if not raw.startswith("http"):
        raw = "https://" + raw.lstrip("/")
    parsed = urlparse(raw)
    path = (parsed.path or "").strip("/")
    if path.endswith(".html"):
        path = path[:-5]
    return path


def chunked(values: List[str], size: int) -> Iterable[List[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def choose_sku_for_scrape(item: Dict) -> str:
    product_type = str(item.get("__typename") or "")
    if product_type == "SimpleProduct":
        return str(item.get("sku") or "").strip()

    fallback: List[str] = []
    for variant in item.get("variants") or []:
        product = variant.get("product") or {}
        sku = str(product.get("sku") or "").strip()
        if not sku:
            continue
        fallback.append(sku)
        if str(product.get("stock_status") or "").strip() == "IN_STOCK":
            return sku
    return fallback[0] if fallback else ""


def get_regular_min_price(item: Dict) -> float:
    try:
        return float(item["price_range"]["minimum_price"]["regular_price"]["value"] or 0)
    except Exception:
        return 0.0


def variant_skus_text(item: Dict) -> str:
    values: List[str] = []
    for variant in item.get("variants") or []:
        product = variant.get("product") or {}
        sku = str(product.get("sku") or "").strip()
        if sku and sku not in values:
            values.append(sku)
    return "|".join(values)


def graphql_fetch_by_url_keys(url_keys: List[str], batch_size: int, timeout: int) -> Dict[str, Dict]:
    by_key: Dict[str, Dict] = {}
    session = requests.Session()
    total_batches = max(1, (len(url_keys) + batch_size - 1) // batch_size)

    for idx, batch in enumerate(chunked(url_keys, batch_size), start=1):
        print(f"[INFO] GraphQL lote {idx}/{total_batches} com {len(batch)} urls")
        payload = {"query": QUERY_PRODUCTS_BY_URL_KEY, "variables": {"keys": batch}}
        for attempt in range(1, 5):
            try:
                response = session.post(GRAPHQL_ENDPOINT, json=payload, timeout=timeout)
                response.raise_for_status()
                body = response.json()
                items = body.get("data", {}).get("products", {}).get("items", [])
                for item in items:
                    key = str(item.get("url_key") or "").strip()
                    if key:
                        by_key[key] = item
                break
            except Exception as exc:
                if attempt == 4:
                    print(f"[ERRO] lote {idx} falhou: {exc}", file=sys.stderr)
                else:
                    time.sleep(min(2 * attempt, 6))
    return by_key


def load_active_products(conn: psycopg.Connection) -> List[Dict[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT codigo_completo, codigo_simples, url, product_name
            FROM products
            WHERE is_active = TRUE AND COALESCE(url, '') <> ''
            ORDER BY codigo_completo
            """
        )
        rows = cur.fetchall()
    return [
        {
            "codigo_completo": row[0],
            "codigo_simples": row[1],
            "url": row[2],
            "product_name": row[3],
        }
        for row in rows
    ]


def main() -> int:
    run_date = env("RUN_DATE", today_iso())
    batch_size = int(env("BATCH_SIZE", "50"))
    timeout = int(env("HTTP_TIMEOUT", "30"))

    conn = build_pg_conn()
    ensure_schema(conn)
    run_id = begin_run(conn, "weekly_catalog_refresh", run_date)

    products = load_active_products(conn)
    url_keys = sorted({extract_url_key(product["url"]) for product in products if extract_url_key(product["url"])})
    by_key = graphql_fetch_by_url_keys(url_keys, batch_size=batch_size, timeout=timeout)

    updated = 0
    deactivated = 0
    now = utc_now_iso()

    with conn.cursor() as cur:
        for product in products:
            url_key = extract_url_key(product["url"])
            item = by_key.get(url_key)
            if not item:
                cur.execute(
                    """
                    UPDATE products
                    SET is_active = FALSE,
                        updated_at = %s
                    WHERE codigo_completo = %s
                    """,
                    (now, product["codigo_completo"]),
                )
                deactivated += 1
                continue

            cur.execute(
                """
                UPDATE products
                SET product_name = %s,
                    preco = %s,
                    sku_filho_para_saldo = %s,
                    variant_skus = %s,
                    last_seen_on = %s,
                    updated_at = %s,
                    is_active = TRUE
                WHERE codigo_completo = %s
                """,
                (
                    str(item.get("name") or product["product_name"]).strip(),
                    get_regular_min_price(item),
                    choose_sku_for_scrape(item),
                    variant_skus_text(item),
                    run_date,
                    now,
                    product["codigo_completo"],
                ),
            )
            updated += 1

    conn.commit()
    finish_run(
        conn,
        run_id,
        rows_read=len(products),
        rows_inserted=0,
        rows_updated=updated,
        rows_deactivated=deactivated,
        notes=f"urls={len(url_keys)} graphql_hits={len(by_key)}",
    )
    print(
        f"[OK] weekly_job products={len(products)} updated={updated} "
        f"deactivated={deactivated} graphql_hits={len(by_key)}"
    )
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
