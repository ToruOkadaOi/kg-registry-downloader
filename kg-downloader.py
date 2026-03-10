#!/usr/bin/env python3
__author__ = "Aman Nalakath + GEMcon integration"
__description__ = "Auto-fetch KG Registry parquet (with cache), filter/dedupe Neo4j products, interactive download selection"

import os
import re
import json
import time
import asyncio
import logging
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
from aiohttp import ClientTimeout
import pyarrow.parquet as pq
import pandas as pd
from rich import print
from rich.console import Console
from rich.table import Table
from rich.progress import (
    Progress, BarColumn, TimeElapsedColumn,
    TimeRemainingColumn, DownloadColumn, TransferSpeedColumn
)

DEFAULT_PARQUET_URL = "https://kghub.org/kg-registry/registry/parquet/resources.parquet"
DEFAULT_CACHE_PATH = "data/data_raw/kg_registry/resources.parquet"
DEFAULT_OUT_DIR = "data/data_raw/KG_neo4j_downloads"
DEFAULT_RETRIES = 3
DEFAULT_CHUNK_SIZE = 1024 * 1024

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())

console = Console()
timeout = ClientTimeout(total=None)
headers = {"User-Agent": "Mozilla/5.0"}

def ensure_parent(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def safe_name(s: str) -> str:
    if not s:
        return "unknown"
    return "".join(c if c.isalnum() or c in ("-", "_", ".", " ") else "_" for c in s).strip()

def infer_filename(product: dict) -> str:
    base = safe_name(product.get("name") or product.get("id") or "")
    if not base:
        purl = product.get("product_url", "")
        base = safe_name(os.path.basename(urlparse(purl).path) or "download")
    fmt = (product.get("format") or "").lower()
    if "." not in base:
        if "dump" in fmt:
            base += ".dump"
        elif "tar" in fmt:
            base += ".tar.gz"
        elif "zip" in fmt:
            base += ".zip"
        elif "csv" in fmt:
            base += ".csv"
    return base

def parse_products_column(value):
    if pd.isna(value):
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return []
    return []

def human_size(n):
    if n is None or (isinstance(n, float) and pd.isna(n)):
        return "N/A"
    try:
        n = float(n)
        units = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while n >= 1024 and i < len(units)-1:
            n /= 1024.0
            i += 1
        return f"{n:.2f} {units[i]}"
    except Exception:
        return str(n)

async def download_file(url, filename, chunk_size=DEFAULT_CHUNK_SIZE, retries=DEFAULT_RETRIES):
    ensure_parent(filename)
    for i in range(retries):
        try:
            connector = aiohttp.TCPConnector(limit=10)
            async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        raise Exception(f"HTTP {response.status}")
                    total = response.content_length
                    with Progress(
                        "[progress.description]{task.description}",
                        DownloadColumn(),
                        BarColumn(),
                        TransferSpeedColumn(),
                        TimeRemainingColumn(),
                        TimeElapsedColumn(),
                    ) as progress:
                        task = progress.add_task(f"Downloading {os.path.basename(filename)}", total=total)
                        with open(filename, "wb") as f:
                            async for chunk in response.content.iter_chunked(chunk_size):
                                f.write(chunk)
                                progress.update(task, advance=len(chunk))
            print(f"[green]Download complete:[/green] {filename}")
            return filename
        except Exception as e:
            print(f"[yellow]Retry {i+1} failed:[/yellow] {e}")
            time.sleep(2 ** i)
    raise RuntimeError(f"Failed to download after {retries} retries: {url}")

async def ensure_cached_parquet(parquet_url: str, cache_path: str, force_refresh: bool = False):
    has_cache = os.path.exists(cache_path) and os.path.getsize(cache_path) > 0
    if has_cache and not force_refresh:
        print(f"[cyan]Using cached parquet:[/cyan] {cache_path}")
        return cache_path
    try:
        print(f"[blue]Fetching parquet:[/blue] {parquet_url}")
        await download_file(parquet_url, cache_path)
        return cache_path
    except Exception as e:
        if has_cache:
            print(f"[yellow]Fetch failed, using stale cache:[/yellow] {e}")
            return cache_path
        raise RuntimeError(f"No cache and download failed: {e}")

def extract_neo4j_products(df: pd.DataFrame):
    out = []
    for _, row in df.iterrows():
        products = parse_products_column(row.get("products"))
        if not isinstance(products, list):
            continue
        for product in products:
            pid = (product.get("id") or "").lower()
            pname = (product.get("name") or "").lower()
            pformat = (product.get("format") or "").lower()
            if ("neo4j" in pid) or ("neo4j" in pname) or ("neo4j" in pformat):
                purl = product.get("product_url")
                if not purl:
                    continue
                out.append({
                    "resource_name": row.get("name", "unknown_resource"),
                    "product_id": product.get("id"),
                    "product_name": product.get("name"),
                    "format": product.get("format"),
                    "file_size": product.get("product_file_size"),
                    "url": purl
                })
    return pd.DataFrame(out)

def apply_keyword_filter(df: pd.DataFrame, keyword_expr: str):
    if not keyword_expr.strip():
        return df
    # regex on resource_name + product_name + product_id
    pat = re.compile(keyword_expr, flags=re.IGNORECASE)
    mask = (
        df["resource_name"].fillna("").str.contains(pat)
        | df["product_name"].fillna("").str.contains(pat)
        | df["product_id"].fillna("").str.contains(pat)
    )
    return df[mask].copy()

def apply_url_required(df: pd.DataFrame):
    return df[df["url"].fillna("").str.len() > 0].copy()

def apply_dedupe(df: pd.DataFrame, mode: str):
    mode = (mode or "none").lower()
    if mode == "url":
        return df.drop_duplicates(subset=["url"], keep="first").copy()
    if mode == "product":
        return df.drop_duplicates(subset=["product_name", "product_id"], keep="first").copy()
    return df

def reindex_for_display(df: pd.DataFrame):
    df = df.reset_index(drop=True).copy()
    df["index"] = df.index
    return df

def print_products_table(df_products: pd.DataFrame, max_rows=300):
    table = Table(title="Neo4j Products (Filtered View)")
    table.add_column("Index", style="cyan", no_wrap=True)
    table.add_column("Resource", style="green")
    table.add_column("Product", style="magenta")
    table.add_column("Format", style="yellow")
    table.add_column("Size", style="blue")

    display_df = df_products.head(max_rows)
    for _, r in display_df.iterrows():
        table.add_row(
            str(r["index"]),
            str(r["resource_name"] or "N/A"),
            str(r["product_name"] or r["product_id"] or "N/A"),
            str(r["format"] or "N/A"),
            human_size(r["file_size"])
        )
    console.print(table)
    if len(df_products) > max_rows:
        console.print(f"[yellow]Showing first {max_rows}/{len(df_products)} rows.[/yellow]")

def parse_selection(selection: str, max_index: int):
    s = selection.strip().lower()
    if s == "all":
        return list(range(max_index + 1))
    chosen = set()
    parts = [p.strip() for p in s.split(",") if p.strip()]
    for part in parts:
        if re.match(r"^\d+$", part):
            idx = int(part)
            if 0 <= idx <= max_index:
                chosen.add(idx)
        elif re.match(r"^\d+\-\d+$", part):
            a, b = map(int, part.split("-"))
            if a > b:
                a, b = b, a
            for idx in range(a, b + 1):
                if 0 <= idx <= max_index:
                    chosen.add(idx)
    return sorted(chosen)

async def download_selected_products(df_products: pd.DataFrame, selected_indices, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    downloaded, failed = [], []
    for idx in selected_indices:
        row = df_products[df_products["index"] == idx]
        if row.empty:
            continue
        row = row.iloc[0]

        product = {
            "id": row["product_id"],
            "name": row["product_name"],
            "format": row["format"],
            "product_url": row["url"]
        }
        resource_name = safe_name(row["resource_name"] or "resource")
        filename = infer_filename(product)
        out_path = os.path.join(out_dir, f"{resource_name}__{filename}")
        url = row["url"]

        console.rule(f"[bold blue]Downloading index {idx}")
        print(f"Product: {row['product_name'] or row['product_id']}")
        print(f"URL: {url}")
        print(f"OUT: {out_path}")

        try:
            await download_file(url, out_path)
            downloaded.append(out_path)
        except Exception as e:
            logger.error(f"Failed index={idx} url={url} err={e}")
            failed.append((idx, str(e)))
    return downloaded, failed

async def main():
    parquet_url = os.getenv("KG_REGISTRY_PARQUET_URL", DEFAULT_PARQUET_URL).strip()
    cache_path = os.getenv("KG_REGISTRY_CACHE_PATH", DEFAULT_CACHE_PATH).strip()
    out_dir = os.getenv("KG_NEO4J_OUT_DIR", DEFAULT_OUT_DIR).strip()
    force_refresh = os.getenv("KG_REGISTRY_FORCE_REFRESH", "false").lower() in {"1", "true", "yes"}

    console.rule("[bold cyan]KG Registry Neo4j Downloader")
    print(f"Parquet URL: {parquet_url}")
    print(f"Cache path: {cache_path}")
    print(f"Output dir: {out_dir}")
    print(f"Force refresh: {force_refresh}")

    local_parquet = await ensure_cached_parquet(parquet_url, cache_path, force_refresh)

    print(f"[blue]Reading parquet:[/blue] {local_parquet}")
    table = pq.read_table(local_parquet)
    df = table.to_pandas()

    base_df = extract_neo4j_products(df)
    if base_df.empty:
        print("[yellow]No Neo4j products found.[/yellow]")
        return

    # interactive filter step
    console.rule("[bold magenta]Filter Options")
    print("Optional keyword regex examples:")
    print("  rna|microbe|monarch")
    print("  ^RNA-KG$")
    print("  RTX-KG2|UBKG")
    keyword_expr = input("\nKeyword filter (blank for no filter): ").strip()

    dedupe_mode = input("Dedupe mode [none/url/product] (default=url): ").strip().lower() or "url"
    require_url = input("Require non-empty URL? [Y/n]: ").strip().lower()
    require_url = False if require_url == "n" else True

    filtered = base_df.copy()
    filtered = apply_keyword_filter(filtered, keyword_expr)
    if require_url:
        filtered = apply_url_required(filtered)
    filtered = apply_dedupe(filtered, dedupe_mode)
    filtered = reindex_for_display(filtered)

    if filtered.empty:
        print("[yellow]No rows matched filter settings.[/yellow]")
        return

    # save filtered metadata
    os.makedirs(out_dir, exist_ok=True)
    meta_csv = os.path.join(out_dir, "neo4j_products_filtered_metadata.csv")
    filtered.to_csv(meta_csv, index=False)
    print(f"[cyan]Saved filtered metadata:[/cyan] {meta_csv}")
    print(f"[green]Filtered rows:[/green] {len(filtered)}")

    # show table + selection
    print_products_table(filtered)

    print("\nEnter selection:")
    print("  - single index: 3")
    print("  - comma list: 1,4,7")
    print("  - range: 2-8")
    print("  - mixed: 1,3-5,10")
    print("  - all: all")
    user_sel = input("\nWhich file(s) to download? ").strip()
    selected = parse_selection(user_sel, max_index=int(filtered["index"].max()))
    if not selected:
        print("[yellow]No valid selection made. Exiting.[/yellow]")
        return

    downloaded, failed = await download_selected_products(filtered, selected, out_dir)

    console.rule("[bold green]Summary")
    print(f"[green]Downloaded:[/green] {len(downloaded)}")
    for p in downloaded:
        print(f"  - {p}")

    if failed:
        print(f"[red]Failed:[/red] {len(failed)}")
        for idx, err in failed:
            print(f"  - index {idx}: {err}")

    last_file = "data/data_raw/_last_downloaded_kg_neo4j.txt"
    ensure_parent(last_file)
    with open(last_file, "w") as f:
        for p in downloaded:
            f.write(p + "\n")

if __name__ == "__main__":
    asyncio.run(main())