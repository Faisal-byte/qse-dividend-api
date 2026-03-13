#!/usr/bin/env python3
"""QSE dividend yield scraper with FastAPI API and optional Excel export."""

from __future__ import annotations

import argparse
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Set

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


LISTED_COMPANIES_URLS = [
    "https://www.qe.com.qa/listed-companies",
    "https://www.qe.com.qa/en/listed-securities",
]

COMPANY_PROFILE_URL = (
    "https://www.qe.com.qa/web/guest/company-profile"
    "?InformationCategory=Company&InformationType=News&FromLocalSite=N&MoreNewsTitle=1"
    "&CompanyCode={code}"
)

NAME_SELECTOR = "#company-main-heading-companyName"
YIELD_SELECTOR = "#qeNLSYield"

TICKER_RE = re.compile(r"^[A-Z0-9]{3,6}$")

CACHE_TTL_SECONDS = 1800  # 30 minutes

_cache_payload: Optional[dict] = None
_cache_time: float = 0.0


@dataclass
class CompanyData:
    ticker: str
    name: str
    dividend_yield: Optional[str]


app = FastAPI(title="QSE Dividend Scraper API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _extract_tickers_from_links(page: Page) -> Set[str]:
    return set(
        page.evaluate(
            """
            () => {
              const links = Array.from(document.querySelectorAll('a[href*="CompanyCode="]'));
              const codes = links.map(a => {
                try {
                  const url = new URL(a.href, window.location.origin);
                  return url.searchParams.get('CompanyCode');
                } catch (_) {
                  return null;
                }
              }).filter(Boolean);
              return Array.from(new Set(codes));
            }
            """
        )
        or []
    )


def _extract_tickers_from_table(page: Page) -> Set[str]:
    return set(
        page.evaluate(
            """
            () => {
              const tables = Array.from(document.querySelectorAll('table'));
              const tickers = [];
              for (const table of tables) {
                const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.textContent.trim());
                const symbolIndex = headers.findIndex(h => h.toLowerCase() === 'symbol');
                if (symbolIndex === -1) continue;
                const rows = Array.from(table.querySelectorAll('tbody tr'));
                for (const row of rows) {
                  const cells = Array.from(row.querySelectorAll('td'));
                  if (cells[symbolIndex]) {
                    const text = cells[symbolIndex].textContent.trim();
                    if (text) tickers.push(text);
                  }
                }
              }
              return tickers;
            }
            """
        )
        or []
    )


def _filter_tickers(candidates: Iterable[str]) -> List[str]:
    clean: List[str] = []
    for value in candidates:
        if not value:
            continue
        text = value.strip().upper()
        if TICKER_RE.match(text):
            clean.append(text)
    return sorted(set(clean))


def get_all_tickers(page: Page, verbose: bool = False) -> List[str]:
    for url in LISTED_COMPANIES_URLS:
        if verbose:
            print(f"Loading listed companies page: {url}")

        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")

        tickers = _filter_tickers(_extract_tickers_from_links(page))
        if tickers:
            return tickers

        tickers = _filter_tickers(_extract_tickers_from_table(page))
        if tickers:
            return tickers

    return []


def _get_text_if_exists(page: Page, selector: str, timeout_ms: int) -> Optional[str]:
    try:
        locator = page.locator(selector)
        locator.wait_for(state="visible", timeout=timeout_ms)
        text = locator.first.inner_text().strip()
        return text or None
    except PlaywrightTimeoutError:
        return None


def fetch_company_data(
    page: Page,
    ticker: str,
    timeout_ms: int,
    verbose: bool = False,
) -> CompanyData:
    url = COMPANY_PROFILE_URL.format(code=ticker)

    if verbose:
        print(f"Fetching {ticker}: {url}")

    try:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
    except Exception:
        return CompanyData(ticker=ticker, name=ticker, dividend_yield=None)

    name = _get_text_if_exists(page, NAME_SELECTOR, timeout_ms) or ticker
    dividend_yield = _get_text_if_exists(page, YIELD_SELECTOR, timeout_ms)

    if dividend_yield is None:
        dividend_yield = page.evaluate(
            """
            () => {
              const label = Array.from(document.querySelectorAll('div, span, p, td, th'))
                .find(el => el.textContent && el.textContent.trim().toLowerCase() === 'dividend yield');
              if (!label) return null;
              const next = label.nextElementSibling;
              if (!next) return null;
              const text = next.textContent.trim();
              return text || null;
            }
            """
        )

    return CompanyData(ticker=ticker, name=name, dividend_yield=dividend_yield)


def _parse_dividend_yield(value: Optional[str]) -> Optional[float]:
    if not value:
        return None

    cleaned = (
        value.replace("%", "")
        .replace(",", "")
        .replace(" ", "")
        .strip()
    )

    try:
        return float(cleaned)
    except ValueError:
        return None


def scrape_companies(
    headless: bool = True,
    limit: Optional[int] = None,
    timeout_ms: int = 15000,
    verbose: bool = False,
) -> dict:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page.set_default_timeout(timeout_ms)
        page.set_default_navigation_timeout(timeout_ms)

        tickers = get_all_tickers(page, verbose=verbose)
        if not tickers:
            browser.close()
            raise RuntimeError(
                "Could not find any tickers on the listed companies page. "
                "Check if the QSE page structure changed."
            )

        if limit:
            tickers = tickers[:limit]

        results: List[CompanyData] = []

        for idx, ticker in enumerate(tickers, start=1):
            if verbose:
                print(f"({idx}/{len(tickers)}) {ticker}")

            try:
                data = fetch_company_data(page, ticker, timeout_ms, verbose=verbose)
                results.append(data)
            except Exception as exc:
                if verbose:
                    print(f"Failed to fetch {ticker}: {exc}")
                results.append(
                    CompanyData(
                        ticker=ticker,
                        name=ticker,
                        dividend_yield=None,
                    )
                )

            time.sleep(0.3)

        browser.close()

    companies = [
        {
            "company": item.name,
            "ticker": item.ticker,
            "dividendYield": _parse_dividend_yield(item.dividend_yield),
            "dividendYieldRaw": item.dividend_yield,
        }
        for item in results
    ]

    return {
        "companies": companies,
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "count": len(companies),
    }


def get_cached_or_fresh_scrape(
    limit: Optional[int],
    timeout_ms: int,
    force_refresh: bool = False,
) -> dict:
    global _cache_payload, _cache_time

    now = time.time()

    if (
        not force_refresh
        and limit is None
        and _cache_payload is not None
        and (now - _cache_time) < CACHE_TTL_SECONDS
    ):
        return _cache_payload

    payload = scrape_companies(
        headless=True,
        limit=limit,
        timeout_ms=timeout_ms,
        verbose=False,
    )

    if limit is None:
        _cache_payload = payload
        _cache_time = now

    return payload


def export_to_excel(
    output_path: str,
    headless: bool,
    limit: Optional[int],
    timeout_ms: int,
    verbose: bool,
) -> None:
    import pandas as pd

    payload = scrape_companies(
        headless=headless,
        limit=limit,
        timeout_ms=timeout_ms,
        verbose=verbose,
    )

    df = pd.DataFrame(
        [
            {
                "Company": item["company"],
                "Ticker": item["ticker"],
                "Dividend Yield": item["dividendYieldRaw"],
            }
            for item in payload["companies"]
        ]
    )

    df.to_excel(output_path, index=False)

    if verbose:
        print(f"Saved: {output_path}")

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/scrape")
def scrape_endpoint(
    limit: Optional[int] = Query(default=None, ge=1),
    timeout: int = Query(default=15000, ge=1000, le=60000),
    refresh: bool = Query(default=False),
) -> dict:
    try:
        return get_cached_or_fresh_scrape(
            limit=limit,
            timeout_ms=timeout,
            force_refresh=refresh,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch QSE company dividend yields to Excel.")
    parser.add_argument(
        "--output",
        default="dividend_yields.xlsx",
        help="Output Excel path (default: dividend_yields.xlsx)",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Run browser in headful mode for debugging.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of tickers for a test run.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=15000,
        help="Timeout in ms for page selectors.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging.",
    )
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    export_to_excel(
        output_path=args.output,
        headless=not args.headful,
        limit=args.limit,
        timeout_ms=args.timeout,
        verbose=args.verbose,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))