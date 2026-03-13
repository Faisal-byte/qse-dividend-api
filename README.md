# QSE Dividend Yield Fetcher

Simple Python fetcher that crawls the Qatar Stock Exchange (QSE) listed companies page, then visits each company profile page to extract the company name and dividend yield.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m playwright install chromium
```

## Run

```bash
python fetch_dividend_yields.py --verbose
```

If you see `No module named playwright`, verify the active interpreter:

```bash
which python
python -c "import playwright; print(playwright.__version__)"
```

### Options

- `--output <path>`: Excel output path (default `dividend_yields.xlsx`).
- `--headful`: Run browser with UI (debugging).
- `--limit <n>`: Limit number of tickers for a test run.
- `--timeout <ms>`: Selector timeout in milliseconds.
- `--verbose`: Verbose logging.

## Notes

- The script first discovers tickers from the listed companies page (tries multiple URLs).
- It looks for the dividend yield in `#qeNLSYield` and the company name in `#company-main-heading-companyName`.
- If the DOM changes, adjust selectors in `fetch_dividend_yields.py`.
