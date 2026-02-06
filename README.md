# Ricardo.ch Telegram Bot (Fixed price, private sellers, Name Surname)

## What it does
Command:
- `/ricardo <Имя Фамилия> [json|txt]`

The bot searches ricardo.ch and returns a file with items matching **ALL** rules:
- Seller must be **private** (best-effort heuristics)
- **Fixed price / Buy now** (no auctions / bids)
- Seller name must look like **"Name Surname"** (2+ words)
- Extracts: seller name, photo, ad link, price, title

## Setup
1) Create `.env` from `.env.example`
2) Put your token:
```
BOT_TOKEN=123:ABC...
OWNER_ID=7416000184
```
3) Install deps:
```
pip install -r requirements.txt
```
4) Run:
```
python bot.py
```

## Notes
- On hosting (Railway, etc.) set environment variables `BOT_TOKEN` and `OWNER_ID`.
- Bot runs in polling mode and clears webhook/pending updates on startup to avoid conflicts.
