# Playwright official image includes all OS deps + browsers
FROM mcr.microsoft.com/playwright/python:v1.50.0-jammy

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Browsers are already included in the base image, but keep this safe no-op if image changes
RUN playwright install chromium || true

COPY . .
CMD ["python", "bot.py"]
