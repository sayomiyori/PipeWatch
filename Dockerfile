FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd --system --uid 10001 pipewatch
USER pipewatch

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

