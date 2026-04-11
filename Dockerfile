FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/instance/uploads

EXPOSE 9003

CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:9003", "app:app"]
