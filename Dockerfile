FROM python:3.11

WORKDIR /app
COPY . .

ENV PYTHONUNBUFFERED=1

RUN pip install -r requirements.txt

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]



