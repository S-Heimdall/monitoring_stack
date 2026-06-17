FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY app ./app
COPY tqg ./tqg

RUN pip install --no-cache-dir .

EXPOSE 8080
ARG APP_MODULE=app.main:app
ENV APP_MODULE=${APP_MODULE}
CMD ["sh", "-c", "uvicorn ${APP_MODULE} --host 0.0.0.0 --port 8080"]
