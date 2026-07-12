FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY alembic.ini ./
COPY migrations ./migrations
COPY configs ./configs
RUN pip install --no-cache-dir .
USER 65532:65532
ENTRYPOINT ["app"]
CMD ["--help"]
