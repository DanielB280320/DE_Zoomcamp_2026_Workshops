FROM python:3.13
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY .python-version pyproject.toml uv.lock ./

RUN uv sync --locked

COPY IngestPipelineWorkshop.py .

ENTRYPOINT ["python", "IngestPipelineWorkshop.py"]