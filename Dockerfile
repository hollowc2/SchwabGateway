FROM python:3.12-slim AS build
WORKDIR /build
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY packages ./packages
RUN pip install --no-cache-dir build && python -m build --wheel && \
    python -m build --wheel packages/sdk && python -m build --wheel packages/token-store

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN groupadd --gid 1001 gateway && useradd --uid 1001 --gid gateway --no-create-home gateway
COPY --from=build /build/dist/*.whl /tmp/wheels/
COPY --from=build /build/packages/sdk/dist/*.whl /tmp/wheels/
COPY --from=build /build/packages/token-store/dist/*.whl /tmp/wheels/
RUN pip install --no-cache-dir /tmp/wheels/*.whl && rm -r /tmp/wheels
USER 1001:1001
ENTRYPOINT ["schwab-gateway"]
CMD ["--serve-live", "--authorize-real-credential-read", "--confirm-single-token-writer"]
