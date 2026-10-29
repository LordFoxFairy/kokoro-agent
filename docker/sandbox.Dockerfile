# Task executor only: no worker source, application dependencies or credentials.
# Keep the immutable multi-architecture base aligned with ../Dockerfile.
FROM python:3.11-slim@sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534

# Fail the build if the pinned base lacks the execution or TLS prerequisites.
# No package downloads and no network request are needed for these checks.
RUN command -v sh && command -v sleep \
    && python -c "import ssl, urllib.request; assert ssl.create_default_context().cert_store_stats()['x509_ca'] > 0"

ENV HOME=/tmp \
    TMPDIR=/tmp \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /workspace

# Build with the host Run-directory owner's numeric UID:GID.
# The bind mount, not the image layer, supplies the writable workspace.
ARG SANDBOX_UID=65532
ARG SANDBOX_GID=65532
USER ${SANDBOX_UID}:${SANDBOX_GID}
ENTRYPOINT []
CMD ["sleep", "1800"]
