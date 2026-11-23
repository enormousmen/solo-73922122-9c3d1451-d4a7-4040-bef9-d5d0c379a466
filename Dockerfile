# Single-instance image for the campus gateway port allocation API.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv/app

# Install dependencies first so application edits don't invalidate the layer.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Run as the unprivileged user shipped with the slim image.
USER nobody

EXPOSE 8080

# Single worker on purpose: leases live in process memory. Scaling to more
# workers/processes would silently split the lease store, so do NOT add
# --workers here. Horizontal scaling requires a shared store instead.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
