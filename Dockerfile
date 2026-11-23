# Campus monitoring gateway: HTTP lease API + real public-port UDP forwarding.
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

# Run as the unprivileged user shipped with the slim image. Public demo ports
# (>=1024) need no capabilities; using a privileged port (<1024) would.
USER nobody

EXPOSE 8080/tcp 40000/udp

# Single worker on purpose: leases and UDP sockets live in one process. Scaling
# to more workers/processes would split the lease store and let two processes
# bind the same public port, so do NOT add --workers here. Horizontal scaling
# requires a shared lease store (and socket ownership) first.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
