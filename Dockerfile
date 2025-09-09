FROM python:3.12-slim

COPY requirements.txt requirements.txt

RUN \
    pip install --quiet --upgrade pip && \
    pip install uv && \
    uv pip install --system --only-binary=all -r requirements.txt && \
    # Clean up unnecessary files
    echo "Delete python cache directories" 1>&2 && \
    find /usr/local/lib/python3.12 \( -iname '*.c' -o -iname '*.pxd' -o -iname '*.pyd' -o -iname '__pycache__' \) -print0 | \
    xargs -0 -r rm -rf && \
    # Create pandora alias
    echo "Setting pandora alias" 1>&2 && \
    printf '#!/bin/sh\nexec python3 /app/pandora.py "$@"\n' > /usr/local/bin/pandora && \
    chmod +x /usr/local/bin/pandora

COPY . /app

WORKDIR /app

CMD pandora --help
