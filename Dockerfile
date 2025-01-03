FROM python:3.8-slim

COPY requirements.txt requirements.txt

# - Install requirements
# - Delete unnecessary Python files
# - Alias command `s3_upload` to `python3 s3_upload.py` for convenience
RUN \
    pip install --quiet --upgrade pip && \
    pip install -r requirements.txt && \

    echo "Delete python cache directories" 1>&2 && \
    find /usr/local/lib/python3.8 \( -iname '*.c' -o -iname '*.pxd' -o -iname '*.pyd' -o -iname '__pycache__' \) | \
    xargs rm -rf {} && \

    echo "Setting pandora alias" 1>&2 && \
    printf '#!/bin/sh\npython3 /app/pandora.py "$@"'  > /usr/local/bin/pandora && \
    chmod +x /usr/local/bin/pandora

COPY . /app

WORKDIR /app/pandora

# display help if no args specified
CMD pandora --help