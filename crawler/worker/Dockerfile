FROM python:3.8.10-alpine

WORKDIR /usr/src/app

COPY requirements.txt .

RUN apk add --no-cache tini && \
    apk add --no-cache --virtual .build-deps gcc libc-dev libxslt-dev && \
    apk add --no-cache libxslt && \
    pip install --no-cache-dir -r requirements.txt && \
    apk del .build-deps

COPY worker.py .

ENTRYPOINT ["/sbin/tini", "--"]

CMD ["python", "./worker.py"]