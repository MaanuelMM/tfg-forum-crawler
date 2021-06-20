FROM python:3.8.10-alpine

WORKDIR /usr/src/app

COPY requirements.txt .

RUN apk add --no-cache tini && \
    pip install --no-cache-dir -r requirements.txt

COPY master.py .

ENTRYPOINT ["/sbin/tini", "--"]

CMD ["python", "./master.py"]