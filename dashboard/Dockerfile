FROM python:3.13-slim
WORKDIR /app/dashboard
COPY server.py index.html styles.css app.js admin.html admin.js ./
USER 10001:10001
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 WT4J_ENV=production \
    WT4J_BIND_HOST=0.0.0.0 WT4J_ADMIN_PORT=8085 \
    WT4J_TLS_CERT_FILE=/run/tls/tls.crt WT4J_TLS_KEY_FILE=/run/tls/tls.key
EXPOSE 8085
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import os, ssl, urllib.request; c=ssl.create_default_context(cafile=os.environ['WT4J_TLS_CERT_FILE']); c.check_hostname=False; urllib.request.urlopen('https://127.0.0.1:8085/healthz', context=c, timeout=3).read()"
CMD ["python", "server.py"]
