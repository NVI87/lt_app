FROM python:3.11-slim

WORKDIR /app

COPY dist/lt_app-*.whl /tmp/
RUN python -m pip install --no-cache-dir /tmp/lt_app-*.whl \
    && rm -f /tmp/*.whl

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')" || exit 1

CMD ["sh", "-c", \
     "streamlit run \"$(python -c 'import lt_app.app as m; print(m.__file__)')\" \
      --server.address=0.0.0.0 --server.port=8501 --server.headless=true"]