FROM python:3.13.16-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY athena.py tools.py server.py test_tools.py ./

USER nobody
# stdio transport: the MCP client starts this container and talks over stdin/stdout.
CMD ["python", "server.py"]
