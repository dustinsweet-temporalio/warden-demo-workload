# Single image for the whole workload. Each compose service overrides the
# command to run one worker, the generator, or the dashboard.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default command; compose overrides per service.
CMD ["python", "-m", "dashboard.app"]
