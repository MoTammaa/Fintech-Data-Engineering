FROM python:3.11-slim

# Install PostgreSQL development packages
RUN apt-get update && \
    apt-get install -y libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app


# Copy only the requirements file first to leverage Docker cache
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy the rest of the application code
# COPY . /app

EXPOSE 8050:8050

# ENTRYPOINT ["python", "src/functions.py"]
ENTRYPOINT ["python", "src/fintech_dashboard.py"]