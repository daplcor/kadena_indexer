# Use the official Python image from the Docker Hub
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Install dependencies required for building Rust extensions
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Rust and Cargo
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Copy the requirements file into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY kadena_indexer/ ./kadena_indexer

# Copy the default config file into the container
COPY config.yaml /app/config.yaml

# Copy the setup.py and install the package
COPY setup.py .
RUN pip install .

# Set the entrypoint to run the Python module
ENTRYPOINT ["python", "-m", "kadena_indexer"]

# Provide the default argument (the config file)
CMD ["/app/config.yaml"]


# Build with docker buildx build --platform linux/amd64,linux/arm64,linux/arm/v7 -t daplco/kadena_indexer:latest --push .
