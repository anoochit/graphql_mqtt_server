FROM python:3.13-slim

# Install uv using the official installer
RUN apt-get update && apt-get install -y curl \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Add uv to PATH (installs to ~/.local/bin by default)
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

# Copy project files first for dependency install
COPY pyproject.toml .

# Install dependencies using uv
RUN uv pip install --system .

# Copy the rest of the application
COPY . .

EXPOSE 8000

CMD ["python", "main.py"]
