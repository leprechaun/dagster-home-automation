FROM python:3.14


# This makes sure that logs show up immediately instead of being buffered
ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip
RUN pip install --upgrade uv

# Copy your Dagster project. You may need to replace the filepath depending on your project structure
WORKDIR /deploy_k8s/

COPY pyproject.toml uv.lock ./
RUN uv pip install --system -r pyproject.toml

COPY . /deploy_k8s/
RUN uv pip install --system -e .

# Expose the port that your Dagster instance will run on
EXPOSE 80
