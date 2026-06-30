FROM eu.gcr.io/finngen-refinery-dev/bioinformatics:0.8

RUN apt-get update && apt-get upgrade --yes
RUN curl https://clickhouse.com/ | sh && ./clickhouse install
RUN apt install pigz

# Install kanta package
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"
RUN uv python install 3.12
ADD pyproject.toml pyproject.toml
ADD README.md README.md
ADD uv.lock uv.lock
ADD src src
RUN uv venv --python 3.12 /opt/kanta-venv && uv pip install --python /opt/kanta-venv/bin/python .
ENV PATH="/opt/kanta-venv/bin:$PATH"

