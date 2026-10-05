FROM eu.gcr.io/finngen-refinery-dev/bioinformatics:0.8

RUN apt-get update && apt-get upgrade --yes
RUN curl https://clickhouse.com/ | sh && ./clickhouse install
RUN apt install pigz

# Install kanta package
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"
RUN uv python install 3.14
ADD pyproject.toml pyproject.toml
ADD README.md README.md
ADD uv.lock uv.lock
ADD src src
ENV UV_PROJECT_ENVIRONMENT="/opt/kanta-venv"
RUN uv sync --python 3.14 --locked
ENV PATH="/opt/kanta-venv/bin:$PATH"

# QC scripts, kept at scripts/qc_scripts so their relative defaults resolve to /src/kanta/engine/data
ADD scripts/qc_scripts scripts/qc_scripts

# The engine reads injection_results.tsv from <repo root>/scripts/injection/data/ (config.
# INJECTION_RESULTS_FILE), refreshing it from GitHub into the same folder; without this the
# refresh can't write there and there's no local fallback, so the engine crashes at startup.
ADD scripts/injection/data/injection_results.tsv scripts/injection/data/injection_results.tsv
