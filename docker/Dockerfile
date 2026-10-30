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

# Scripts engine.wdl runs, folder by folder (scripts/ also holds data files that must not go into the image);
# reference-table defaults come from kanta.config
ADD scripts/abnormality scripts/abnormality
ADD scripts/counts scripts/counts
ADD scripts/posneg scripts/posneg
ADD scripts/metabolomics scripts/metabolomics
ADD scripts/ref_ranges scripts/ref_ranges
ADD scripts/summaries/summaries.py scripts/summaries/summaries.py
