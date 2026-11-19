# Slim Debian with Python 3.14: no gcloud SDK, R or bioinformatics tools
FROM python:3.14-slim-trixie

# security updates for the few OS packages present
RUN apt-get update && apt-get upgrade --yes && rm -rf /var/lib/apt/lists/*

# uv, pinned to the version used for development
COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /usr/local/bin/uv

# Install kanta package into /opt/kanta-venv, using the image's Python (no download)
ENV UV_PROJECT_ENVIRONMENT="/opt/kanta-venv" \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1
ADD pyproject.toml pyproject.toml
ADD README.md README.md
ADD uv.lock uv.lock
ADD src src
RUN uv sync --python 3.14 --locked --no-cache
ENV PATH="/opt/kanta-venv/bin:$PATH"

# Scripts engine.wdl and intake.wdl run, folder by folder (scripts/ also holds data files that must not go into the
# image); reference-table defaults come from kanta.config
ADD scripts/abnormality scripts/abnormality
ADD scripts/statements scripts/statements
ADD scripts/counts scripts/counts
ADD scripts/posneg scripts/posneg
ADD scripts/metabolomics scripts/metabolomics
ADD scripts/ref_ranges scripts/ref_ranges
ADD scripts/summaries/summaries.py scripts/summaries/summaries.py
# service-provider / region scripts, with their reference tables in scripts/sote/data (THL's sote register,
# municipality -> maakunta): code and public reference data only, about 9 MB
ADD scripts/sote scripts/sote

# python:*-slim defaults to a python prompt; a shell, as with the old bioinformatics base (the WDL tasks give their own command)
CMD ["bash"]
