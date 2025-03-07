FROM eu.gcr.io/finngen-refinery-dev/bioinformatics:0.8

RUN apt-get update && apt-get upgrade --yes
RUN curl https://clickhouse.com/ | sh && ./clickhouse install
ADD finngen_qc/requirements.txt requirements.txt
RUN pip3 install --upgrade scipy
RUN  pip3 install -r requirements.txt

ADD finngen_qc finngen_qc
ADD analysis analysis
ADD sb_release sb_release