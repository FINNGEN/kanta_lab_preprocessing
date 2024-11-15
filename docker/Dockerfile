FROM eu.gcr.io/finngen-refinery-dev/bioinformatics:0.8

RUN apt-get update && apt-get upgrade --yes
ADD finngen_qc/requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
ADD finngen_qc finngen_qc