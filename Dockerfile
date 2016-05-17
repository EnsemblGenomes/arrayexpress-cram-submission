FROM ubuntu:16.04
RUN apt-get -y update && apt-get install -y \
    python3 \
    python3-pip \
    git \
    libxml2-dev \
    libxslt-dev \
    zlib1g-dev
RUN git clone https://github.com/EnsemblGenomes/arrayexpress-cram-submission.git
WORKDIR arrayexpress-cram-submission
RUN pip3 install -r requirements.txt

ENV PYTHONPATH .
ENTRYPOINT ["luigi", "--local-scheduler", "--module", "pipeline"]