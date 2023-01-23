FROM ubuntu:jammy

LABEL maintainer="Texas Natural Resources Information Systems <tnris.twdb@gmail.com>"
LABEL org.opencontainers.image.source https://github.com/twdbben/twdhcli

# Set timezone
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Set Locale
ENV LC_ALL=en_US.UTF-8

# Directory for source code
ENV SRC_DIR=/srv/app/twdhcli
# Directory for virtual environment
ENV VENV=/twdhclienv

WORKDIR ${SRC_DIR}

# Set the locale          
RUN apt-get update
RUN apt-get install --no-install-recommends -y locales
RUN sed -i "/$LC_ALL/s/^# //g" /etc/locale.gen
RUN dpkg-reconfigure --frontend=noninteractive locales 
RUN update-locale LANG=${LC_ALL}

# Instal apt-utils
RUN apt-get install --no-install-recommends -y \
    apt-utils \
    python3-venv \
    python3-dev \
    python3 \
    build-essential \
    libxslt1-dev \
    libxml2-dev \
    libffi-dev \
    wget \
    curl \
    unzip \
    python3-pip \
    git

#python env setup; link python3 to python cmd; make venv; install uwsgi;
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN python -m venv ${VENV} && \
    git clone https://github.com/twdbben/twdhcli.git ${SRC_DIR} && \
    cd ${SRC_DIR} && \
    ${VENV}/bin/pip install -r requirements.txt

ENTRYPOINT ["tail", "-f", "/dev/null"]