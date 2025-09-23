# Dockerfile pour namenode avec Python 3
FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

USER root

# Corrige les sources pour utiliser les archives Debian et supprime stretch-updates
RUN sed -i '/stretch-updates/d' /etc/apt/sources.list \
    && sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list \
    && sed -i 's|http://security.debian.org/debian-security|http://archive.debian.org/debian-security|g' /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y wget build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev curl libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev \
    && cd /tmp \
    && wget https://www.python.org/ftp/python/3.7.18/Python-3.7.18.tgz \
    && tar xzf Python-3.7.18.tgz \
    && cd Python-3.7.18 \
    && ./configure --enable-optimizations \
    && make -j$(nproc) \
    && make altinstall \
    && ln -sf /usr/local/bin/python3.7 /usr/bin/python \
    && ln -sf /usr/local/bin/pip3.7 /usr/bin/pip \
    && cd / \
    && rm -rf /tmp/Python-3.7.18* \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Vérifie que l'utilisateur hdfs existe toujours, puis corrige les droits et repasse en USER hdfs
RUN id hdfs || useradd -m hdfs \
    && chown -R hdfs:hdfs /etc/hadoop /opt/hadoop-3.2.1 /hadoop
USER hdfs
