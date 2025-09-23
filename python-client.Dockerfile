# Dockerfile pour python-client
FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y openjdk-21-jre wget curl procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Installer le client Hadoop
RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-3.2.1/hadoop-3.2.1.tar.gz -O /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt/ \
    && mv /opt/hadoop-3.2.1 /opt/hadoop \
    && rm /tmp/hadoop.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV PATH="$PATH:/opt/hadoop/bin:/opt/hadoop/sbin"
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /workspace

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
