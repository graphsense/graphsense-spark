FROM openjdk:8

ARG UID=10000
ADD requirements.txt /tmp/requirements.txt

#ARG SPARK_DRIVER_PORT=0
ARG SPARK_UI_PORT=8080
#ARG SPARK_BLOCKMGR_PORT=0

RUN apt-get update && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install -y --no-install-recommends -y python3-pip python3-setuptools python3-wheel sbt && \
    pip3 install -r /tmp/requirements.txt && \
    pip3 install cqlsh && \
    useradd -m -d /home/dockeruser -r -u $UID dockeruser

# install Spark
RUN mkdir -p /opt/graphsense && \
    wget https://archive.apache.org/dist/spark/spark-3.2.3/spark-3.2.3-bin-without-hadoop.tgz -O - | tar -xz -C /opt && \
    ln -s /opt/spark-3.2.3-bin-without-hadoop /opt/spark && \
    wget https://archive.apache.org/dist/hadoop/core/hadoop-2.7.7/hadoop-2.7.7.tar.gz -O - | tar -xz -C /opt && \
    ln -s /opt/hadoop-2.7.7 /opt/hadoop && \
    echo "#!/usr/bin/env bash\nexport SPARK_DIST_CLASSPATH=$(/opt/hadoop/bin/hadoop classpath)" >> /opt/spark/conf/spark-env.sh && \
    chmod 755 /opt/spark/conf/spark-env.sh


ENV SPARK_HOME /opt/spark

WORKDIR /opt/graphsense

RUN mkdir bin && cd bin && curl -OL https://downloads.datastax.com/dsbulk/dsbulk-1.10.tar.gz && tar -xzvf dsbulk-1.10.tar.gz
ENV PATH="$PATH:/opt/graphsense/bin/dsbulk-1.10.0/bin"


ADD src/ ./src
ADD build.sbt .
RUN sbt package && \
    chown -R dockeruser /opt/graphsense && \
    rm -rf /root/.ivy2 /root/.cache /root/.sbt

ADD docker/ .
ADD scripts/ ./scripts

USER dockeruser

EXPOSE $SPARK_UI_PORT 
#$SPARK_DRIVER_PORT $SPARK_BLOCKMGR_PORT
