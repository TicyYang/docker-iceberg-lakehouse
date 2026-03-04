FROM python:3.10-slim-bullseye

ARG SPARK_VERSION=3.5.6
ARG SPARK_MAJOR_VERSION=3.5
ARG ICEBERG_VERSION=1.9.0

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    sudo curl vim unzip openjdk-17-jdk build-essential software-properties-common ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR ${SPARK_HOME}

# Download Spark 
RUN curl -s https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark.tgz \
 && tar xvzf spark.tgz --directory /opt/spark --strip-components 1 \
 && rm spark.tgz

# Download JARs
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar \
 && curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
 && curl -s https://jdbc.postgresql.org/download/postgresql-42.7.8.jar -Lo /opt/spark/jars/postgresql-jdbc.jar

# Install Python and other packages
COPY requirements.txt .
RUN pip3 install -r requirements.txt \
 && pip3 install --no-cache-dir awscli \
 && python3 -m spylon_kernel install

# Create directories
RUN mkdir -p /home/iceberg/localwarehouse /home/iceberg/notebooks /home/iceberg/warehouse /home/iceberg/spark-events /home/iceberg /root/.ipython/profile_default/startup

# Set up startup scripts
RUN echo '#! /bin/sh\nexport PYSPARK_DRIVER_PYTHON=jupyter-notebook\nexport PYSPARK_DRIVER_PYTHON_OPTS="--notebook-dir=/home/iceberg/notebooks --ip='\''*'\'' --NotebookApp.token='\'''\'' --NotebookApp.password='\'''\'' --port=8888 --no-browser --allow-root"\npyspark' > /bin/notebook \
 && chmod u+x /bin/notebook \
 && ln -s /bin/notebook /bin/pyspark-notebook

# Copy local configurations and notebooks
COPY notebooks/ /home/iceberg/notebooks
COPY ipython/startup/ /root/.ipython/profile_default/startup/
COPY spark-defaults.conf /opt/spark/conf
COPY .pyiceberg.yaml /root/.pyiceberg.yaml
COPY entrypoint.sh .

RUN chmod u+x /opt/spark/sbin/* && chmod u+x /opt/spark/bin/*

ENTRYPOINT ["./entrypoint.sh"]
CMD ["notebook"]
