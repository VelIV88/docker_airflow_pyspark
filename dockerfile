FROM puckel/docker-airflow:1.10.9

COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8

ENV JAVA_HOME /usr/local/openjdk-8

USER root
RUN pip install pyspark==2.3.1 && pip install requests && pip install mysql-connector-python

RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1

COPY mysql-connector-java_8.0.29-1debian10_all.deb mysql-connector-java_8.0.29-1debian10_all.deb
RUN dpkg -i mysql-connector-java_8.0.29-1debian10_all.deb

COPY create_tables.py create_tables.py
