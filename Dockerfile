FROM ubuntu:latest
RUN apt-get update && yes|apt-get upgrade
RUN apt-get install -y wget bzip2 sudo
RUN apt-get install -y emacs nano vim default-jre
RUN apt-get install openjdk-8-jre-headless

RUN adduser --disabled-password --gecos '' nancy
RUN adduser nancy sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER nancy
WORKDIR /home/nancy/

RUN sudo chmod a+rwx -R /home/nancy/
 
RUN wget https://repo.anaconda.com/archive/Anaconda3-2019.07-Linux-x86_64.sh
RUN bash Anaconda3-2019.07-Linux-x86_64.sh -b
RUN rm Anaconda3-2019.07-Linux-x86_64.sh
ENV PATH /home/nancy/anaconda3/bin:$PATH

RUN pip install --upgrade pip

RUN jupyter notebook --generate-config --allow-root

 
RUN wget https://archive.apache.org/dist/zookeeper/zookeeper-3.5.5/apache-zookeeper-3.5.5-bin.tar.gz
RUN tar -xvf apache-zookeeper-3.5.5-bin.tar.gz
RUN rm apache-zookeeper-3.5.5-bin.tar.gz
RUN mkdir apache-zookeeper-3.5.5-bin/data
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

RUN wget https://archive.apache.org/dist/kafka/2.3.0/kafka_2.12-2.3.0.tgz
RUN tar -xzf kafka_2.12-2.3.0.tgz
RUN rm kafka_2.12-2.3.0.tgz

RUN wget https://archive.apache.org/dist/cassandra/3.11.4/apache-cassandra-3.11.4-bin.tar.gz
RUN tar -xvf apache-cassandra-3.11.4-bin.tar.gz
RUN rm apache-cassandra-3.11.4-bin.tar.gz
ENV PATH /home/nancy/apache-cassandra-3.11.4/bin:$PATH


RUN sudo apt-get install -y gcc
RUN pip install apache-airflow

RUN pip install Flask==1.0.4
RUN airflow db init
RUN mkdir /home/nancy/airflow/dags

ADD . /home/nancy/kafka_stock
RUN sudo chmod 777 -R /home/nancy/kafka_stock
RUN cp kafka_stock/zoo.cfg apache-zookeeper-3.5.5-bin/conf/
RUN pip install -r kafka_stock/requirements.txt

RUN sudo apt-get install -y tmux
ENV NAME kafka_stock


