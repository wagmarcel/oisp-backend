FROM ubuntu:18.04
RUN apt-get update -qq && apt-get upgrade -y

RUN apt-get install -y curl unzip build-essential openjdk-8-jdk-headless wget
RUN wget https://services.gradle.org/distributions/gradle-5.1.1-bin.zip -P /tmp && unzip -d /opt/gradle /tmp/gradle-*.zip && mv /opt/gradle/gradle-*/* /opt/gradle/
RUN export PATH=$PATH:/opt/gradle/bin

ADD ./build.gradle /app/build.gradle
#RUN export PATH=$PATH:/opt/gradle/bin && cd /app && gradle build -x test --continue

ADD . /app
WORKDIR /app
RUN export PATH=$PATH:/opt/gradle/bin && make build

FROM anapsix/alpine-java:8

RUN apk add make

COPY --from=0 /app /app
RUN rm -rf /app/build/distributions
WORKDIR /app

RUN adduser -D appuser
USER appuser

EXPOSE 8080