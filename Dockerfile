FROM gradle:5.3.1-jdk as build
USER root
RUN apt-get update && apt-get install -y make

ADD ./build.gradle /app/build.gradle

ADD . /app
WORKDIR /app
RUN make build
USER appuser
FROM anapsix/alpine-java:11

RUN apk --no-cache add make && rm -rf /var/cache/apk/*

COPY --from=build /app/build/libs /app/build/libs/
COPY --from=build /app/Makefile /app/wait-for-it.sh /app/
RUN rm -rf /app/build/distributions
WORKDIR /app

RUN adduser -D appuser
USER appuser

EXPOSE 8080
