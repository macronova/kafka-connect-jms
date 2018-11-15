FROM alpine:latest as builder
ENV VERSION 1.0.0
COPY build/distributions/kafka-connect-jms-${VERSION}.zip /tmp/
RUN mkdir /tmp/build \
	&& unzip /tmp/kafka-connect-jms-${VERSION}.zip -d /tmp/build/ \
	&& rm /tmp/kafka-connect-jms-${VERSION}.zip

FROM confluentinc/cp-kafka-connect:5.0.0
ENV VERSION 1.0.0
ENV CLASSPATH ${CLASSPATH}:/usr/share/java/macronova-connect-jms/*
COPY --from=builder /tmp/build/* /usr/share/java/macronova-connect-jms/
COPY examples/*/client/*.jar /usr/share/java/macronova-connect-jms/
COPY examples/*/config/* /tmp/config/