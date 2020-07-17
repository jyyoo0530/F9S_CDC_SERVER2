FROM openjdk:8-jdk

ENV SBT_VERSION  1.3.13
ENV SBT_JAR      https://repo1.maven.org/maven2/org/scala-sbt/sbt/$SBT_VERSION/sbt-$SBT_VERSION.jar


ADD  $SBT_JAR  /usr/local/bin/sbt-launch.jar
COPY sbt.sh    /usr/local/bin/sbt

RUN echo "==> fetch all sbt jars from Maven repo..."       && \
    echo "==> [CAUTION] this may take several minutes!!!"  && \
    sbt


VOLUME [ "/app" ]
WORKDIR /app


# Define default command.
ENTRYPOINT ["sbt"]
CMD ["--version"]