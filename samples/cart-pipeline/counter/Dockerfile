# The counter's image: Flink's own, with the streamlet's jar and the dependencies Flink does not
# provide in usrlib, which Flink puts on the job's classpath. `sbt cartCounter/image` stages the
# jars in target/image and builds this file there.
ARG FLINK_IMAGE
FROM ${FLINK_IMAGE}
COPY --chown=flink:flink usrlib/ /opt/flink/usrlib/
