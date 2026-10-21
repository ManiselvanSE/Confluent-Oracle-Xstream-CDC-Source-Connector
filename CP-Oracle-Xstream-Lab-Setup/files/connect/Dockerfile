# Kafka Connect worker with the Oracle XStream CDC source connector.
# The XStream Out API uses the OCI (thick) JDBC driver, so the worker needs
# Oracle Instant Client + libaio, and the connector's lib/ needs ojdbc8.jar
# and xstreams.jar from the SAME Instant Client version.
#
# stage/ is filled on the host by bin/03_deploy_confluent.sh (downloads are
# done there so the image build needs no curl/unzip inside the container).
ARG CP_VERSION

# CP 8.x images are UBI9-micro based (no microdnf/yum), so libaio is taken
# from a UBI9-minimal build stage instead of being installed in place.
FROM registry.access.redhat.com/ubi9/ubi-minimal AS libaio
RUN microdnf install -y libaio && microdnf clean all

FROM confluentinc/cp-kafka-connect:${CP_VERSION}

ARG XSTREAM_CONNECTOR_VERSION
ARG JDBC_CONNECTOR_VERSION

USER root

COPY --chown=appuser:appuser stage/instantclient /opt/oracle/instantclient
COPY --from=libaio /usr/lib64/libaio.so.1* /opt/oracle/instantclient/
COPY --chown=appuser:appuser stage/confluentinc-kafka-connect-oracle-xstream-cdc-source-${XSTREAM_CONNECTOR_VERSION} \
     /usr/share/confluent-hub-components/confluentinc-kafka-connect-oracle-xstream-cdc-source
# JDBC source/sink connector; bundles its own ojdbc8, so it needs no Instant Client
COPY --chown=appuser:appuser stage/confluentinc-kafka-connect-jdbc-${JDBC_CONNECTOR_VERSION} \
     /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc

ENV LD_LIBRARY_PATH=/opt/oracle/instantclient

USER appuser
