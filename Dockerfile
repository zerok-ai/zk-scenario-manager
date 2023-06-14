FROM golang:1.18-alpine
WORKDIR /zk
COPY zk-scenario-manager .
CMD ["/zk/zk-scenario-manager", "-c", "/zk/config/config.yaml"]

