FROM alpine:latest
WORKDIR /zk

# base name of the executable
ENV exeBaseName="zk-scenario-manager"

# full path to the all the executables
ENV exeAMD64="${exeBaseName}-amd64"
ENV exeARM64="${exeBaseName}-arm64"

# copy the executables
COPY *"bin/$exeAMD64" .
COPY *"bin/$exeARM64" .

# copy the start script
COPY app-start.sh .
RUN chmod +x app-start.sh
RUN #apk add delve

# call the start script
CMD ["sh","-c","./app-start.sh --amd64 ${exeAMD64} --arm64 ${exeARM64} -c config/config.yaml"]
#CMD ["sh","-c","dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec /zk/zk-scenario-manager-amd64 -- -c /zk/config/config.yaml"]