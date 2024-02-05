NAME = zk-scenario-manager

# VERSION defines the project version for the project.
# Update this value when you upgrade the version of your project.
IMAGE_VERSION ?= latest

#Docker image location
#change this to your docker hub username
DOCKER_HUB ?= zerokai
IMAGE_NAME ?= zk-scenario-manager
ART_Repo_URI ?= $(DOCKER_HUB)/$(IMAGE_NAME)
IMG ?= $(ART_Repo_URI):$(IMAGE_VERSION)

export GO111MODULE=on
export BUILDER_NAME=multi-platform-builder
export GOPRIVATE=github.com/zerok-ai/zk-utils-go,github.com/zerok-ai/zk-rawdata-reader

sync:
	go get -v ./...

build: sync
	go build -v -o bin/$(NAME) cmd/main.go

run: build
	go run cmd/main.go -c ./config/config.yaml

docker-build: sync
	$(GOOS) $(ARCH) go build -v -o bin/$(NAME)-$(ARCH) cmd/main.go

docker-build-multi-arch: sync
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -o bin/$(NAME)-amd64 cmd/main.go
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -v -o bin/$(NAME)-arm64 cmd/main.go
	docker buildx rm ${BUILDER_NAME} || true
	docker buildx create --use --platform=linux/arm64,linux/amd64 --name ${BUILDER_NAME}
	docker buildx build --platform=linux/arm64,linux/amd64 --push --tag ${IMG} -f Dockerfile .
	docker buildx rm ${BUILDER_NAME}

docker-push:
	docker push $(ART_Repo_URI):$(IMAGE_VERSION)

# ------- CI-CD ------------
ci-cd-build: sync
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -o bin/$(NAME)-amd64 cmd/main.go
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -v -o bin/$(NAME)-arm64 cmd/main.go