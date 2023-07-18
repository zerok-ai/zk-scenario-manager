NAME = zk-scenario-manager
IMAGE_NAME = zk-scenario-manager
IMAGE_VERSION = 1.0

LOCATION ?= us-west1
PROJECT_ID ?= zerok-dev
REPOSITORY ?= zk-client

export GO111MODULE=on
export GOPRIVATE=github.com/zerok-ai/zk-utils-go,github.com/zerok-ai/zk-rawdata-reader

sync:
	go get -v ./...

build: sync
	go build -v -o $(NAME) cmd/main.go

docker-build: sync
	CGO_ENABLED=0 GOOS=linux $(ARCH) go build -v -o $(NAME) cmd/main.go
	docker build --no-cache -t $(IMAGE_PREFIX)$(IMAGE_NAME):$(IMAGE_VERSION) .

docker-build-gke: IMAGE_PREFIX := $(LOCATION)-docker.pkg.dev/$(PROJECT_ID)/$(REPOSITORY)/
docker-build-gke: ARCH := GOARCH=amd64
docker-build-gke: docker-build

docker-push-gke: IMAGE_PREFIX := $(LOCATION)-docker.pkg.dev/$(PROJECT_ID)/$(REPOSITORY)/
docker-push-gke:
	docker push $(IMAGE_PREFIX)$(IMAGE_NAME):$(IMAGE_VERSION)

docker-build-push-gke: docker-build-gke docker-push-gke

run: build
	go run cmd/main.go -c ./config/config.yaml 2>&1 | grep -v '^(0x'

create-migration-file:
	migrate create -ext sql -dir db/migrations -seq $(name)

migrate-up:
	migrate -path db/migrations -database "postgres://$$PL_POSTGRES_USERNAME:$$PL_POSTGRES_PASSWORD=@localhost:5432/zk?sslmode=disable&x-migrations-table=$$ZK_SCHEMA_MIGRATIONS_TABLE_NAME" -verbose up $(count)

migrate-down:
	migrate -path db/migrations -database "postgres://$$PL_POSTGRES_USERNAME:$$PL_POSTGRES_PASSWORD@localhost:5432/zk?sslmode=disable&x-migrations-table=$$ZK_SCHEMA_MIGRATIONS_TABLE_NAME" -verbose down $(count)

fix-migration:
	migrate -path db/migrations -database "postgres://$$PL_POSTGRES_USERNAME:$$PL_POSTGRES_PASSWORD@localhost:5432/zk?sslmode=disable&x-migrations-table=$$ZK_SCHEMA_MIGRATIONS_TABLE_NAME" force $(version)
