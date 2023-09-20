NAME = zk-scenario-manager
IMAGE_NAME = zk-scenario-manager
IMAGE_NAME_MIGRATION_SUFFIX = -migration
IMAGE_VERSION = latest

LOCATION ?= us-west1
PROJECT_ID ?= zerok-dev
REPOSITORY ?= zk-client

export GO111MODULE=on
export GOPRIVATE=github.com/zerok-ai/zk-utils-go,github.com/zerok-ai/zk-rawdata-reader

sync:
	go get -v ./...

build: sync
	go build -v -o $(NAME) cmd/main.go

run: build
	go run cmd/main.go -c ./config/config.yaml 2>&1 | grep -v '^(0x'

docker-build: sync
	$(GOOS) $(ARCH) go build -v -o $(NAME) cmd/main.go

docker-push:
	docker push $(IMAGE_PREFIX)$(IMAGE_NAME)$(IMAGE_NAME_SUFFIX):$(IMAGE_VERSION)


# ------- GKE ------------

# build app image
docker-build-gke: GOOS := GOOS=linux
docker-build-gke: ARCH := GOARCH=amd64
docker-build-gke: IMAGE_PREFIX := $(LOCATION)-docker.pkg.dev/$(PROJECT_ID)/$(REPOSITORY)/
docker-build-gke: DockerFile := -f Dockerfile
docker-build-gke: docker-build

# build migration image
docker-build-migration-gke: GOOS := GOOS=linux
docker-build-migration-gke: ARCH := GOARCH=amd64
docker-build-migration-gke: IMAGE_PREFIX := $(LOCATION)-docker.pkg.dev/$(PROJECT_ID)/$(REPOSITORY)/
docker-build-migration-gke: DockerFile := -f Dockerfile-Migration
docker-build-migration-gke: IMAGE_NAME_SUFFIX := $(IMAGE_NAME_MIGRATION_SUFFIX)
docker-build-migration-gke: docker-build

# push app image
docker-push-gke: IMAGE_PREFIX := $(LOCATION)-docker.pkg.dev/$(PROJECT_ID)/$(REPOSITORY)/
docker-push-gke: docker-push

# push migration image
docker-push-migration-gke: IMAGE_PREFIX := $(LOCATION)-docker.pkg.dev/$(PROJECT_ID)/$(REPOSITORY)/
docker-push-migration-gke: IMAGE_NAME_SUFFIX := $(IMAGE_NAME_MIGRATION_SUFFIX)
docker-push-migration-gke: docker-push

# build and push
docker-build-push-gke: docker-build-gke docker-push-gke
docker-build-push-migration-gke: docker-build-migration-gke docker-push-migration-gke

# ------- CI-CD ------------
ci-cd-build: sync
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -o $(NAME) cmd/main.go

ci-cd-build-migration:

#only for local migration
create-migration-file:
	migrate create -ext sql -dir db/migrations -seq $(name)

migrate-up:
	migrate -path db/migrations -database "postgres://pl:pl=@localhost:5432/pl?sslmode=disable&x-migrations-table=zk_schema_migrations" -verbose up $(count)

migrate-down:
	migrate -path db/migrations -database "postgres://pl:pl=@localhost:5432/pl?sslmode=disable&x-migrations-table=zk_schema_migrations" -verbose down $(count)

fix-migration:
	migrate -path db/migrations -database "postgres://pl:pl=@localhost:5432/pl?sslmode=disable&x-migrations-table=zk_schema_migrations" force $(version)
