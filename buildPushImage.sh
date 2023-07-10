GOOS=linux GOARCH=amd64 go build -o zk-scenario-manager cmd/main.go
docker build -t zk-scenario-manager:dev .
sh ./gcp-artifact-deploy-go.sh