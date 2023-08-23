#!/bin/bash
THIS_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DOCKER_REPO=$1

echo "THIS_DIR=$THIS_DIR"
echo "DOCKER_REPO=$DOCKER_REPO"

# Set the image tag based on the chart version
perl -pi -e "s#dockerBase: \".*\"#dockerBase: \"$DOCKER_REPO\"#" $THIS_DIR/values.yaml
