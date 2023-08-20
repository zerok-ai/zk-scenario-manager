#!/bin/bash
THIS_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CHART_VERSION=$1

# Set the image tag based on the chart version
sed -i '' -e "s/tag: \"latest\"/tag: \"$CHART_VERSION\"/" $THIS_DIR/values.yaml
sed -i '' -e "s/migrationTag: \"latest\"/migrationTag: \"$CHART_VERSION\"/" $THIS_DIR/values.yaml
