# Zerok Scenario Manager
The Zerok Scenario Manager is part of the [Zerok System](https://zerok-ai.github.io/helm-charts/), which is a set of tools for observability in Kubernetes clusters. The Zerok System works along with the OpenTelemetry Operator. Check out these docs [add link here] to learn more about how Zerok can benefit you.

The Zerok Scenario Manager uses filtered OpenTelementry Span data retrieved from Redis. Zerok observer populates this data in Redis. The Zerok Scenario Manager collates the data for filtered traces across different Zerok Observer pods and exports the data to OpenTelemetry Collector. You can write an OpenTelemetry Collector pipeline to export this data to another tracing platform like Jaeger. 
## Prerequisites
Zerok Operator and Zerok Observer need to be installed in the cluster in the zk-client namespace for the scenario manager to work. You can refer to the links [zk-operator](https://github.com/zerok-ai/zk-operator) and [zk-observer](https://github.com/zerok-ai/zk-observer) for details about installing the Zerok Operator and Zerok Observer.

## Get Helm Repositories Info

```console
helm repo add zerok-ai https://zerok-ai.github.io/helm-charts
helm repo update
```

_See [`helm repo`](https://helm.sh/docs/helm/helm_repo/) for command documentation._

## Install Helm Chart

Install the Zerok Observer.
```console
helm install [RELEASE_NAME] zerok-ai/zk-scenario-manager
```

_See [helm install](https://helm.sh/docs/helm/helm_install/) for command documentation._

## Uninstall Helm Chart

```console
helm uninstall [RELEASE_NAME]
```

This removes all the Kubernetes components associated with the chart and deletes the release.

_See [helm uninstall](https://helm.sh/docs/helm/helm_uninstall/) for command documentation._

## Upgrading Helm Chart

```console
helm upgrade [RELEASE_NAME] [CHART] --install
```

_See [helm upgrade](https://helm.sh/docs/helm/helm_upgrade/) for command documentation._

### Contributing
Contributions to the Zerok Scenario Manager are welcome! Submit bug reports, feature requests, or code contributions.

### Reporting Issues
Encounter an issue? Please file a report on our GitHub issues page with detailed information to aid in quick resolution.