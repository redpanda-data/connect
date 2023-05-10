Studio Demo
===========

This directory contains some fun toys for messing around with Benthos in Kubernetes, both with ConfigMap based deployments as well as instances using [Benthos Studio](https://studio.benthos.dev/) for their config management.

## Preparation

Firstly, make sure you have Kubernetes set up and the `kubectl` command configured to access the context you're interested in.

Next, run any queue systems you might want to experiment with along with Benthos, this directory contains a very bare-bones [NATS](https://nats.io/) set up that you can run with:

```sh
kubectl apply -f ./nats.yaml
```

> Hint: Expose the NATS server to your local environment with `kubectl port-forward service/nats 4222:4222`.

## Benthos with ConfigMap

There are two Benthos deployments that utilise a ConfigMap you can run, the first `./benthos-generator.yaml` generates data and dumps it into a NATS subject "demo-a", you can run it with:

```sh
kubectl apply -f ./benthos-generator.yaml
```

And similarly you can run the other ConfigMap example `./benthos-server.yaml`, which creates an HTTP server via Benthos, with:

```sh
kubectl apply -f ./benthos-server.yaml
```

> Hint: Expose the Benthos server to your local environment with `kubectl port-forward service/benthos-server 4195:4195`.

## Benthos with Studio

A major advantage to using [Benthos Studio](https://studio.benthos.dev/) is that you can deploy an arbitrary (and dynamic) number of Benthos replicas to k8s and they will be automatically distributed across your configs. No need for using a ConfigMap and no need to have a separate deployment for each config.

Once you've created a session, some configs and at least one deployment add a secret to k8s containing the access token and secret combination, these are all found in the Benthos Studio session page:

```bash
kubectl create secret generic benthos-studio \
    --from-literal=token='TODO' \
    --from-literal=secret='TODO'
```

And edit the `benthos-studio-node.yaml` deployment template to swap out `<SESSION_ID>` within the container args to match the session you're targetting.

Then deploy your Benthos instances with:

```sh
kubectl apply -f ./benthos-studio-node.yaml
```

> Hint: Now in your Studio Session try adding a config that joins all these components together:

```yaml
input:
  nats:
    urls:
      - nats://nats:4222
    subject: demo-a
    queue: studio-nodes
pipeline:
  processors:
    - http:
        url: http://benthos-server:4195/test/a
        verb: POST
    - http:
        url: http://benthos-server:4195/test/b
        verb: POST
output:
  nats:
    urls:
      - nats://nats:4222
    subject: demo-b
```

## Probing the Data

In order to see data at different subjects from your local machine ensure that you have the NATS server port 4222 forwarded onto your machine:

```sh
kubectl port-forward service/nats 4222:4222
```

And then run a simple Benthos server to consume data at a given subject:

```sh
benthos -s 'input.nats.urls=nats://localhost:4222' -s 'input.nats.subject=demo-b'
```
