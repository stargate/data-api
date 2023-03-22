# Docker Compose scripts for JSONAPI with DSE 6.8

This directory provides two ways to start the JSON API and Stargate coordinator with DSE 6.8 using `docker compose`.

## Stargate JSON API with 3-node DSE 6.8 cluster

> **PREREQUISITES:** Please build the latest JSONAPI docker image first by running: 
> 
> ```bash
> ./mvnw clean package -Dquarkus.container-image.build=true -DskipTests
> ```
> 
> You can control the platform using the `-Dquarkus.docker.buildx.platform=linux/amd64,linux/arm64` property.

You can start a simple Stargate configuration with the following command:

```
docker-compose up -d
``` 

This brings up the configuration described in the `docker-compose.yml` file. The configuration includes health criteria for each container that is used to ensure the containers come up in the correct order.

Using the `-d` option tracks the startup progress, so that the compose command exits when all containers have started or a failure is detected. Omitting the `-d` option causes the command to track the progress of all containers, including all log output, and a `Ctrl-C` will cause all the containers to exit.

The default environment settings in the `.env` file include variables that describe which image tags to use, typically Stargate `v2` and DSE `6.8.X` (where `X` is the latest supported patch version). We recommend doing a `docker compose pull` periodically to ensure you always have the latest patch versions of these tags.

You can override the default environment settings in your local shell, or use the convenient shell script `start_dse_68.sh`.

Whether you use the shell script or start `docker compose` directly, you can remove the stack of containers created by executing `docker compose down`.

## Stargate JSON API with embedded DSE 6.8 in coordinator (developer mode)

This alternate configuration runs the Stargate coordinator node in developer mode, so that no separate Cassandra cluster is required. This can be run with the command:

```
docker-compose -f docker-compose-dev-mode.yml up -d
``` 

To stop the configuration, use the command:

```
docker-compose -f docker-compose-dev-mode.yml down
``` 

This configuration is useful for development and testing since it initializes more quickly, but is not recommended for production deployments. This configuration also has a convenience script: `start_dse_68_dev_mode.sh`.

## Script options

Both convenience scripts support the following options:

* You can specify an image tag (version) of the JSON API using `-t [VERSION]`. 

* The scripts default to using the Java-based image for JSON API, you can specify to use the native GraalVM based variant using `-n`.

* You can change the default root log level using `-r [LEVEL]` (default `INFO`). Valid values: `ERROR`, `WARN`, `INFO`, `DEBUG`

* You can enable reguest logging using `-q`: if so, each request is logged under category `io.quarkus.http.access-log`

## Notes

* The `.env` file defines variables for the docker compose project name (`COMPOSE_PROJECT_NAME`),
 the DSE Docker image tag to use (`DSETAG`), and the Stargate Docker image tag to use (`SGTAG`).

* When using the convenience scripts, the Docker image (`JSONTAG`) is the current (snapshot) version as defined in the top level project `pom.xml` file. It can be overridden with the `-t` option on either script:

  `./start_dse_68.sh -t v1.0.0`

* Running more than one of these multi-container environments on one host may require changing the port mapping to be changed to avoid conflicts on the host machine.

## Troubleshooting

If you see an error like:
```
Pulling coordinator (stargateio/jsonapi:1.0.0-SNAPSHOT)...
ERROR: manifest for stargateio/jsonapi:1.0.0-SNAPSHOT not found: manifest unknown: manifest unknown
```

you are trying to deploy a version that is neither publicly available (official release) nor built locally. See the top level [README](../README.md) file for instructions on building the local image. 

