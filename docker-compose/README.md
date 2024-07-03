# Docker Compose scripts for Data API with HCD or DSE-6.9

This directory provides two ways to start the Data API with HCD or DSE-6.9 using `docker compose`.

## Prerequisites

### Docker / Docker Compose Versions

Make sure that you have Docker engine 20.x installed, which should include Docker compose 2.x. Our compose files rely on features only available in the Docker compose v2 file format.

### Building the local Docker image
If you want to use a locally built version of the Data API Docker image rather than pulling a released version from Docker Hub, run the following command at the root of the repository to build the image:

```bash
./mvnw clean package -Dquarkus.container-image.build=true -DskipTests
```

You can control the platform using the `-Dquarkus.docker.buildx.platform=linux/amd64,linux/arm64` property.

Follow instructions under the [Script options](#script-options) section to use the locally built image.

## Data API with HCD or DSE-6.9 cluster

You can start a simple configuration with HCD with the following command:

```
./start_hcd.sh
``` 

You can start a simple configuration with DSE 6.9 with the following command:

```
./start_dse69.sh
``` 

This convenience script verifies your Docker installation meets minimum requirements and brings up the configuration described in the `docker-compose.yml` (DSE-6.9) or `docker-compose-hcd.yml` (HCD) file.
The configuration includes health criteria for each container that is used to ensure the containers come up in the correct order.

The convenience script uses the `-d` and `--wait` options to track the startup progress, so that the compose command exits when all containers have started and reported healthy status within a specified timeout. 

The environment settings in the `.env` file include variables that describe the image tags that will be used by default, typically Data API `v1` and DSE `6.9`. The `start_dse69.sh` script supports [options](#script-options) for overriding which image tags are used, including using a locally generated image as described [above](#building-the-local-docker-image).
We recommend doing a `docker compose pull` periodically to ensure you always have the latest patch versions of these tags.

Once done using the containers, you can stop them using the command `docker compose down`.

## HCD or DSE-6.9 cluster WITHOUT Data API

To run local non-Docker instance of Data API, use `-d` with the script: it will prevent running of Data API container:

```
./start_hcd.sh -d
# OR
./start_dse69.sh -d
```

To run locally built Data API in Quarkus Development mode, you will use:

```
/mvnw compile quarkus:dev -Dstargate.data-store.ignore-bridge=true \
  -Dstargate.jsonapi.operations.vectorize-enabled=true \
  -Dstargate.jsonapi.operations.database-config.local-datacenter=dc1
```

## Script options

Both convenience scripts support the following options:

* You can specify an image tag (version) of the Data API using `-j [VERSION]`, or use the `-l` tag to use a locally built image with the latest snapshot version. 

* The scripts default to using the Java-based image for Data API, you can specify to use the native GraalVM based variant using `-n`.

* You can change the default root log level for the Data API using `-r [LEVEL]` (default `INFO`). Valid values: `ERROR`, `WARN`, `INFO`, `DEBUG`

* You can enable reguest logging for the Data API using `-q`: if so, each request is logged under category `io.quarkus.http.access-log`

* You can start DSE/HCD node only using `-d` option. This is useful when you want to start Data API locally for development.

* You can specify the number of cassandra node required using `-n [NUMBER]`. Default is 1.

## CQLSH

A convenient way to run `cqlsh` to connect locally is to use one of:

```
# HCD:
docker exec -it data-api-hcd-1 cqlsh -u cassandra -p cassandra

# DSE 6.9:
docker exec -it data-api-dse-1 cqlsh -u cassandra -p cassandra
```

## Notes

* The `.env` file defines variables for the docker compose project name (`COMPOSE_PROJECT_NAME`),
 the DSE Docker image tag to use (`DSETAG`) and the HCD Docker image tag to use (`HCDTAG`).

* You can override forwarding cqlsh port for `DSE` using `DSE_FWD_PORT` environment variable respectively.
