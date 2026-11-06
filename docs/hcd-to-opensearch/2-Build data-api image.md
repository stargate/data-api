# Building the `data-api-os` Container Image (Docker or Podman)

This guide explains how to build a multi-architecture (`linux/amd64` and `linux/arm64`) container image of the Data API, named `data-api-os`, from the source code, using either **Docker** or **Podman**.

The application is packaged as platform-independent Java bytecode on top of the multi-arch base image `registry.access.redhat.com/ubi9/openjdk-21-runtime`, so a single Maven build is enough to produce an image for every architecture.

---

## 1. Prerequisites

- **Java 21 JDK** (e.g., Eclipse Temurin 21)
- A clone of the repository on branch `clun/hcd-to-opensearch`
- One of the following container engines:
  - **Docker** with **Buildx** enabled (Docker Desktop, or Colima/Rancher with the `buildx` plugin)
  - **Podman** 4.x or later
    - macOS/Windows: a running Podman machine (`podman machine init && podman machine start`)
    - Linux: `qemu-user-static` installed (e.g. `sudo dnf install qemu-user-static`) to build for a non-native architecture
- For multi-arch builds: a container registry you can push to (`docker login` / `podman login`)

> All `podman` commands below are the same as their `docker` equivalents unless stated otherwise. Podman's `docker` CLI compatibility alias (`alias docker=podman`) is **not** sufficient for multi-arch builds, because `docker buildx` has no Podman equivalent: use the Podman commands shown here.

---

## 2. Package the Application

From the root of the repository:

```bash
./mvnw clean package -DskipTests
```

This creates the `target/quarkus-app/` directory used by [`Dockerfile.jvm`](../../src/main/docker/Dockerfile.jvm).

> Optionally add `-Dfmt.skip=true -Djacoco.skip=true` to skip formatting and coverage checks.

If the build fails with `FormatterException: ... error: : or -> expected` (or `There were errors when formatting files`), the build is running on an older JDK. The sources use Java 21 syntax (`case null ->`, `case String s when ...`) that the formatter cannot parse on Java 17. This is not a code style problem, and `fmt:format` cannot fix it.

Check and switch the JDK, then rebuild:

```bash
java -version                # must report 21
sdk use java 21.0.7-tem      # SDKMAN example, current shell only
# or: export JAVA_HOME=/path/to/jdk-21
./mvnw clean package -DskipTests
```

Once on Java 21, genuine formatting issues can be fixed with:

```bash
./mvnw com.spotify.fmt:fmt-maven-plugin:2.23:format
```

---

## 3. Build the Image

### Option A: Multi-architecture image (`linux/amd64` + `linux/arm64`)

#### With Docker

1. Create a Buildx builder (once only):
   ```bash
   docker buildx create --name data-api-builder --use
   docker buildx inspect --bootstrap
   ```

2. Build and push the multi-arch image:
   ```bash
   docker buildx build \
     --platform linux/amd64,linux/arm64 \
     -f src/main/docker/Dockerfile.jvm \
     -t <registry>/<namespace>/data-api-os:latest \
     --push .
   ```

   Replace `<registry>/<namespace>` with your target (e.g. `docker.io/myuser` or `quay.io/myorg`).

   > A multi-platform image cannot be loaded into the local Docker image store with `--load` when using the classic Docker image store. It must be pushed to a registry (`--push`) or exported (`--output type=oci,dest=data-api-os.tar`). If Docker Desktop's *containerd image store* is enabled, `--load` works for multi-platform images as well.

3. Check the published architectures:
   ```bash
   docker buildx imagetools inspect <registry>/<namespace>/data-api-os:latest
   ```

#### With Podman

Podman builds multi-arch images by building each platform into a local **manifest list**, then pushing that list.

1. Build both architectures into a manifest list named `data-api-os`:
   ```bash
   podman build \
     --platform linux/amd64,linux/arm64 \
     --manifest data-api-os:latest \
     -f src/main/docker/Dockerfile.jvm .
   ```

2. Push the manifest list (and all the images it references) to a registry:
   ```bash
   podman manifest push --all data-api-os:latest \
     docker://<registry>/<namespace>/data-api-os:latest
   ```

   Replace `<registry>/<namespace>` with your target (e.g. `docker.io/myuser` or `quay.io/myorg`).

3. Check the architectures contained in the manifest list:
   ```bash
   podman manifest inspect data-api-os:latest
   ```

> If a `data-api-os:latest` manifest or image already exists locally, remove it first with `podman manifest rm data-api-os:latest`, otherwise the new images are appended to it.

### Option B: Single-architecture image for local use

Build for the architecture of your machine and keep it in the local image store.

**Docker**
```bash
docker build \
  -f src/main/docker/Dockerfile.jvm \
  -t data-api-os:latest .
```

**Podman**
```bash
podman build \
  -f src/main/docker/Dockerfile.jvm \
  -t data-api-os:latest .
```

To target a specific architecture explicitly (e.g. `linux/amd64` from an Apple Silicon Mac):

**Docker**
```bash
docker buildx build \
  --platform linux/amd64 \
  -f src/main/docker/Dockerfile.jvm \
  -t data-api-os:latest-amd64 \
  --load .
```

**Podman**
```bash
podman build \
  --platform linux/amd64 \
  -f src/main/docker/Dockerfile.jvm \
  -t data-api-os:latest-amd64 .
```

---

## 4. Verify the Image

**Docker**
```bash
docker images data-api-os
```

**Podman**
```bash
podman images data-api-os
```

The image is listed as `data-api-os:latest` (`localhost/data-api-os:latest` with Podman).

The Data API needs a backend database to start, so the image is not run on its own. It is started together with HCD and OpenSearch by the compose stack, see [3-Start everything.md](3-Start%20everything.md).
