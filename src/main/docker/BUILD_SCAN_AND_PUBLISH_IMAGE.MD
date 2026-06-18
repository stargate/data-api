# 🐳 Building and Testing the Docker Image for `sgv2-jsonapi`

This document explains how to build, test, tag, push, and scan the `sgv2-jsonapi` 
Docker image using **Podman**. These steps are typically run **before a release** 
to verify the integrity and security of the image.

---

## 🧰 Prerequisites

### Install Podman (if not already installed)

This installation is provided as an alternative for DOCKER DESKTOP if not allowed in your machine.

<details>
<summary><strong>macOS</strong></summary>

```bash
brew install podman
podman machine init  # if first time
podman machine start
```

</details>

<details>
<summary><strong>Linux (Debian/Ubuntu)</strong></summary>

```bash
sudo apt update
sudo apt -y install podman
```

</details>

For other OS, see: https://podman.io/getting-started/installation

## 🔐 Docker Hub Login

Make sure you're authenticated before pushing images:

```bash
podman login docker.io
```

---

## 🏗️ Step 1: Prepare and Build the Project

Format the code and build the JAR without running tests. 
The purpose is to populate `./target` that is used in the docker image build.

```bash
./mvnw fmt:format
./mvnw xml-format:xml-format
./mvnw clean test-compile package -Dmaven.test.skip=true -DskipTests=true
```

---

## 🐋 Step 2: Build the Docker Image

The target architecture is here `linux/amd64` but th

```bash
podman build --platform linux/amd64,linux/arm64 -f src/main/docker/Dockerfile.jvm -t quarkus/sgv2-jsonapi-jvm .
```

---

## 🔖 Step 3: Tag the Image

```bash
export DOCKER_HUB_ACCOUNT=yourdockerhubusername 
 # change this to your Docker Hub account
podman tag localhost/quarkus/sgv2-jsonapi-jvm $DOCKER_HUB_ACCOUNT/stargate-builds:latest
---

## ✅ Step 4: Test the Image Locally

```bash
podman run -i --rm -p 8181:8181 quarkus/sgv2-jsonapi-jvm
```

---

## 📦 Step 5: Push the Image to Remote Registry

Replace the tag with the appropriate version:

```bash
export IMAGE_TAG=1.0.27
podman tag localhost/quarkus/sgv2-jsonapi-jvm $DOCKER_HUB_ACCOUNT/stargate-builds:$IMAGE_TAG
podman push $DOCKER_HUB_ACCOUNT/stargate-builds:1.0.27-patchcve-7
```

---

## 🔍 Step 6: Scan the Image with Snyk

Install and authenticate:

```bash
brew tap snyk/tap
brew install snyk
snyk auth
```

Scan the image:

```bash
snyk container test \
  $DOCKER_HUB_ACCOUNT/stargate-builds:$IMAGE_TAG \
  --platform=linux/amd64 \
  --json-file-output=data-api-java.json
```

---
