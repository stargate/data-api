# Building Data API Docker Image & Configuration Guide

This guide explains how to build a Docker container image of the Data API from source and configure feature flags (including the OpenSearch feature flag) via environment variables, configuration properties, or HTTP request headers.

---

## 1. Prerequisites

- **Java 21 JDK** installed (e.g., Eclipse Temurin 21)
- **Docker** daemon running (Docker Desktop, Colima, Rancher, or Podman)
- Git repository clone on branch `clun/hcd-to-opensearch`

---

## 2. Building the Docker Image

### Option A: Standard Build (Recommended)

1. **Package the Quarkus Application:**
   ```bash
   ./mvnw clean package -DskipTests
   ```
   *(Optionally add `-Dfmt.skip=true -Djacoco.skip=true` to skip formatting/coverage checks).*

2. **Build the Container Image:**
   ```bash
   docker build \
     -f src/main/docker/Dockerfile.jvm \
     -t stargate-data-api:hcd-opensearch .
   ```

### Option B: One-Step Build via Maven & Quarkus

Quarkus can package and build the Docker image in a single command:
```bash
./mvnw clean package -DskipTests \
  -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.group=stargateio \
  -Dquarkus.container-image.name=data-api \
  -Dquarkus.container-image.tag=opensearch-preview
```

### Option C: Cross-Platform Build (e.g., linux/amd64 on Apple Silicon)

If building on an ARM64 Mac for deployment onto an x86/amd64 Linux server or Kubernetes cluster:
```bash
./mvnw clean package -DskipTests
docker buildx build \
  --platform linux/amd64 \
  -f src/main/docker/Dockerfile.jvm \
  -t stargate-data-api:hcd-opensearch-amd64 \
  --load .
```

---

## 3. Feature Flags & Configuration

### OpenSearch Feature Flag Overview

| Aspect | Value / Setting |
|---|---|
| **Property Name** | `stargate.feature.flags.open-search` |
| **Environment Variable** | `STARGATE_FEATURE_FLAGS_OPEN_SEARCH` |
| **Default Value** | `false` (**Disabled by default**) |
| **Per-Request HTTP Header** | `Feature-Flag-open-search: true` or `false` |

> **Default Behavior:** When deployed where OpenSearch is **not** available (such as Astra DB or standard Cassandra), OpenSearch support is completely disabled out-of-the-box. Any attempt to create collections or tables with OpenSearch replication will fail gracefully with error `OPEN_SEARCH_FEATURE_NOT_ENABLED`.

### Summary of Available Feature Flags

| Feature | Config Property | Environment Variable | Default | HTTP Header Override |
|---|---|---|---|---|
| **OpenSearch** | `stargate.feature.flags.open-search` | `STARGATE_FEATURE_FLAGS_OPEN_SEARCH` | `false` | `Feature-Flag-open-search` |
| **Lexical Search** | `stargate.feature.flags.lexical` | `STARGATE_FEATURE_FLAGS_LEXICAL` | `true` | `Feature-Flag-lexical` |
| **Reranking** | `stargate.feature.flags.reranking` | `STARGATE_FEATURE_FLAGS_RERANKING` | `false` | `Feature-Flag-reranking` |
| **MCP (Model Context Protocol)** | `stargate.feature.flags.mcp` | `STARGATE_FEATURE_FLAGS_MCP` | `false` | `Feature-Flag-mcp` |
| **Request Tracing** | `stargate.feature.flags.request-tracing` | `STARGATE_FEATURE_FLAGS_REQUEST_TRACING` | `false` | `Feature-Flag-request-tracing` |

---

## 4. Running the Docker Container

### Example 1: Run with OpenSearch Support Enabled (HCD)

```bash
docker run -d --rm \
  --name data-api \
  -p 8181:8181 \
  -e QUARKUS_CASSANDRA_CONTACT_POINTS="hcd-node1:9042" \
  -e QUARKUS_CASSANDRA_LOCAL_DATACENTER="dc1" \
  -e STARGATE_FEATURE_FLAGS_OPEN_SEARCH="true" \
  stargate-data-api:hcd-opensearch
```

### Example 2: Run with OpenSearch Support Disabled (Astra / Vanilla Cassandra)

```bash
docker run -d --rm \
  --name data-api \
  -p 8181:8181 \
  -e QUARKUS_CASSANDRA_CONTACT_POINTS="cassandra-node1:9042" \
  -e QUARKUS_CASSANDRA_LOCAL_DATACENTER="dc1" \
  -e STARGATE_FEATURE_FLAGS_OPEN_SEARCH="false" \
  stargate-data-api:hcd-opensearch
```
*(Note: Since `false` is the default, omitting the environment variable will also keep OpenSearch disabled).*

---

## 5. Verification & Testing

### 1. Check Swagger UI
Open `http://localhost:8181/swagger-ui/` in your browser.

### 2. Test Creating an OpenSearch Collection (When Enabled)

**Request:**
```bash
curl -X POST http://localhost:8181/v1/my_keyspace \
  -H "Content-Type: application/json" \
  -H "Token: <your-token>" \
  -d '{
    "createCollection": {
      "name": "articles",
      "options": {
        "openSearch": {
          "enabled": true,
          "mappings": {
            "title": { "type": "text" },
            "body": { "type": "text" },
            "published": { "type": "boolean" }
          }
        }
      }
    }
  }'
```

**Expected Response:**
```json
{
  "status": {
    "ok": 1
  }
}
```

### 3. Test Behavior When OpenSearch is Disabled

When `STARGATE_FEATURE_FLAGS_OPEN_SEARCH=false`:
- Creating a collection with `"openSearch": {"enabled": true, ...}` returns:
  ```json
  {
    "errors": [
      {
        "message": "OpenSearch feature is not enabled for this database.",
        "errorCode": "OPEN_SEARCH_FEATURE_NOT_ENABLED",
        "exceptionClass": "SchemaException"
      }
    ]
  }
  ```
- Creating standard collections without `"openSearch"` or explicitly passing `"openSearch": {"enabled": false}` **succeeds normally**.

### 4. Per-Request Header Override (Testing / Dynamic Gating)

If the feature flag is left unset in the environment, you can dynamically enable or disable it on a per-request basis using the HTTP header:
```bash
curl -X POST http://localhost:8181/v1/my_keyspace \
  -H "Content-Type: application/json" \
  -H "Token: <your-token>" \
  -H "Feature-Flag-open-search: true" \
  -d '{
    "createCollection": {
      "name": "articles_dynamic",
      "options": {
        "openSearch": {
          "enabled": true,
          "mappings": {
            "title": { "type": "text" }
          }
        }
      }
    }
  }'
```
