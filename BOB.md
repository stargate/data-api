# Stargate Data API - Project Analysis Summary

## Project Overview

**Stargate Data API** is a standalone HTTP microservice that provides a JSON Document-based interface for accessing data stored in Apache Cassandra clusters. Built with Quarkus 3.29.2 and Java 21, it targets JavaScript developers who interact through client libraries like stargate-mongoose.

## Core Technology Stack

- **Framework**: Quarkus 3.29.2
- **Language**: Java 21
- **Build Tool**: Maven
- **Backend Databases**: 
  - DataStax Enterprise (DSE) 6.9.15
  - HyperConverged Database (HCD) 1.2.3
  - Apache Cassandra with Storage Attached Index (SAI)
- **Driver**: Cassandra Java Driver 4.17.0 + custom QueryBuilder 4.19.0-preview1
- **Container**: Docker with native image support

## Key Features

1. **JSON Document API**: MongoDB-like API for document operations on Cassandra
2. **Vector Search**: Embeddings up to 4096 dimensions with cosine, euclidean, dot_product metrics
3. **Embedding Providers**: OpenAI, AWS Bedrock, NVIDIA, Mistral integration
4. **Lexical Search**: Full-text search with configurable analyzers
5. **Reranking**: Document reranking with NVIDIA models
6. **Tables API**: Structured table operations alongside collections
7. **MCP Server**: Model Context Protocol integration (Quarkus MCP 1.7.1)

## Architecture Components

### API Layer (`api/`)
- REST endpoints with OpenAPI/Swagger documentation
- Security, authentication, and tenant management
- Request validation and token handling

### Service Layer (`service/`)
- **Operations**: CRUD for collections and tables
- **Embedding**: Server-side vectorization
- **Reranking**: Document reranking services
- **Schema**: Collection and table schema management
- **Shredding**: Document decomposition for Cassandra storage
- **CQL**: Query building and execution

### Configuration (`config/`)
- Document limits: 4MB size, 16 levels depth, 2000 properties
- Database limits: 5 collections per database
- Operation limits: 20 documents per insert/update/delete
- Vectorization and feature flags

## Supported Commands

**Collections**: `find`, `findOne`, `insertOne`, `insertMany`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`, `findOneAndUpdate`, `findOneAndReplace`, `findOneAndDelete`, `countDocuments`, `estimatedDocumentCount`, `createCollection`, `findCollections`, `deleteCollection`

**Tables**: `createTable`, `dropTable`, `alterTable`, `listTables`, `createIndex`, `createVectorIndex`, `createTextIndex`, `dropIndex`, `listIndexes`, `createType`, `dropType`, `alterType`, `listTypes`

**Keyspaces**: `createKeyspace`, `findKeyspaces`, `dropKeyspace`

## Document Model

- **Field Names**: `[a-zA-Z0-9_-]+` pattern (except reserved `_id`)
- **Paths**: Dotted notation (e.g., `address.suburb`, `tags.0`)
- **Filter Operators**: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$not`, `$and`, `$or`, `$nor`, `$all`, `$elemMatch`, `$size`
- **Array Support**: Zero-based indexing with array-specific operators

## Deployment Options

1. **Docker Compose**: Quick start with `./start_hcd.sh` or `./start_dse69.sh`
2. **Kubernetes**: Helm charts in `helm/jsonapi/`
3. **Native Executable**: GraalVM support
4. **Dev Mode**: `./mvnw compile quarkus:dev`

## Testing Infrastructure

- **Unit Tests**: JUnit 5 + Mockito (5.20.0)
- **Integration Tests**: Testcontainers with DSE/HCD
- **Profiles**: `dse69-it`, `hcd-it`
- **Performance**: NoSQLBench integration

## Monitoring & Observability

- **Metrics**: Micrometer + Prometheus
- **Health**: SmallRye Health at `/stargate/health`
- **Logging**: JSON logging, command-level logging
- **API Docs**: Swagger UI at `/swagger-ui/`

## Key Configuration Defaults

- Default page size: 20 documents
- Max in-memory sort: 10,000 documents
- Session cache: 300s TTL, max 50 sessions
- LWT retries: 3 attempts
- Max vector dimensions: 4096 floats
- Max string length: 8000 bytes
- Max array length: 1000 elements

## Notable Design Decisions

1. **Non-REST Design**: Optimized for machine-generated queries from ODMs
2. **Failure Modes**: "Fail Fast" vs "Fail Silently" for multi-document operations
3. **Optimistic Locking**: Compare-and-set for concurrent updates
4. **Document Shredding**: Custom decomposition for Cassandra storage
5. **Server-Side Vectorization**: Optional embedding generation

## Project Structure

- `src/main/java/io/stargate/sgv2/jsonapi/` - Core application
- `src/test/java/` - Comprehensive test suite
- `docs/` - API specifications (dataapi-spec.md, dataapi-network-spec.md)
- `docker-compose/` - Local deployment scripts
- `helm/` - Kubernetes deployment
- `lib/` - Custom Java driver repository

## Quick Start

### Running with Docker Compose

```bash
cd docker-compose
./start_hcd.sh    # For HCD
# or
./start_dse69.sh  # For DSE 6.9
```

### Running in Development Mode

```bash
# Start backend database first
cd docker-compose
./start_hcd.sh -d

# Then start Data API in dev mode
./mvnw compile quarkus:dev -Dstargate.jsonapi.operations.vectorize-enabled=true \
  -Dstargate.jsonapi.operations.database-config.local-datacenter=dc1
```

### Building Docker Image

```bash
./mvnw clean package -Dquarkus.container-image.build=true -DskipTests
```

### Running Tests

```bash
./mvnw verify              # All tests
./mvnw verify -DskipITs    # Skip integration tests
./mvnw verify -DskipUnitTests  # Only integration tests
```

## API Access

Once running, access:
- **Swagger UI**: http://localhost:8181/swagger-ui/
- **Health Check**: http://localhost:8181/stargate/health
- **Metrics**: http://localhost:8181/q/metrics

## Authentication

Token format for Cassandra backend:
```
Token: Cassandra:Base64(username):Base64(password)
```

Example with default credentials (cassandra/cassandra):
```
Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh
```

## Additional Resources

- [Configuration Guide](CONFIGURATION.md)
- [Data API Specification](docs/dataapi-spec.md)
- [Network Specification](docs/dataapi-network-spec.md)
- [Docker Compose README](docker-compose/README.md)

---

This is a production-ready microservice bridging document-oriented applications with Cassandra's distributed capabilities, with strong support for modern AI/ML workloads through vector search and embedding integration.