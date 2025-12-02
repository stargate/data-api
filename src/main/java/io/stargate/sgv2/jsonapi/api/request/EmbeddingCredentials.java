package io.stargate.sgv2.jsonapi.api.request;

import java.util.Optional;

/**
 * EmbeddingCredentials is a record that holds the embedding provider credentials for the embedding
 * service passed as header.
 *
 * @param apiKey - API token for the embedding service
 * @param accessId - Access Id used for AWS Bedrock embedding service
 * @param secretId - Secret Id used for AWS Bedrock embedding service
 */
public record EmbeddingCredentials(
    Optional<String> apiKey, Optional<String> accessId, Optional<String> secretId) {}
