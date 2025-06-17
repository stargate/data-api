package io.stargate.sgv2.jsonapi.api.request;

import java.util.Optional;

/**
 * EmbeddingCredentials is a record that holds the embedding provider credentials for the embedding
 * service passed as header.
 *
 * <p>Includes the tenantID, so we can fully identify the usage when creating the {@link
 * io.stargate.sgv2.jsonapi.service.provider.ModelUsage}
 *
 * @param tenantId - Tenant Id that called the API.
 * @param apiKey - API token for the embedding service
 * @param accessId - Access Id used for AWS Bedrock embedding service
 * @param secretId - Secret Id used for AWS Bedrock embedding service
 */
public record EmbeddingCredentials(
    String tenantId,
    Optional<String> apiKey,
    Optional<String> accessId,
    Optional<String> secretId) {}
