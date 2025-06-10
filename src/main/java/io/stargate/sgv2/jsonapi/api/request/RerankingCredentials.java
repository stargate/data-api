package io.stargate.sgv2.jsonapi.api.request;

import java.util.Optional;

/**
 * This is the RerankingCredentials record that holds the API key for the reranking. String will be
 * resolved from the request header 'reranking-api-key', if it is not present, then we will use the
 * cassandra token as the reranking api key. Note, both cassandra token and reranking-api-key could
 * be absent in Data API request, although it is invalid for authentication.
 */
public record RerankingCredentials(String tenantId, Optional<String> apiKey) {}
