package io.stargate.sgv2.jsonapi.api.request;

import java.util.Optional;

/**
 * This is the RerankingCredentials record that holds the token and the API key for the rerank. It
 * will be resolved from {@link DataApiRequestInfo}.
 */
public record RerankingCredentials(String token, Optional<String> apiKey) {}
