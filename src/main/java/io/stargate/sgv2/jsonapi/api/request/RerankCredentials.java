package io.stargate.sgv2.jsonapi.api.request;

import java.util.Optional;

/**
 * This is the RerankCredentials record that holds the token and the API key for the rerank. It will
 * be resolved from {@link DataApiRequestInfo}.
 */
public record RerankCredentials(String token, Optional<String> apiKey) {}
