package io.stargate.sgv2.jsonapi.api.request;

import java.util.Optional;

/**
 * This is the RerankCredentials record that holds the Astra token and the API key for the rerank.
 * It will be resolved from {@link DataApiRequestInfo}.
 *
 * <p>Currently, the only rerank provider we have is self-hosted Nvidia model in GPU plane. And it
 * uses astraToken as the authentication token. So apiKey is not required from user's Data API
 * request.
 */
public record RerankCredentials(Optional<String> astraToken, Optional<String> apiKey) {}
