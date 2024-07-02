package io.stargate.sgv2.jsonapi.api.request;

import java.util.Optional;

public record EmbeddingCredential(
    Optional<String> apiKey, Optional<String> accessId, Optional<String> secretId) {}
