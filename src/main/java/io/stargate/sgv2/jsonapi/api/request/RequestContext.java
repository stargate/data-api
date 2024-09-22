package io.stargate.sgv2.jsonapi.api.request;

import java.util.Optional;

public record RequestContext(Optional<String> tenantId, Optional<String> authToken) {}
