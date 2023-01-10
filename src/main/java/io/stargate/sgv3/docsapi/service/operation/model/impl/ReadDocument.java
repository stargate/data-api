package io.stargate.sgv3.docsapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Optional;
import java.util.UUID;

public record ReadDocument(String id, Optional<UUID> txnId, JsonNode document) {}
