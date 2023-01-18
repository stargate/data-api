package io.stargate.sgv3.docsapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

/**
 * Represents a document read from the database
 *
 * @param id Document Id identifying the document
 * @param txnId Unique UUID resenting point in time of a document, used for LWT transactions
 * @param document JsonNode representation of the document
 */
public record ReadDocument(String id, UUID txnId, JsonNode document) {}
