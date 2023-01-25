package io.stargate.sgv3.docsapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * UpdateOperation represents a unit of data to be updated. A update clause can have list of *
 * UpdateOperation.
 *
 * @param path
 * @param operator
 * @param value
 */
public record UpdateOperation(String path, UpdateOperator operator, JsonNode value) {}
