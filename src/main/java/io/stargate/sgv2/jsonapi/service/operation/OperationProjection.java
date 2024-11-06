package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;

/**
 * Interface for applying a command projection to a row read from the database to produce a document
 * for the command result.
 *
 * <p>NOTE: This was added for use by Tables, is not part of the Collection path (yet)
 */
public interface OperationProjection {

  /**
   * Called to apply the projection to the row and build the document for the result.
   *
   * <p>Implementations do not need to cache the results, callers should do that if they expect to
   * call this method multiple times.
   *
   * <p>Implementations should use the {@link
   * io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry} to map the
   * columns in the row to the fields in the document.
   *
   * @param row the {@link Row} to be projected.
   * @return {@link JsonNode} representing the projected row.
   */
  JsonNode projectRow(Row row);

  /**
   * Called to get the schema description of the projection, used in the read result for the {@link
   * io.stargate.sgv2.jsonapi.api.model.command.CommandStatus#PROJECTION_SCHEMA}
   *
   * @return {@link ColumnsDescContainer} representing the schema description of the projection.
   */
  ColumnsDescContainer getSchemaDescription();
}
