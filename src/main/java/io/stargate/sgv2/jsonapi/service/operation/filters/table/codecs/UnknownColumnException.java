package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Exception thrown when we cannot find a column in a table. Used in the operations, which expect
 * that a column with a name should be available in the table.
 *
 * <p>Not intended to be returned on the API, usage of the JSONCodec's should catch this and turn it
 * into the appropriate API error.
 */
public class UnknownColumnException extends RuntimeException {

  public final TableMetadata table;
  public final CqlIdentifier column;

  public UnknownColumnException(TableMetadata table, CqlIdentifier column) {
    super(
        String.format(
            "No column found for table %s with name %s", table.getName(), column.asInternal()));
    this.table = table;
    this.column = column;
  }
}
