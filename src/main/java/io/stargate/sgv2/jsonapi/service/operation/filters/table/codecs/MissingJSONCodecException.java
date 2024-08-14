package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Checked exception thrown when we cannot find a codec for a column that matches the types we are
 * using.
 *
 * <p>Not intended to be returned on the API, usage of the JSONCodec's should catch this and turn it
 * into the appropriate API error.
 */
public class MissingJSONCodecException extends Exception {

  // TODO: both javaType and value may be null when going toJSON
  public final TableMetadata table;
  public final ColumnMetadata column;
  public final Class<?> javaType;
  public final Object value;

  public MissingJSONCodecException(
      TableMetadata table, ColumnMetadata column, Class<?> javaType, Object value) {
    super(
        String.format(
            "No JSONCodec found for table '%s' column '%s' column type %s with java type %s and value %s",
            table.getName(), column.getName(), column.getType(), javaType, value));
    this.table = table;
    this.column = column;
    this.javaType = javaType;
    this.value = value;
  }
}
