package io.stargate.sgv2.jsonapi.exception.checked;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Checked exception thrown when we cannot find a codec for a column that matches the types we are
 * using.
 *
 * <p>Not intended to be returned on the API, usage of the JSONCodec's should catch this and turn it
 * into the appropriate API error.
 */
public class MissingJSONCodecException extends CheckedApiException {

  // TODO: both javaType and value may be null when going toJSON
  public final TableMetadata table;
  public final CqlIdentifier columnName;
  public final DataType cqlType;
  public final Class<?> javaType;
  public final Object value;

  public MissingJSONCodecException(
      TableMetadata table,
      CqlIdentifier columnName,
      DataType cqlType,
      Class<?> javaType,
      Object value) {
    super(
        String.format(
            "No JSONCodec found for table '%s' column '%s' column type %s with java type %s and value %s",
            table.getName(), columnName.toString(), cqlType, javaType, value));
    this.table = table;
    this.columnName = columnName;
    this.cqlType = cqlType;
    this.javaType = javaType;
    this.value = value;
  }
}
