package io.stargate.sgv2.jsonapi.exception.checked;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToMessageString;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import java.util.Objects;

/**
 * Thrown when we encounter a Cql column definition for an existing CQL table that has a type that
 * we cannot represent internally.
 *
 * <p>This exception is for when we have the {@link ColumnMetadata} and is preferable to the {@link
 * UnsupportedCqlType} exception which is for when we only have the type.
 */
public class UnsupportedCqlColumn extends CheckedApiException {

  private final CqlIdentifier column;
  private final DataType type;

  public UnsupportedCqlColumn(ColumnMetadata columnMetadata, UnsupportedCqlType cause) {
    this(
        Objects.requireNonNull(columnMetadata, "columnMetadata must not be null").getName(),
        Objects.requireNonNull(columnMetadata, "columnMetadata must not be null").getType(),
        cause);
  }

  public UnsupportedCqlColumn(CqlIdentifier column, DataType type, UnsupportedCqlType cause) {
    super(
        String.format(
            "Unsupported column type: %s for column: %s",
            Objects.requireNonNull(type, "type must not be null").asCql(true, true),
            cqlIdentifierToMessageString(
                Objects.requireNonNull(column, "column must not be null"))));
    this.column = column;
    this.type = type;
  }

  public CqlIdentifier getColumn() {
    return column;
  }

  public DataType getType() {
    return type;
  }
}
