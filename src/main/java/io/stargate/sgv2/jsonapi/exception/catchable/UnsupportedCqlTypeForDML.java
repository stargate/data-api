package io.stargate.sgv2.jsonapi.exception.catchable;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToStringForUser;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import java.util.Objects;

/**
 * Thrown when we encounter a Cql column definition for an existing CQL table that has a type that
 * we cannot represent internally.
 *
 * <p>This probably happens very rarely, an example would be a UDT though
 */
public class UnsupportedCqlTypeForDML extends CheckedApiException {

  private final CqlIdentifier column;
  private final DataType type;

  public UnsupportedCqlTypeForDML(ColumnMetadata columnMetadata) {
    this(
        Objects.requireNonNull(columnMetadata, "columnMetadata must not be null").getName(),
        Objects.requireNonNull(columnMetadata, "columnMetadata must not be null").getType());
  }

  public UnsupportedCqlTypeForDML(CqlIdentifier column, DataType type) {
    super(
        String.format(
            "Unsupported column type: %s for column: %s",
            type, cqlIdentifierToStringForUser(column)));
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
