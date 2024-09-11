package io.stargate.sgv2.jsonapi.exception.catchable;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

/**
 * Thrown when we encounter a Cql column definition for an existing CQL table that has a type that
 * we cannot represent internally.
 *
 * <p>This probably happens very rarely, an example would be a UDT though
 */
public class UnsupportedCqlTypeForDML extends CheckedApiException {

  private final ColumnMetadata columnMetadata;

  public UnsupportedCqlTypeForDML(ColumnMetadata columnMetadata) {
    super(
        String.format(
            "Unsupported column type: %s for column: %s",
            columnMetadata.getType(), columnMetadata.getName()));
    this.columnMetadata = columnMetadata;
  }

  public ColumnMetadata columnMetadata() {
    return columnMetadata;
  }
}
