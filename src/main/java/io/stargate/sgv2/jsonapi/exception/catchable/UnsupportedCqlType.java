package io.stargate.sgv2.jsonapi.exception.catchable;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

/** Thrown when we encounter a Cql column definition that is not supported by the API. */
public class UnsupportedCqlType extends CatchableApiException {

  private final ColumnMetadata columnMetadata;

  public UnsupportedCqlType(ColumnMetadata columnMetadata) {
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
