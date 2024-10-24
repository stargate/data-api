package io.stargate.sgv2.jsonapi.exception.checked;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import java.util.Objects;

/**
 * Thrown when we encounter a CQL {@link DataType} that cannot be represented as a {@link
 * ApiDataTypeDef}.
 *
 * <p>Prefer to use the @{@link UnsupportedCqlColumn} exception when you have the {@link
 * ColumnMetadata} available.
 */
public class UnsupportedCqlType extends CheckedApiException {

  private final DataType type;

  public UnsupportedCqlType(DataType type) {
    super(
        String.format(
            "Unsupported CQL datatype: %s",
            Objects.requireNonNull(type, "type must not be null").asCql(true, true)));
    this.type = type;
  }

  public DataType getType() {
    return type;
  }
}
