package io.stargate.sgv2.jsonapi.exception.checked;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.TypeBindingPoint;
import java.util.Objects;

/**
 * Thrown when we encounter a CQL {@link DataType} that cannot be represented as a {@link
 * ApiDataTypeDef}.
 *
 * <p>Prefer to use the @{@link UnsupportedCqlColumn} exception when you have the {@link
 * ColumnMetadata} available.
 */
public class UnsupportedCqlType extends CheckedApiException {

  private final TypeBindingPoint bindingPoint;
  private final DataType cqlType;

  public UnsupportedCqlType(TypeBindingPoint bindingPoint, DataType cqlType) {
    this(bindingPoint, cqlType, null);
  }

  public UnsupportedCqlType(TypeBindingPoint bindingPoint, DataType cqlType, Throwable cause) {
    super(msg(bindingPoint, cqlType), cause);
    this.cqlType = cqlType;
    this.bindingPoint = bindingPoint;
  }

  private static String msg(TypeBindingPoint bindingPoint, DataType cqlType) {
    Objects.requireNonNull(cqlType, "cqlType must not be null");
    Objects.requireNonNull(bindingPoint, "bindingPoint must not be null");

    return String.format(
        "Unsupported CQL type usage cqlType: %s at bindingPoint: %s ",
        cqlType.asCql(true, true), bindingPoint);
  }
}
