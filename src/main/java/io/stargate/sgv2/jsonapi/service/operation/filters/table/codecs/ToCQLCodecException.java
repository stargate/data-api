package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.DataType;

public class ToCQLCodecException extends Exception {

  public final Object value;
  public final DataType targetCQLType;

  /**
   * TODO: confirm we want / need this, the idea is to encapsulate any exception when doing the
   * conversion to to the type CQL expects. This would be a checked exception, and not something we
   * expect to return to the user
   *
   * @param value
   * @param targetCQLType
   * @param cause
   */
  public ToCQLCodecException(Object value, DataType targetCQLType, Exception cause) {
    super("Error trying to convert value " + value + " to " + targetCQLType, cause);
    this.value = value;
    this.targetCQLType = targetCQLType;
  }
}
