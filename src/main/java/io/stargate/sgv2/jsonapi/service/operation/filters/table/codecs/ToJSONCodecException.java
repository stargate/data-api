package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.DataType;

public class ToJSONCodecException extends Exception {

  public final Object value;
  public final DataType fromCqlType;

  /**
   * TODO: confirm we want / need this, the idea is to encapsulate any exception when doing the
   * conversion to to the type CQL expects. This would be a checked exception, and not something we
   * expect to return to the user
   *
   * @param value
   * @param fromCqlType
   * @param cause
   */
  public ToJSONCodecException(Object value, DataType fromCqlType, Exception cause) {
    super(
        "Error trying to convert value " + value + " from " + fromCqlType + " to JSONNode", cause);
    this.value = value;
    this.fromCqlType = fromCqlType;
  }
}
