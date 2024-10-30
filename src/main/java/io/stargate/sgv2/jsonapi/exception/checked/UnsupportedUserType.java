package io.stargate.sgv2.jsonapi.exception.checked;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import java.util.Objects;

public class UnsupportedUserType extends CheckedApiException {

  private final ColumnDesc type;

  public UnsupportedUserType(ColumnDesc type) {
    this(type, null);
  }

  public UnsupportedUserType(ColumnDesc type, Throwable cause) {
    super(
        String.format(
            "Unsupported user datatype definition : %s",
            Objects.requireNonNull(type, "type must not be null")),
        cause);
    this.type = type;
  }

  public ColumnDesc getType() {
    return type;
  }
}
