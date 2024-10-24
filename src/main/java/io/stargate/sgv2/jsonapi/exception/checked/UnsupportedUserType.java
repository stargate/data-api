package io.stargate.sgv2.jsonapi.exception.checked;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import java.util.Objects;

public class UnsupportedUserType extends CheckedApiException {

  private final ColumnType type;

  public UnsupportedUserType(ColumnType type) {
    super(
        String.format(
            "Unsupported user datatype definition : %s",
            Objects.requireNonNull(type, "type must not be null")));
    this.type = type;
  }

  public ColumnType getType() {
    return type;
  }
}
