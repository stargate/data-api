package io.stargate.sgv2.jsonapi.exception.checked;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import java.util.Objects;

public class UnsupportedUserColumn extends CheckedApiException {

  private final ColumnDesc type;

  public UnsupportedUserColumn(String name, ColumnDesc type) {
    this(name, type, null);
  }

  public UnsupportedUserColumn(String name, ColumnDesc type, UnsupportedUserType cause) {
    super(
        String.format(
            "Unsupported column type: %s for column: %s",
            Objects.requireNonNull(type, "type must not be null").toString(), name));
    this.type = type;
  }

  public ColumnDesc getType() {
    return type;
  }
}
