package io.stargate.sgv2.jsonapi.exception.checked;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.TypeBindingPoint;
import java.util.Objects;

public class UnsupportedUserType extends CheckedApiException {

  public final TypeBindingPoint bindingPoint;
  public final ColumnDesc columnDesc;
  public final SchemaException schemaException;

  public UnsupportedUserType(
      TypeBindingPoint bindingPoint, ColumnDesc columnDesc, UnsupportedUserType cause) {
    this(bindingPoint, columnDesc, cause.schemaException);
  }

  public UnsupportedUserType(
      TypeBindingPoint bindingPoint, ColumnDesc columnDesc, SchemaException schemaException) {
    super(msg(bindingPoint, columnDesc), schemaException);
    this.columnDesc = columnDesc;
    this.bindingPoint = bindingPoint;
    this.schemaException = schemaException;
  }

  private static String msg(TypeBindingPoint bindingPoint, ColumnDesc columnDesc) {
    Objects.requireNonNull(columnDesc, "columnDesc must not be null");
    Objects.requireNonNull(bindingPoint, "bindingPoint must not be null");

    return String.format(
        "Unsupported user datatype definition columnDesc.typeName: %s at bindingPoint: %s ",
        columnDesc.typeName(), bindingPoint);
  }
}
