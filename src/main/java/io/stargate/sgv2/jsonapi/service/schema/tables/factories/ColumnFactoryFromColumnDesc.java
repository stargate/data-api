package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserColumn;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.TypeBindingPoint;

public interface ColumnFactoryFromColumnDesc {

  ApiColumnDef create(
      TypeBindingPoint bindingPoint,
      String fieldName,
      ColumnDesc columnDesc,
      VectorizeConfigValidator validateVectorize)
      throws UnsupportedUserColumn;

  ApiColumnDef createUnsupported(String fieldName, ColumnDesc columnDesc);
}
