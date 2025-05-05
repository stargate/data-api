package io.stargate.sgv2.jsonapi.service.schema.tables;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserColumn;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;

public interface ColumnFactoryFromColumnDesc {

  ApiColumnDef create(
      String fieldName, ColumnDesc columnDesc, VectorizeConfigValidator validateVectorize)
      throws UnsupportedUserColumn;

  ApiColumnDef createUnsupported(String fieldName, ColumnDesc columnDesc);
}
