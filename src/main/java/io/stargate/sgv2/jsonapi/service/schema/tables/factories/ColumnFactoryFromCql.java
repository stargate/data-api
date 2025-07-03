package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlColumn;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.TypeBindingPoint;

public interface ColumnFactoryFromCql {

  ApiColumnDef create(
      TypeBindingPoint bindingPoint, ColumnMetadata columnMetadata, VectorConfig vectorConfig)
      throws UnsupportedCqlColumn;

  ApiColumnDef createUnsupported(ColumnMetadata columnMetadata);
}
