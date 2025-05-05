package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlColumn;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;

public interface ColumnFactoryFromCql {

  ApiColumnDef create(ColumnMetadata columnMetadata, VectorConfig vectorConfig)
      throws UnsupportedCqlColumn;

  ApiColumnDef createUnsupported(ColumnMetadata columnMetadata);
}
