package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.IndexDesc;
import java.util.Map;

public class UnsupportedUserIndex extends UnsupportedIndex {

  UnsupportedUserIndex(CqlIdentifier indexName, Map<String, String> options) {
    super(indexName, options);
  }

  @Override
  public IndexDesc indexDesc() {
    // aaron - 9-nov-2024 - for now we cannot list indexes that we do not support from user desc so
    // just throwring an exception
    throw new UnsupportedOperationException("Unsupported index does not have indexDesc");
  }
}
