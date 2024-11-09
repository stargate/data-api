package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.IndexDesc;

/**
 * The API definition of an Index, is an interface so the unsupported indexs can be represented as
 * well.
 */
public interface ApiIndexDef {

  CqlIdentifier indexName();

  CqlIdentifier targetColumn();

  ApiIndexType indexType();

  IndexDesc indexDesc();

  default boolean isUnsupported() {
    return false;
  }
}
