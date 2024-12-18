package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;

/** Defines what type of index configuration (vector or regular) */
public enum ApiIndexType {
  // map, set, list
  COLLECTION,
  // Something on a scalar column, and non analysed text index
  REGULAR,
  // Not available yet
  TEXT_ANALYSED,
  // on a vector
  VECTOR;

  static ApiIndexType fromCql(
      ApiColumnDef apiColumnDef, CQLSAIIndex.IndexTarget indexTarget, IndexMetadata indexMetadata)
      throws UnsupportedCqlIndexException {

    // If there is no function on the indexTarget, and the column is a scalar, then it is a regular
    // index on
    // an int, text, etc
    if (indexTarget.indexFunction() == null && apiColumnDef.type().isPrimitive()) {
      return REGULAR;
    }

    // if the target column is a vector, it can only be a vector index, we will let building the
    // index check the options.
    // NOTE: check this before the container check, as a vector is a container
    if (apiColumnDef.type().typeName() == ApiTypeName.VECTOR) {
      return VECTOR;
    }

    // if the target column is a collection, it can only be a collection index, we will let building
    // the
    // collection index to check if the function is supported.
    if (apiColumnDef.type().isContainer()) {
      return COLLECTION;
    }

    // we do not support text analysed indexes yet
    throw new UnsupportedCqlIndexException(
        "Unsuported index:%s on field: %s".formatted(indexMetadata.getName(), apiColumnDef.name()),
        indexMetadata);
  }
}
