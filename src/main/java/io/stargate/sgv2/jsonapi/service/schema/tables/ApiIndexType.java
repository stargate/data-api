package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Defines what type of index configuration (vector or regular) */
public enum ApiIndexType {
  // map, set, list
  COLLECTION("collection"),
  // Something on a scalar column, and non analysed text index
  REGULAR("regular"),
  // Not available yet
  TEXT_ANALYSED("text-analysed"),
  // on a vector
  VECTOR("vector");

  private final String indexTypeName;

  private static final List<String> ALL_TYPE_NAMES =
      Arrays.stream(values()).map(ApiIndexType::indexTypeName).toList();
  private static final Map<String, ApiIndexType> BY_API_NAME = new HashMap<>();

  static {
    for (ApiIndexType type : ApiIndexType.values()) {
      BY_API_NAME.put(type.indexTypeName, type);
    }
  }

  ApiIndexType(String indexTypeName) {
    this.indexTypeName = indexTypeName;
  }

  public String indexTypeName() {
    return indexTypeName;
  }

  public static List<String> allTypeNames() {
    return ALL_TYPE_NAMES;
  }

  /**
   * Get the ApiIndexType from the index type name. If the type name is not known, then null is
   * returned.
   */
  public static ApiIndexType fromTypeName(String typeName) {
    return BY_API_NAME.get(typeName);
  }

  static ApiIndexType fromCql(
      ApiColumnDef apiColumnDef, CQLSAIIndex.IndexTarget indexTarget, IndexMetadata indexMetadata)
      throws UnsupportedCqlIndexException {
    // If cqlIndexType is values, and the column is a scalar
    // then it is a regular index on primitive types
    if (indexTarget.cqlIndexType() == CQLSAIIndex.CqlIndexType.VALUES
        && apiColumnDef.type().isPrimitive()) {
      return REGULAR;
    }

    // if the target column is a vector, it can only be a vector index
    // we will let building the index check the options.
    // NOTE: check this before the container check, as a vector is a container
    if (apiColumnDef.type().typeName() == ApiTypeName.VECTOR) {
      return VECTOR;
    }

    // if the target column is a collection, it can only be a collection index
    // we will let building collection index to check if the function is supported.
    if (apiColumnDef.type().isContainer()) {
      return COLLECTION;
    }

    // we do not support text analysed indexes yet
    throw new UnsupportedCqlIndexException(
        "Unsupported index:%s on field: %s".formatted(indexMetadata.getName(), apiColumnDef.name()),
        indexMetadata);
  }
}
