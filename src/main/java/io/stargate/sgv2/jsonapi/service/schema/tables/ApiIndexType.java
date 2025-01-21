package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Defines what type of index configuration (vector or regular) */
public enum ApiIndexType {
  // Something on a scalar column, or collection, and non analysed text index
  REGULAR("regular"),
  // Not available yet
  TEXT_ANALYSED("textAnalysed"),
  // on a vector
  VECTOR("vector");

  private final String apiName;

  private static final Map<String, ApiIndexType> BY_API_NAME;

  static {
    var map = new HashMap<String, ApiIndexType>();
    for (ApiIndexType type : ApiIndexType.values()) {
      map.put(type.apiName, type);
    }
    BY_API_NAME = Map.copyOf(map);
  }

  ApiIndexType(String apiName) {
    this.apiName = Objects.requireNonNull(apiName, "apiName must not be null");
  }

  /**
   * Gets the name to use when identifying this type of index in the API.
   *
   * @return
   */
  public String apiName() {
    return apiName;
  }

  /**
   * Get the ApiIndexType by the API name.
   *
   * @return The ApiIndexType or null if not found
   */
  public static ApiIndexType fromApiName(String apiName) {
    return BY_API_NAME.get(apiName);
  }

  static ApiIndexType fromCql(
      ApiColumnDef apiColumnDef, CQLSAIIndex.IndexTarget indexTarget, IndexMetadata indexMetadata)
      throws UnsupportedCqlIndexException {

    // If there is no function on the indexTarget, and the column is a scalar, then it is a regular
    // index on
    // an int, text, etc
    if (indexTarget.indexFunction() == null && apiColumnDef.type().isPrimitive()) {
      return REGULAR;
    }

    // if the target column is a collection, it can only be a collection index, we will let building
    // the collection index to check if the function is supported.
    if (apiColumnDef.type().isContainer()) {
      return REGULAR;
    }

    // if the target column is a vector, it can only be a vector index, we will let building the
    // index check the options.
    // NOTE: check this before the container check, as a vector is a container
    if (apiColumnDef.type().typeName() == ApiTypeName.VECTOR) {
      return VECTOR;
    }

    // we do not support text analysed indexes yet
    throw new UnsupportedCqlIndexException(
        "Unsuported index:%s on field: %s".formatted(indexMetadata.getName(), apiColumnDef.name()),
        indexMetadata);
  }
}
