package io.stargate.sgv2.jsonapi.service.schema.collections;

import io.stargate.sgv2.jsonapi.exception.ServerException;
import java.util.HashMap;
import java.util.Map;

/** Collection Id Type enum, UNDEFINED represents unwrapped id */
public enum CollectionIdType {
  OBJECT_ID("objectId"),
  UUID("uuid"),
  UUID_V6("uuidv6"),
  UUID_V7("uuidv7"),
  UNDEFINED("");

  private static final Map<String, CollectionIdType> BY_API_NAME = new HashMap<>();

  static {
    for (CollectionIdType type : values()) {
      BY_API_NAME.put(type.apiName, type);
    }
  }

  private final String apiName;

  CollectionIdType(String apiName) {
    this.apiName = apiName;
  }

  public static CollectionIdType fromString(String idType) {
    if (idType == null) return UNDEFINED;
    CollectionIdType type = BY_API_NAME.get(idType);
    if (type == null) {
      throw ServerException.internalServerError(
          "Invalid Collection _id type: '%s'".formatted(idType));
    }
    return type;
  }

  public String toString() {
    return apiName;
  }
}
