package io.stargate.sgv2.jsonapi.service.schema.collections;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;

/** Collection Id Type enum, UNDEFINED represents unwrapped id */
public enum CollectionIdType {
  OBJECT_ID,
  UUID,
  UUID_V6,
  UUID_V7,
  UNDEFINED;

  // TODO: store the name of the enum in the enum itself

  public static CollectionIdType fromString(String idType) {
    if (idType == null) return UNDEFINED;
    return switch (idType) {
      case "objectId" -> OBJECT_ID;
      case "uuid" -> UUID;
      case "uuidv6" -> UUID_V6;
      case "uuidv7" -> UUID_V7;
      case "" -> UNDEFINED;
      default -> throw ErrorCodeV1.INVALID_ID_TYPE.toApiException(idType);
    };
  }

  public String toString() {
    return switch (this) {
      case OBJECT_ID -> "objectId";
      case UUID -> "uuid";
      case UUID_V6 -> "uuidv6";
      case UUID_V7 -> "uuidv7";
      case UNDEFINED -> "";
    };
  }
}
