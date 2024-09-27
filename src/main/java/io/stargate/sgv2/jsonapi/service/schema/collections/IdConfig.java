package io.stargate.sgv2.jsonapi.service.schema.collections;

public record IdConfig(CollectionIdType idType) {
  public static IdConfig defaultIdConfig() {
    return new IdConfig(CollectionIdType.UNDEFINED);
  }
}
