package io.stargate.sgv2.jsonapi.service.schema.versioning;

public enum SchemaVersion {

  // Table comment == null || comment.isBlank()
  V_minus(-1),

  // we had table comment BUT only "indexing" see CollectionSettingsV0Reader
  V_0(0),

  // we had table comment, and it was structured with a version number, see
  // CollectionSettingsV1Reader, but we dont have lexical / rerank
  V_1(1),

  // version 1 + we added lexical and reranking config
  V_2(2);

  public static final SchemaVersion CURRENT_VERSION = V_2;

  private final int ordinalValue;

  SchemaVersion(int ordinalValue) {
    this.ordinalValue = ordinalValue;
  }

  public int ordinalValue() {
    return ordinalValue;
  }

  @Override
  public String toString() {
    return String.valueOf(ordinalValue);
  }
}
