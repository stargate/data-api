package io.stargate.sgv2.jsonapi.service.schema.versioning;


/**
 * The canonical record of the versions of the collection schema.
 *
 * <p>Use {@link #CURRENT_VERSION} to get the current version. {#link #ordinalValue()} is used to
 * actually compare if a version comes before or after another
 */
public enum CollectionSchemaVersion implements SchemaVersion {

  // Table comment == null || comment.isBlank()
  V_minus(-1),

  // we had table comment BUT only "indexing" see CollectionSettingsV0Reader
  V_0(0),

  // we had table comment, and it was structured with a version number, see
  // CollectionSettingsV1Reader, but we dont have lexical / rerank
  V_1(1),

  // version 1 + we added lexical and reranking config
  // NOTE: when we first put lexical and reranking into the table comment, we did NOT bump the
  // version from 1 to 2 so the CollectionSettingsV1Reader does some work to guess if it is v2
  // schema
  V_2(2);

  public static final CollectionSchemaVersion CURRENT_VERSION = V_2;

  private final int ordinalValue;

  CollectionSchemaVersion(int ordinalValue) {
    this.ordinalValue = ordinalValue;
  }

  @Override
  public int ordinalValue() {
    return ordinalValue;
  }

  @Override
  public String toString() {
    return String.valueOf(ordinalValue);
  }
}
