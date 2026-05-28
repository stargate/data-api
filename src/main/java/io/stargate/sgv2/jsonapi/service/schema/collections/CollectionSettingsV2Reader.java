package io.stargate.sgv2.jsonapi.service.schema.collections;

import io.stargate.sgv2.jsonapi.service.schema.CollectionSchemaVersion;

/**
 * A reader when we know the schema version is V_2. This simply extends the V1 reader to make the
 * decision on the version. This is because we did not increase schema version from 1 to 2 when we
 * added lexical and reranking support. See {@link CollectionSchemaVersion} for more details.
 */
public class CollectionSettingsV2Reader extends CollectionSettingsV1Reader {

  @Override
  protected CollectionSchemaVersion decideSchemaVersion(
      CollectionLexicalDef persistedLexical, CollectionRerankDef persistedRerank) {
    return CollectionSchemaVersion.V_2;
  }
}
