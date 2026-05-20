package io.stargate.sgv2.jsonapi.service.schema.versioning;

import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalDefSchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDefSchemaFactory;

/**
 * Container for the {@link SchemaFactory} 's that are configured with the APIFeatures instance.
 *
 * <p><b>NOTE:</b> Most queries will not need to access schema factories if they get their schema
 * objects directly from cache, so we do not always need an instance of this class. It is lazy
 * created by the {@link io.stargate.sgv2.jsonapi.api.request.RequestContext} and would be a waste
 * to make for every request (but APIFeatures are request scoped so kind of needed).
 */
public class VersionedSchema {

  private final CollectionLexicalDefSchemaFactory lexicalDefSchemaValueDef;
  private final CollectionRerankDefSchemaFactory rerankDefSchemaValueDef;

  public VersionedSchema(ApiFeatures apiFeatures) {

    this.lexicalDefSchemaValueDef =
        new CollectionLexicalDefSchemaFactory(!apiFeatures.isFeatureEnabled(ApiFeature.LEXICAL));
    this.rerankDefSchemaValueDef =
        new CollectionRerankDefSchemaFactory(!apiFeatures.isFeatureEnabled(ApiFeature.RERANKING));
  }

  public CollectionLexicalDefSchemaFactory lexicalDef() {
    return lexicalDefSchemaValueDef;
  }

  public CollectionRerankDefSchemaFactory rerankDef() {
    return rerankDefSchemaValueDef;
  }
}
