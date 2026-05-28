package io.stargate.sgv2.jsonapi.service.schema;

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
public class SchemaRegistry {

  private final CollectionLexicalDefSchemaFactory lexicalDefFactory;
  private final CollectionRerankDefSchemaFactory rerankDefFactory;

  public SchemaRegistry(ApiFeatures apiFeatures) {

    this.lexicalDefFactory =
        new CollectionLexicalDefSchemaFactory(!apiFeatures.isFeatureEnabled(ApiFeature.LEXICAL));
    this.rerankDefFactory =
        new CollectionRerankDefSchemaFactory(!apiFeatures.isFeatureEnabled(ApiFeature.RERANKING));
  }

  public CollectionLexicalDefSchemaFactory lexicalDef() {
    return lexicalDefFactory;
  }

  public CollectionRerankDefSchemaFactory rerankDef() {
    return rerankDefFactory;
  }
}
