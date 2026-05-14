package io.stargate.sgv2.jsonapi.service.schema.versioning;

import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;

public class VersionedSchema {

  private final LexicalDefSchemaValueDef lexicalDefSchemaValueDef;
  private final RerankDefSchemaValueDef rerankDefSchemaValueDef;

  public VersionedSchema(ApiFeatures apiFeatures) {

    this.lexicalDefSchemaValueDef =
        new LexicalDefSchemaValueDef(!apiFeatures.isFeatureEnabled(ApiFeature.LEXICAL));
    this.rerankDefSchemaValueDef =
        new RerankDefSchemaValueDef(!apiFeatures.isFeatureEnabled(ApiFeature.RERANKING));
  }

  public LexicalDefSchemaValueDef lexicalDef() {
    return lexicalDefSchemaValueDef;
  }

  public RerankDefSchemaValueDef rerankDef() {
    return rerankDefSchemaValueDef;
  }
}
