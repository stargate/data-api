package io.stargate.sgv2.jsonapi.service.schema;

import java.util.concurrent.CompletionStage;

public class SchemsObjectFactory implements SchemaObjectCache.SchemaObjectFactory {

  @Override
  public CompletionStage<SchemaObject> apply(SchemaObjectIdentifier identifier) {
    return null;
  }
}
