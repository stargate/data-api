package io.stargate.sgv2.jsonapi.service.schema.collections;

import io.stargate.sgv2.jsonapi.service.schema.versioning.CollectionSchemaVersion;

/**
 * schema_version 1 sample: {"collection":{"name":"newVectorize","schema_version":1,
 * "options":{"indexing":{"deny":["heh"]}, "defaultId":{"type":"objectId"}},
 * "vector":{"dimension":1024,"metric":"cosine","service":{"provider":"nvidia","modelName":"query","authentication":{"type":["HEADER"]},
 * "parameters":{"projectId":"test project"}}} }, "lexical":{"enabled":true,"analyzer":"standard"},
 * "rerank":{"enabled":true,"provider":"nvidia","modelName":"nvidia/llama-3.2-nv-rerankqa-1b-v2"}, }
 */
public class CollectionSettingsV2Reader extends CollectionSettingsV1Reader {

  @Override
  protected CollectionSchemaVersion decideSchemaVersion(
      CollectionLexicalDef persistedLexical, CollectionRerankDef persistedRerank) {
    return CollectionSchemaVersion.V_2;
  }
}
