package io.stargate.sgv3.docsapi.service.schema.configuration;

import io.stargate.bridge.proto.Schema;
import javax.inject.Singleton;

// TODO remove after https://github.com/stargate/stargate/issues/2195

/** Defines all needed properties for schema access. */
public class SchemaAccessConfiguration {

  @Singleton
  Schema.SchemaRead.SourceApi sourceApi() {
    return Schema.SchemaRead.SourceApi.REST;
  }
}
