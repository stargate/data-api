package io.stargate.sgv2.jsonapi.api.model.schema;

import java.util.List;
import org.eclipse.microprofile.openapi.OASFactory;
import org.eclipse.microprofile.openapi.models.media.Schema;

/**
 * A wire schema shared by OpenAPI and MCP. Link a provider from the owning type or field with
 * {@code @Schema(implementation = ..., ref = ...)} and give the provider a unique
 * {@code @Schema(name = ...)}. The reference prevents OpenAPI from also scanning the internal Java
 * fields. Providers need a public no-argument constructor and {@code @RegisterForReflection} for
 * native MCP use.
 */
public interface ApiSchemaProvider {
  Schema schema();

  static Schema type(Schema.SchemaType... types) {
    return OASFactory.createSchema().type(List.of(types));
  }

  static ApiSchemaProvider from(org.eclipse.microprofile.openapi.annotations.media.Schema schema) {
    if (schema == null || !ApiSchemaProvider.class.isAssignableFrom(schema.implementation())) {
      return null;
    }
    return create(schema.implementation().asSubclass(ApiSchemaProvider.class));
  }

  static ApiSchemaProvider create(Class<? extends ApiSchemaProvider> providerClass) {
    try {
      return providerClass.getConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Cannot create API schema provider " + providerClass.getName(), e);
    }
  }
}
