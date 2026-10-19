package io.stargate.sgv2.jsonapi.api.v1.mcp;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.CustomDefinition;
import com.github.victools.jsonschema.generator.CustomPropertyDefinition;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import io.quarkiverse.mcp.server.runtime.SchemaGeneratorConfigCustomizer;
import io.smallrye.openapi.model.OpenApiVersion;
import io.smallrye.openapi.runtime.io.IOContext;
import io.smallrye.openapi.runtime.io.JsonIO;
import io.smallrye.openapi.runtime.io.media.SchemaIO;
import io.stargate.sgv2.jsonapi.api.model.schema.ApiSchemaProvider;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Generates MCP schemas using Data API clause definitions and Jackson and OpenAPI annotations.
 * Custom deserializers and delegating creators cannot be described by reflecting their fields.
 *
 * <p>The generator's PLAIN_JSON preset does not read annotations. This customizer supplies field
 * names, ignored fields, descriptions, and schema overrides.
 */
@Singleton
public class McpSchemaCustomizer implements SchemaGeneratorConfigCustomizer {
  private final SchemaIO<JsonNode, ArrayNode, ObjectNode, ArrayNode, ObjectNode> schemaIO;
  private final Map<Class<? extends ApiSchemaProvider>, ObjectNode> schemas = new HashMap<>();

  public McpSchemaCustomizer() {
    var context =
        IOContext.forJson(
            JsonIO.<JsonNode, ArrayNode, ObjectNode, ArrayNode, ObjectNode>newInstance(null));
    // OpenAPI 3.1 uses the same JSON Schema dialect as the MCP generator.
    context.setOpenApiVersion(OpenApiVersion.V3_1);
    schemaIO = context.schemaIO();
  }

  @Override
  public void customize(SchemaGeneratorConfigBuilder builder) {
    builder
        .forFields()
        .withDescriptionResolver(
            field -> {
              var schema = field.getAnnotationConsideringFieldAndGetter(Schema.class);
              return schema != null && !schema.description().isEmpty()
                  ? schema.description()
                  : null;
            })
        .withIgnoreCheck(
            field -> {
              var ignore = field.getAnnotationConsideringFieldAndGetter(JsonIgnore.class);
              return ignore != null && ignore.value();
            })
        .withCustomDefinitionProvider(
            (field, context) -> {
              var schema = schemaNode(field.getAnnotationConsideringFieldAndGetter(Schema.class));
              return schema == null
                  ? null
                  : new CustomPropertyDefinition(schema, CustomDefinition.AttributeInclusion.YES);
            })
        .withPropertyNameOverrideResolver(
            field -> {
              var property = field.getAnnotationConsideringFieldAndGetter(JsonProperty.class);
              // Jackson uses an empty value for the Java name. Whitespace is an explicit JSON name.
              return property != null && !property.value().isEmpty() ? property.value() : null;
            });

    builder
        .forTypesInGeneral()
        .withDescriptionResolver(
            type -> {
              var schema = type.getType().getErasedType().getAnnotation(Schema.class);
              return schema != null && !schema.description().isEmpty()
                  ? schema.description()
                  : null;
            })
        .withCustomDefinitionProvider(
            (javaType, context) -> {
              var schema = schemaNode(javaType.getErasedType().getAnnotation(Schema.class));
              return schema == null
                  ? null
                  : new CustomDefinition(
                      schema,
                      CustomDefinition.DefinitionType.INLINE,
                      CustomDefinition.AttributeInclusion.YES);
            });
  }

  private synchronized ObjectNode schemaNode(Schema annotation) {
    if (annotation == null
        || !ApiSchemaProvider.class.isAssignableFrom(annotation.implementation())) {
      return null;
    }
    var providerClass = annotation.implementation().asSubclass(ApiSchemaProvider.class);
    // Serialize each provider once. Synchronization also protects the shared OpenAPI writer.
    var schema =
        schemas.computeIfAbsent(
            providerClass,
            type ->
                (ObjectNode) schemaIO.write(ApiSchemaProvider.create(type).schema()).orElseThrow());
    // The generator adds field metadata, so each use needs its own mutable copy.
    return schema.deepCopy();
  }
}
