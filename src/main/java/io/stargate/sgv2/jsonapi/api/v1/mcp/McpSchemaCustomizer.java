package io.stargate.sgv2.jsonapi.api.v1.mcp;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.CustomDefinition;
import com.github.victools.jsonschema.generator.CustomPropertyDefinition;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import io.quarkiverse.mcp.server.runtime.SchemaGeneratorConfigCustomizer;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Generates MCP schemas using Data API clause definitions and Jackson and OpenAPI annotations.
 *
 * <p>The MCP server builds its generator with the victools PLAIN_JSON preset and no modules, so by
 * default it reads no annotations at all. Without this class, MCP schemas would use Java field
 * names, include fields Jackson ignores, and drop every {@code @Schema} description.
 *
 * <p>MCP clients use these schemas to build tool arguments. Custom deserialization can accept JSON
 * shapes that differ from the Java fields, so those types need overrides. Ordinary records still
 * use field-based generation.
 *
 * <p>Named {@code @Schema} definitions refer to the schemas Quarkus already generates for Swagger.
 * {@link McpOpenApiSchemaAdapter} packages those definitions into the complete MCP input schema.
 *
 * <p>This class also includes the field description resolver that was previously in {@code
 * McpSchemaDescriptionCustomizer}.
 */
@Singleton
public class McpSchemaCustomizer implements SchemaGeneratorConfigCustomizer {
  @Override
  public void customize(SchemaGeneratorConfigBuilder builder) {
    builder
        .forFields()
        // Field descriptions come from @Schema, as they do for Swagger.
        .withDescriptionResolver(
            field -> {
              var schema = field.getAnnotationConsideringFieldAndGetter(Schema.class);
              return schema != null && !schema.description().isBlank()
                  ? schema.description()
                  : null;
            })
        // Leave out fields that are not part of the request JSON.
        .withIgnoreCheck(
            field -> {
              var ignore = field.getAnnotationConsideringFieldAndGetter(JsonIgnore.class);
              var schema = field.getAnnotationConsideringFieldAndGetter(Schema.class);
              return (ignore != null && ignore.value()) || (schema != null && schema.hidden());
            })
        // A field with @Schema(ref) uses that OpenAPI component instead of its Java type.
        .withCustomDefinitionProvider(
            (field, context) -> {
              var schema =
                  schemaNode(field.getAnnotationConsideringFieldAndGetter(Schema.class), false);
              return schema == null
                  ? null
                  : new CustomPropertyDefinition(schema, CustomDefinition.AttributeInclusion.YES);
            })
        // Property names come from @JsonProperty, so MCP sees the JSON names clients send.
        .withPropertyNameOverrideResolver(
            field -> {
              var property = field.getAnnotationConsideringFieldAndGetter(JsonProperty.class);
              // Jackson uses an empty value for the Java name. Whitespace is an explicit JSON name.
              return property != null && !property.value().isEmpty() ? property.value() : null;
            });

    builder
        .forTypesInGeneral()
        // Type descriptions come from @Schema on the class.
        .withDescriptionResolver(
            type -> {
              var schema = type.getType().getErasedType().getAnnotation(Schema.class);
              return schema != null && !schema.description().isBlank()
                  ? schema.description()
                  : null;
            })
        // A type with a named @Schema uses that OpenAPI component instead of its Java fields.
        .withCustomDefinitionProvider(
            (javaType, context) -> {
              var schema = schemaNode(javaType.getErasedType().getAnnotation(Schema.class), true);
              return schema == null
                  ? null
                  : new CustomDefinition(
                      schema,
                      CustomDefinition.DefinitionType.INLINE,
                      CustomDefinition.AttributeInclusion.YES);
            });
  }

  private static ObjectNode schemaNode(Schema annotation, boolean typeAnnotation) {
    if (annotation == null) {
      return null;
    }
    var reference = annotation.ref();
    if (reference.isBlank()) {
      // A type name identifies an OpenAPI component. A field name only names that property.
      var name = typeAnnotation ? annotation.name() : "";
      if (name.isBlank()) {
        return null;
      }
      reference = "#/components/schemas/" + name.replace("~", "~0").replace("/", "~1");
    }
    return JsonNodeFactory.instance.objectNode().put("$ref", reference);
  }
}
