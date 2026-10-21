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
 * <p>MCP clients use these schemas to build tool arguments. Custom deserialization can accept JSON
 * shapes that differ from the Java fields, so those types need overrides. Ordinary records still
 * use field-based generation.
 *
 * <p>Named {@code @Schema} definitions refer to the schemas Quarkus already generates for Swagger.
 * {@link McpOpenApiSchemaAdapter} packages those definitions into the complete MCP input schema.
 */
@Singleton
public class McpSchemaCustomizer implements SchemaGeneratorConfigCustomizer {
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
              var schema = field.getAnnotationConsideringFieldAndGetter(Schema.class);
              return (ignore != null && ignore.value()) || (schema != null && schema.hidden());
            })
        .withCustomDefinitionProvider(
            (field, context) -> {
              var schema =
                  schemaNode(field.getAnnotationConsideringFieldAndGetter(Schema.class), false);
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
    if (reference.isEmpty()) {
      // A type name identifies an OpenAPI component. A field name only names that property.
      var name = typeAnnotation ? annotation.name() : "";
      if (name.isEmpty()) {
        return null;
      }
      reference = "#/components/schemas/" + name.replace("~", "~0").replace("/", "~1");
    }
    return JsonNodeFactory.instance.objectNode().put("$ref", reference);
  }
}
