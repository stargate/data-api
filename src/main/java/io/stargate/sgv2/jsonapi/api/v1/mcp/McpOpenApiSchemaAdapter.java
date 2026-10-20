package io.stargate.sgv2.jsonapi.api.v1.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkiverse.mcp.server.GlobalInputSchemaGenerator;
import io.quarkiverse.mcp.server.ToolManager.ToolInfo;
import io.quarkus.smallrye.openapi.runtime.OpenApiConstants;
import io.vertx.core.json.JsonObject;
import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.Set;

/**
 * Shares Swagger's generated schemas with MCP while retaining the existing tool generator.
 *
 * <p>OpenAPI stores reusable schemas under components/schemas, while standalone JSON Schema uses
 * $defs. Copy only the referenced definitions and adjust their addresses. Keeping references
 * preserves validation constraints without expanding or merging schemas.
 */
@Decorator
@Priority(1)
public class McpOpenApiSchemaAdapter implements GlobalInputSchemaGenerator {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String OPEN_API_REF = "#/components/schemas/";
  private static final String DEFINITION_PREFIX = "_openapi_";
  private static final Set<String> SCHEMA_MAPS =
      Set.of("properties", "patternProperties", "$defs", "dependentSchemas");
  private static final Set<String> SCHEMA_ARRAYS = Set.of("allOf", "anyOf", "oneOf", "prefixItems");
  private static final Set<String> SCHEMA_VALUES =
      Set.of(
          "items",
          "additionalProperties",
          "unevaluatedProperties",
          "contains",
          "not",
          "propertyNames",
          "if",
          "then",
          "else",
          "unevaluatedItems",
          "contentSchema");

  @Inject @Delegate GlobalInputSchemaGenerator delegate;
  private JsonNode openApi;

  public McpOpenApiSchemaAdapter() {}

  McpOpenApiSchemaAdapter(JsonNode openApi) {
    this.openApi = openApi;
  }

  @Override
  public InputSchema generate(ToolInfo tool) {
    var generated = delegate.generate(tool);
    try {
      return new AdaptedInputSchema(
          new JsonObject(adapt(MAPPER.readTree(generated.asJson())).toString()));
    } catch (IOException e) {
      throw new IllegalStateException("Cannot read the MCP input schema for " + tool.name(), e);
    }
  }

  ObjectNode adapt(JsonNode input) {
    ObjectNode result = input.deepCopy();
    var definitions = MAPPER.createObjectNode();
    collectDefinitions(result, definitions);
    if (!definitions.isEmpty()) {
      var target = result.withObject("/$defs");
      definitions
          .properties()
          .forEach(
              entry -> {
                if (target.has(entry.getKey())) {
                  throw new IllegalStateException(
                      "Duplicate MCP schema definition " + entry.getKey());
                }
                target.set(entry.getKey(), entry.getValue());
              });
    }
    return result;
  }

  private void collectDefinitions(JsonNode node, ObjectNode definitions) {
    if (!(node instanceof ObjectNode schema)) {
      return;
    }
    var reference = schema.path("$ref").asText();
    if (reference.startsWith(OPEN_API_REF)) {
      var suffix = reference.substring(OPEN_API_REF.length());
      var component = suffix.split("/", 2)[0];
      var name = DEFINITION_PREFIX + component.replace("~1", "/").replace("~0", "~");
      var document = openApi();
      if (document.at(reference.substring(1)).isMissingNode()) {
        throw new IllegalStateException("Unresolved OpenAPI schema reference " + reference);
      }
      schema.put("$ref", "#/$defs/" + DEFINITION_PREFIX + suffix);
      if (!definitions.has(name)) {
        var definition = document.at("/components/schemas/" + component).deepCopy();
        // Register before following dependencies so recursive definitions are copied only once.
        definitions.set(name, definition);
        collectDefinitions(definition, definitions);
      }
    }
    // Examples, defaults, and property names can contain literal $ref values. Visit schemas only.
    for (var keyword : SCHEMA_MAPS) {
      if (schema.path(keyword).isObject()) {
        schema.path(keyword).forEach(value -> collectDefinitions(value, definitions));
      }
    }
    for (var keyword : SCHEMA_ARRAYS) {
      if (schema.path(keyword).isArray()) {
        schema.path(keyword).forEach(value -> collectDefinitions(value, definitions));
      }
    }
    for (var keyword : SCHEMA_VALUES) {
      collectDefinitions(schema.path(keyword), definitions);
    }
  }

  private synchronized JsonNode openApi() {
    if (openApi == null) {
      var loader = OpenApiConstants.classLoader;
      if (loader == null) {
        loader = Thread.currentThread().getContextClassLoader();
      }
      // Quarkus packages the same build-time document for JVM and native applications.
      try (var stream = loader.getResourceAsStream(OpenApiConstants.BASE_NAME + ".JSON")) {
        if (stream == null) {
          throw new IllegalStateException("The generated OpenAPI document is unavailable");
        }
        openApi = MAPPER.readTree(stream);
      } catch (IOException e) {
        throw new IllegalStateException("Cannot read the generated OpenAPI document", e);
      }
    }
    return openApi;
  }

  private record AdaptedInputSchema(JsonObject value) implements InputSchema {
    @Override
    public String asJson() {
      return value.encode();
    }
  }
}
