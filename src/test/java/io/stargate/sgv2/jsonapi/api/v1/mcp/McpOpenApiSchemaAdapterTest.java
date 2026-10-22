package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import org.junit.jupiter.api.Test;

/** Checks that standalone MCP schemas retain OpenAPI references and validation semantics. */
class McpOpenApiSchemaAdapterTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void shouldCopyOnlyReachableComponentsAndRebaseTheirReferences() throws Exception {
    var schema = adapt("{\"$ref\":\"#/components/schemas/ObjectValue\"}");

    assertEquals("#/$defs/_openapi_ObjectValue", schema.path("$ref").asText());
    var definitions = schema.path("$defs");
    assertEquals(2, definitions.size());
    assertTrue(definitions.has("_openapi_ObjectValue"));
    assertTrue(definitions.has("_openapi_Value"));
    assertFalse(definitions.has("_openapi_Unused"));
    assertEquals(
        "#/$defs/_openapi_Value",
        definitions
            .path("_openapi_ObjectValue")
            .path("properties")
            .path("a")
            .path("$ref")
            .asText());
    assertAccepts(schema, "{\"a\":10}");
    assertRejects(schema, "{\"a\":15}");
  }

  @Test
  void shouldLeaveFallbackSchemasAndExistingDefinitionsUnchanged() throws Exception {
    var input =
        MAPPER.readTree(
            """
            {
              "type":"object",
              "properties":{"value":{"$ref":"#/$defs/Local"}},
              "$defs":{"Local":{"type":"string"}}
            }
            """);

    var adapted = new McpOpenApiSchemaAdapter(fixtureDocument()).adapt(input);

    assertEquals(input, adapted);
    assertAccepts(adapted, "{\"value\":\"text\"}");
    assertRejects(adapted, "{\"value\":5}");
  }

  @Test
  void shouldResolveFallbackDefinitionsWithinEachToolArgument() throws Exception {
    var input =
        MAPPER.readTree(
            """
            {
              "type":"object",
              "properties":{
                "options/~":{
                  "type":"object",
                  "properties":{
                    "value":{"$ref":"#/$defs/Local"},
                    "shared":{"$ref":"#/components/schemas/Value"}
                  },
                  "$defs":{"Local":{
                    "type":"object",
                    "properties":{
                      "name":{"type":"string"},
                      "next":{"$ref":"#/$defs/Local"}
                    }
                  }},
                  "examples":[{"value":{"$ref":"#/$defs/Local"}}]
                },
                "other":{
                  "type":"object",
                  "properties":{"value":{"$ref":"#/$defs/Local"}},
                  "$defs":{"Local":{"type":"integer"}}
                }
              }
            }
            """);
    var original = input.deepCopy();
    var schema = new McpOpenApiSchemaAdapter(fixtureDocument()).adapt(input);

    assertEquals(original, input);
    assertEquals(
        "#/properties/options~1~0/$defs/Local",
        schema.at("/properties/options~1~0/properties/value/$ref").asText());
    assertEquals(
        original.at("/properties/options~1~0/examples"),
        schema.at("/properties/options~1~0/examples"));
    assertAccepts(
        schema,
        """
        {"options/~":{"value":{"name":"first","next":{"name":"second"}},"shared":10},
         "other":{"value":2}}
        """);
    assertRejects(schema, "{\"options/~\":{\"value\":{\"next\":{\"name\":2}}}}");
    assertRejects(schema, "{\"options/~\":{\"shared\":15}}");
    assertRejects(schema, "{\"other\":{\"value\":\"wrong\"}}");
  }

  @Test
  void shouldPreserveValidRootReferencesWhenArgumentDefinitionsHaveTheSameName() throws Exception {
    var input =
        MAPPER.readTree(
            """
        {
          "type":"object",
          "properties":{"options":{
            "type":"object",
            "properties":{"value":{"$ref":"#/$defs/Local"}},
            "$defs":{"Local":{"type":"integer"}}
          }},
          "$defs":{"Local":{"type":"string"}}
        }
        """);
    var schema = new McpOpenApiSchemaAdapter(fixtureDocument()).adapt(input);

    assertEquals(input, schema);
    assertAccepts(schema, "{\"options\":{\"value\":\"text\"}}");
    assertRejects(schema, "{\"options\":{\"value\":5}}");
  }

  @Test
  void shouldRetainExistingDefinitionsWhenAddingOpenApiComponents() throws Exception {
    var schema =
        adapt(
            """
            {
              "type":"object",
              "properties":{
                "local":{"$ref":"#/$defs/Local"},
                "remote":{"$ref":"#/components/schemas/Value"}
              },
              "$defs":{"Local":{"type":"string"}}
            }
            """);

    assertEquals("#/$defs/Local", schema.path("properties").path("local").path("$ref").asText());
    assertEquals("string", schema.path("$defs").path("Local").path("type").asText());
    assertTrue(schema.path("$defs").has("_openapi_Value"));
    assertAccepts(schema, "{\"local\":\"text\",\"remote\":10}");
    assertRejects(schema, "{\"local\":\"text\",\"remote\":15}");
  }

  @Test
  void shouldSupportRecursiveComponentsWithoutExpandingThem() throws Exception {
    var schema = adapt("{\"$ref\":\"#/components/schemas/Recursive\"}");

    assertEquals(1, schema.path("$defs").size());
    assertEquals(
        "#/$defs/_openapi_Recursive",
        schema
            .path("$defs")
            .path("_openapi_Recursive")
            .path("properties")
            .path("next")
            .path("anyOf")
            .get(0)
            .path("$ref")
            .asText());
    assertAccepts(schema, "{\"value\":1,\"next\":{\"value\":2,\"next\":null}}");
    assertRejects(schema, "{\"value\":1,\"next\":{\"value\":\"wrong\"}}");
  }

  @Test
  void shouldPreserveEscapedComponentNamesAndNestedPointerSuffixes() throws Exception {
    var document = fixtureDocument();
    ((ObjectNode) document.path("components").path("schemas"))
        .putObject("A/B~C")
        .put("type", "object")
        .putObject("properties")
        .putObject("value")
        .put("type", "integer");
    var input =
        MAPPER.readTree(
            """
            {
              "type":"object",
              "properties":{
                "whole":{"$ref":"#/components/schemas/A~1B~0C"},
                "value":{"$ref":"#/components/schemas/A~1B~0C/properties/value"}
              }
            }
            """);

    var schema = new McpOpenApiSchemaAdapter(document).adapt(input);

    assertTrue(schema.path("$defs").has("_openapi_A/B~C"));
    assertEquals(
        "#/$defs/_openapi_A~1B~0C", schema.path("properties").path("whole").path("$ref").asText());
    assertEquals(
        "#/$defs/_openapi_A~1B~0C/properties/value",
        schema.path("properties").path("value").path("$ref").asText());
    assertAccepts(schema, "{\"whole\":{\"value\":1},\"value\":2}");
    assertRejects(schema, "{\"whole\":{\"value\":1},\"value\":\"wrong\"}");
  }

  @Test
  void shouldRetainOverlappingConstraintsOnReferenceSiblings() throws Exception {
    var schema = adapt("{\"$ref\":\"#/components/schemas/Value\",\"maximum\":20}");

    assertEquals(20, schema.path("maximum").asInt());
    assertAccepts(schema, "10");
    assertRejects(schema, "15");
  }

  @Test
  void shouldRetainDisjointConstraintsOnReferenceSiblings() throws Exception {
    var schema =
        adapt("{\"$ref\":\"#/components/schemas/ObjectValue\",\"additionalProperties\":false}");

    assertTrue(schema.has("$ref"));
    assertFalse(schema.path("additionalProperties").asBoolean());
    assertAccepts(schema, "{}");
    assertRejects(schema, "{\"a\":1}");
  }

  @Test
  void shouldKeepReferenceSiblingsInsideCopiedComponents() throws Exception {
    var document = fixtureDocument();
    ((ObjectNode)
            document
                .path("components")
                .path("schemas")
                .path("ObjectValue")
                .path("properties")
                .path("a"))
        .put("maximum", 20);
    var schema =
        new McpOpenApiSchemaAdapter(document)
            .adapt(MAPPER.readTree("{\"$ref\":\"#/components/schemas/ObjectValue\"}"));

    assertAccepts(schema, "{\"a\":10}");
    assertRejects(schema, "{\"a\":15}");
  }

  @Test
  void shouldPreserveAPropertyNamedRefAndLiteralReferencesInInstanceData() throws Exception {
    var input =
        MAPPER.readTree(
            """
            {
              "type":"object",
              "properties":{"$ref":{"$ref":"#/components/schemas/Value"}},
              "examples":[{"$ref":"#/components/schemas/Unknown"}],
              "default":{"$ref":"#/components/schemas/Unknown"},
              "const":{"$ref":"#/components/schemas/Unknown"},
              "enum":[{"$ref":"#/components/schemas/Unknown"}]
            }
            """);
    var schema = new McpOpenApiSchemaAdapter(fixtureDocument()).adapt(input);

    assertEquals(
        "#/$defs/_openapi_Value", schema.path("properties").path("$ref").path("$ref").asText());
    for (var keyword : new String[] {"examples", "default", "const", "enum"}) {
      assertEquals(input.path(keyword), schema.path(keyword));
    }
  }

  @Test
  void shouldNotMutateInputsOrShareMutableComponentCopies() throws Exception {
    var document = fixtureDocument();
    var originalDocument = document.deepCopy();
    var input = MAPPER.readTree("{\"$ref\":\"#/components/schemas/Value\"}");
    var originalInput = input.deepCopy();
    var adapter = new McpOpenApiSchemaAdapter(document);
    var schema = adapter.adapt(input);

    ((ObjectNode) schema.path("$defs").path("_openapi_Value")).put("maximum", 100);

    assertEquals(originalDocument, document);
    assertEquals(originalInput, input);
    assertRejects(adapter.adapt(input), "15");
  }

  @Test
  void shouldRejectMissingComponentReferences() throws Exception {
    var adapter = new McpOpenApiSchemaAdapter(fixtureDocument());
    var input = MAPPER.readTree("{\"$ref\":\"#/components/schemas/Missing\"}");

    assertThrows(IllegalStateException.class, () -> adapter.adapt(input));
  }

  @Test
  void shouldPreserveFractionalSchemaBoundsAndDefault() throws Exception {
    var document = fixtureDocument();
    ((ObjectNode) document.path("components").path("schemas"))
        .putObject("Fractional")
        .put("type", "number")
        .put("minimum", 0.25)
        .put("maximum", 1.75)
        .put("default", 0.5);
    var schema =
        new McpOpenApiSchemaAdapter(document)
            .adapt(MAPPER.readTree("{\"$ref\":\"#/components/schemas/Fractional\"}"));
    var component = schema.at(schema.path("$ref").asText().substring(1));

    assertEquals(0.25, component.path("minimum").doubleValue());
    assertEquals(1.75, component.path("maximum").doubleValue());
    assertEquals(0.5, component.path("default").doubleValue());
    assertAccepts(schema, "0.25");
    assertAccepts(schema, "1.75");
    assertAccepts(schema, component.path("default").toString());
    assertRejects(schema, "0.125");
    assertRejects(schema, "1.875");
  }

  private static ObjectNode adapt(String input) throws Exception {
    return new McpOpenApiSchemaAdapter(fixtureDocument()).adapt(MAPPER.readTree(input));
  }

  private static void assertAccepts(JsonNode schema, String input) throws Exception {
    var errors =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(schema)
            .validate(MAPPER.readTree(input));
    assertTrue(errors.isEmpty(), errors::toString);
  }

  private static void assertRejects(JsonNode schema, String input) throws Exception {
    var errors =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(schema)
            .validate(MAPPER.readTree(input));
    assertFalse(errors.isEmpty());
  }

  private static JsonNode fixtureDocument() throws Exception {
    return MAPPER.readTree(
        """
        {
          "components":{"schemas":{
            "Value":{"type":"integer","maximum":10},
            "ObjectValue":{"type":"object","properties":{"a":{"$ref":"#/components/schemas/Value"}}},
            "Recursive":{
              "type":"object",
              "properties":{
                "value":{"type":"integer"},
                "next":{"anyOf":[{"$ref":"#/components/schemas/Recursive"},{"type":"null"}]}
              },
              "required":["value"],
              "additionalProperties":false
            },
            "Unused":{"type":"string"}
          }}
        }
        """);
  }
}
