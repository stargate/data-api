package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link McpSchemaCustomizer}. Each test covers one rule the customizer adds to the
 * MCP schema generator, using records that apply the rule next to fields that do not.
 */
class McpSchemaCustomizerTest {
  private static final String COMPONENT_REF = "#/components/schemas/CustomizerTestSchema";

  record PropertyNames(
      @JsonProperty("wireName") String javaName,
      // Jackson treats an empty name as "use the Java name".
      @JsonProperty("") String emptyName,
      String plain) {}

  record IgnoredFields(
      @JsonIgnore String ignored,
      @JsonIgnore(false) String notIgnored,
      @Schema(hidden = true) String hidden,
      @Schema(description = "Not hidden") String visible) {}

  @Schema(description = "Type description")
  record DescribedType(String value) {}

  record Descriptions(
      @Schema(description = "Field description") String described,
      @Schema(description = " ") String blank,
      String plain,
      DescribedType typed) {}

  @Schema(name = "CustomizerTestSchema")
  record NamedType(String javaField) {}

  @Schema(ref = COMPONENT_REF)
  record ReferencedType(String javaField) {}

  record FieldValue(String javaField) {}

  record References(
      NamedType namedType,
      ReferencedType referencedType,
      @Schema(ref = COMPONENT_REF, description = "Field description") FieldValue referencedField,
      // A field's @Schema name only names the property, so the field keeps its own type.
      @Schema(name = "notAComponent") String namedField,
      // A type @Schema without a name or ref keeps the Java fields.
      DescribedType describedType) {}

  @Test
  void propertiesUseJsonPropertyNames() {
    assertEquals(Set.of("wireName", "emptyName", "plain"), propertyNames(PropertyNames.class));
  }

  @Test
  void ignoredAndHiddenFieldsAreLeftOut() {
    assertEquals(Set.of("notIgnored", "visible"), propertyNames(IgnoredFields.class));
  }

  @Test
  void descriptionsComeFromSchemaAnnotations() {
    var properties = schemaFor(Descriptions.class).path("properties");

    assertEquals("Field description", properties.path("described").path("description").asText());
    assertEquals("Type description", properties.path("typed").path("description").asText());
    assertFalse(properties.path("blank").has("description"));
    assertFalse(properties.path("plain").has("description"));
  }

  @Test
  void namedSchemasBecomeComponentReferences() {
    var properties = schemaFor(References.class).path("properties");

    for (var name : List.of("namedType", "referencedType", "referencedField")) {
      assertEquals(COMPONENT_REF, properties.path(name).path("$ref").asText(), name);
      assertFalse(properties.path(name).has("properties"), name);
    }
    assertEquals(
        "Field description", properties.path("referencedField").path("description").asText());
    assertEquals("string", properties.path("namedField").path("type").asText());
    assertFalse(properties.path("describedType").has("$ref"));
    assertTrue(properties.path("describedType").path("properties").has("value"));
  }

  private static Set<String> propertyNames(Class<?> type) {
    var names = new TreeSet<String>();
    schemaFor(type).path("properties").fieldNames().forEachRemaining(names::add);
    return names;
  }

  /** Generates a schema the way Quarkus MCP Server's {@code DefaultSchemaGenerator} does. */
  private static JsonNode schemaFor(Class<?> type) {
    var builder =
        new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON)
            .without(Option.SCHEMA_VERSION_INDICATOR);
    new McpSchemaCustomizer().customize(builder);
    return new SchemaGenerator(builder.build()).generateSchema(type);
  }
}
