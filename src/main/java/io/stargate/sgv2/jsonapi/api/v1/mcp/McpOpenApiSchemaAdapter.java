package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants.Components.SCHEMAS_POINTER;
import static io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants.Components.SCHEMA_REF_PREFIX;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
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
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Makes each MCP tool input schema standalone by copying in the OpenAPI schemas it points to.
 *
 * <p>{@link McpSchemaCustomizer} points clause fields at the schemas Quarkus generates for Swagger,
 * so both APIs share one definition. An MCP tool's input schema is a standalone JSON Schema
 * document, though, so a pointer into the OpenAPI document leads nowhere for an MCP client.
 *
 * <p>After the MCP library generates a tool schema, this class copies each OpenAPI schema the tool
 * needs into the tool schema's {@code $defs}, and rewrites the pointers to match:
 *
 * <pre>
 * before: {"properties": {"filter": {"$ref": "#/components/schemas/FilterDefinition"}}}
 * after:  {"properties": {"filter": {"$ref": "#/$defs/_openapi_FilterDefinition"}},
 *          "$defs": {"_openapi_FilterDefinition": (copy of the OpenAPI schema)}}
 * </pre>
 *
 * <p>Pointers stay pointers rather than being replaced by the schemas they name. Clause schemas can
 * contain themselves, so replacing them would never finish, and keywords written next to a pointer
 * would have to be merged into the copied schema.
 */
// @Decorator wraps the MCP library's generator instead of replacing it. CDI enables a decorator for
// the whole application only when it has @Priority. The value orders decorators of the same bean,
// and this is the only one.
@Decorator
@Priority(1)
public class McpOpenApiSchemaAdapter implements GlobalInputSchemaGenerator {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> JSON_OBJECT = new TypeReference<>() {};

  /** Start of a pointer to a definition in the tool schema's own {@code $defs}. */
  private static final String DEFS_REF_PREFIX = "#/$defs/";

  /**
   * Start of each copied schema's name in {@code $defs}. The MCP library names its own definitions
   * after Java types, so the prefix keeps a copied {@code FilterDefinition} apart from a definition
   * the library generated from a Java class with the same name.
   */
  private static final String DEFINITION_PREFIX = "_openapi_";

  // The JSON Schema 2020-12 keywords that hold other schemas. The walk follows only these, so a
  // "$ref" that appears as data, in an example or a default value, is never rewritten.

  /** Keywords whose value is an object of schemas, such as {@code properties}. */
  private static final Set<String> SCHEMA_OBJECTS =
      Set.of("properties", "patternProperties", "$defs", "dependentSchemas");

  /** Keywords whose value is an array of schemas, such as {@code anyOf}. */
  private static final Set<String> SCHEMA_ARRAYS = Set.of("allOf", "anyOf", "oneOf", "prefixItems");

  /** Keywords whose value is a single schema, such as {@code items}. */
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

  private final GlobalInputSchemaGenerator delegate;

  /** The OpenAPI document Quarkus generated at build time. Read once and never changed. */
  private final JsonNode openApi;

  /**
   * Reads the OpenAPI document once, here, rather than on each request. The MCP HTTP extension
   * creates its tool manager, and with it this decorator, while the server starts. A missing
   * document therefore stops startup instead of failing each request that lists tools.
   */
  @Inject
  public McpOpenApiSchemaAdapter(@Delegate GlobalInputSchemaGenerator delegate) {
    this.delegate = delegate;
    this.openApi = readGeneratedOpenApiDocument();
  }

  /**
   * Generates the tool schema as usual, then copies in the OpenAPI schemas it points to.
   *
   * <p>The MCP library calls this each time a client lists tools, once for each tool.
   */
  @Override
  public InputSchema generate(ToolInfo tool) {
    ObjectNode schema;
    try {
      // The interface promises the schema only as JSON text, so read it once to change it.
      schema = (ObjectNode) MAPPER.readTree(delegate.generate(tool).asJson());
    } catch (JsonProcessingException e) {
      // Not expected: the library wrote this JSON itself. Listing tools fails with this error.
      throw new IllegalStateException("Cannot read the MCP input schema for " + tool.name(), e);
    }
    bundleOpenApiSchemas(schema, openApi);
    // The library sends value(), a Vert.x object. Convert the tree to it without writing JSON text.
    return new StandaloneInputSchema(new JsonObject(MAPPER.convertValue(schema, JSON_OBJECT)));
  }

  /**
   * Changes {@code schema} in place so it no longer points into the OpenAPI document.
   *
   * <p>Package-private and static so tests can check the conversion against a small OpenAPI
   * document without starting Quarkus. {@link #generate} is the only production caller.
   *
   * @param schema a tool input schema, which this method changes
   * @param openApi the OpenAPI document, which this method only reads
   * @throws IllegalStateException if a pointer names a schema the OpenAPI document does not have
   */
  static void bundleOpenApiSchemas(ObjectNode schema, JsonNode openApi) {
    rebaseArgumentReferences(schema);
    var copies = copyReferencedSchemas(schema, openApi);
    if (copies.isEmpty()) {
      // Most tools have no clause arguments. Leave their schemas without an empty $defs.
      return;
    }
    var definitions = schema.withObject("/$defs");
    copies.forEach(
        (name, copy) -> {
          // Library definitions never start with the prefix, so a clash is a bug in this class.
          // Fail rather than replace a definition. Listing tools fails with this error.
          if (definitions.has(name)) {
            throw new IllegalStateException("Duplicate MCP schema definition " + name);
          }
          definitions.set(name, copy);
        });
  }

  /** Returns a pointer to the named schema under {@code components/schemas} in OpenAPI. */
  static String componentRef(String name) {
    return SCHEMA_REF_PREFIX + escapePointerToken(name);
  }

  /**
   * Reads the OpenAPI document that Quarkus generates at build time and packages with the
   * application. Quarkus builds the document Swagger serves from the same file.
   *
   * <p>The lookup matches Quarkus's own {@code OpenApiDocumentService}: the class loader the
   * OpenAPI extension records while starting, or the current thread's class loader if none is
   * recorded. Package-private so tests can use the same packaged document.
   */
  static JsonNode readGeneratedOpenApiDocument() {
    var loader = OpenApiConstants.classLoader;
    if (loader == null) {
      loader = Thread.currentThread().getContextClassLoader();
    }
    try (var stream = loader.getResourceAsStream(OpenApiConstants.BASE_NAME + ".JSON")) {
      if (stream == null) {
        // Only if the build stops generating the document, for example when OpenAPI generation is
        // turned off. Clause schemas cannot be described without it, so stop startup.
        throw new IllegalStateException("The generated OpenAPI document is unavailable");
      }
      return MAPPER.readTree(stream);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot read the generated OpenAPI document", e);
    }
  }

  /**
   * Fixes pointers inside tool arguments that carry their own {@code $defs}.
   *
   * <p>The MCP library generates each tool argument separately and nests the result under {@code
   * properties}. When an argument's schema has its own {@code $defs}, its pointers still start at
   * the document root, where those definitions do not exist:
   *
   * <pre>
   * before: {"properties": {"options": {"$defs": {"Local": ...},
   *                                     "properties": {"value": {"$ref": "#/$defs/Local"}}}}}
   * after:  the inner pointer becomes "#/properties/options/$defs/Local"
   * </pre>
   *
   * <p>A pointer changes only when the root lacks the definition and the argument has it, so
   * pointers that already work stay as they are. Each argument keeps its own definitions, so two
   * arguments can both define {@code Local} without a clash.
   */
  private static void rebaseArgumentReferences(ObjectNode schema) {
    for (var argument : schema.path("properties").properties()) {
      var argumentSchema = argument.getValue();
      if (!argumentSchema.path("$defs").isObject()) {
        continue;
      }
      var argumentPointer = "#/properties/" + escapePointerToken(argument.getKey());
      visitSchemas(
          argumentSchema,
          node -> {
            var reference = node.path("$ref").asText();
            if (reference.startsWith(DEFS_REF_PREFIX)
                && schema.at(reference.substring(1)).isMissingNode()
                && !argumentSchema.at(reference.substring(1)).isMissingNode()) {
              node.put("$ref", argumentPointer + reference.substring(1));
            }
          });
    }
  }

  /**
   * Rewrites each OpenAPI pointer in {@code schema} to point into {@code $defs}. Returns copies of
   * the OpenAPI schemas those pointers need, keyed by their {@code $defs} name.
   *
   * <p>Copied schemas can point to further OpenAPI schemas, so they are rewritten and searched as
   * well. Each schema is copied once, which also ends the search for schemas that contain
   * themselves.
   */
  private static Map<String, JsonNode> copyReferencedSchemas(ObjectNode schema, JsonNode openApi) {
    var copies = new LinkedHashMap<String, JsonNode>();
    var pending = new ArrayDeque<JsonNode>();
    pending.add(schema);
    while (!pending.isEmpty()) {
      visitSchemas(
          pending.remove(),
          node -> {
            var reference = node.path("$ref").asText();
            if (!reference.startsWith(SCHEMA_REF_PREFIX)) {
              return;
            }
            if (openApi.at(reference.substring(1)).isMissingNode()) {
              throw new IllegalStateException("Unresolved OpenAPI schema reference " + reference);
            }
            // The path is usually a schema name, such as "FilterDefinition", but it can point
            // inside one, such as "FilterDefinition/properties/x". Copy the whole named schema and
            // keep the rest of the path.
            var path = reference.substring(SCHEMA_REF_PREFIX.length());
            var escapedName = path.split("/", 2)[0];
            node.put("$ref", DEFS_REF_PREFIX + DEFINITION_PREFIX + path);
            // Pointers escape "~" and "/" in names, but $defs keys hold the plain name.
            var name = DEFINITION_PREFIX + unescapePointerToken(escapedName);
            if (!copies.containsKey(name)) {
              // Copy, because every request shares the document and the copy gets rewritten.
              var copy = openApi.at(SCHEMAS_POINTER + escapedName).deepCopy();
              copies.put(name, copy);
              pending.add(copy);
            }
          });
    }
    return copies;
  }

  /** Calls {@code visitor} on {@code node} and on every schema inside it, parents first. */
  private static void visitSchemas(JsonNode node, Consumer<ObjectNode> visitor) {
    if (!(node instanceof ObjectNode schema)) {
      return;
    }
    visitor.accept(schema);
    for (var keyword : SCHEMA_OBJECTS) {
      if (schema.path(keyword).isObject()) {
        schema.path(keyword).forEach(value -> visitSchemas(value, visitor));
      }
    }
    for (var keyword : SCHEMA_ARRAYS) {
      if (schema.path(keyword).isArray()) {
        schema.path(keyword).forEach(value -> visitSchemas(value, visitor));
      }
    }
    for (var keyword : SCHEMA_VALUES) {
      visitSchemas(schema.path(keyword), visitor);
    }
  }

  /** Escapes a name for a JSON Pointer (RFC 6901): "~" becomes "~0" and "/" becomes "~1". */
  private static String escapePointerToken(String name) {
    return name.replace("~", "~0").replace("/", "~1");
  }

  /** Reverses {@link #escapePointerToken}, replacing "~1" first as RFC 6901 requires. */
  private static String unescapePointerToken(String token) {
    return token.replace("~1", "/").replace("~0", "~");
  }

  /** The tool schema as the Vert.x object the MCP library puts into its tool list. */
  private record StandaloneInputSchema(JsonObject value) implements InputSchema {
    @Override
    public String asJson() {
      // Matches the library's own InputSchema implementation.
      return value.toString();
    }
  }
}
