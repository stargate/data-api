package io.stargate.sgv2.jsonapi.api.v1.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.CustomDefinition;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaKeyword;
import io.quarkiverse.mcp.server.runtime.SchemaGeneratorConfigCustomizer;
import io.stargate.sgv2.jsonapi.api.model.command.JsonDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import jakarta.inject.Singleton;
import java.util.Map;

/**
 * Advertises the user-facing Data API JSON shape for command clause types in MCP tool input
 * schemas, instead of the internal Java shape.
 *
 * <p>Clause types such as {@link FilterDefinition} are deserialized from the raw user JSON (via
 * delegating creators or custom deserializers), so their Java fields ({@code filterClause}, {@code
 * json}, {@code updateOperationDefs}, ...) are not the wire format. Without this customizer, the
 * victools generator used by Quarkus MCP Server reflects over those fields and advertises a schema
 * that the server cannot actually deserialize — schema-compliant MCP clients then send arguments
 * like {@code {"filter": {"json": {...}}}} that fail, instead of the plain Data API clause {@code
 * {"filter": {"age": {"$gt": 40}}}}.
 *
 * <p>For these types this customizer emits a plain {@code {"type": "object"}} schema with a
 * description and an example of the accepted Data API clause syntax.
 */
@Singleton
public class McpClauseSchemaCustomizer implements SchemaGeneratorConfigCustomizer {

  /** Description and example of the user-facing Data API JSON shape for a clause type. */
  record PlainObjectSchema(String description, String exampleJson) {}

  /**
   * Clause types whose MCP schema must be the plain user-facing Data API JSON object. Examples must
   * be valid JSON: they are embedded into the generated schema's {@code examples}.
   */
  private static final Map<Class<?>, PlainObjectSchema> PLAIN_OBJECT_SCHEMAS =
      Map.of(
          FilterDefinition.class,
          new PlainObjectSchema(
              "Filter as a plain Data API filter object: maps document paths to exact values"
                  + " or to operator expressions such as $eq, $ne, $gt, $gte, $lt, $lte, $in,"
                  + " $nin, $exists, $and, $or, $not.",
              """
              {"name": "Aaron", "country": {"$eq": "NZ"}, "age": {"$gt": 40}}
              """),
          SortDefinition.class,
          new PlainObjectSchema(
              "Sort as a plain Data API sort object: maps document paths to 1 (ascending) or"
                  + " -1 (descending), or uses $vector / $vectorize for vector search.",
              """
              {"user.age": -1, "user.name": 1}
              """),
          UpdateClause.class,
          new PlainObjectSchema(
              "Update as a plain Data API update object keyed by update operators such as"
                  + " $set, $unset, $inc, $push, $pull.",
              """
              {"$set": {"location": "New York"}, "$unset": {"new_data": 1}}
              """),
          FindAndRerankSort.class,
          new PlainObjectSchema(
              "Hybrid sort as a plain object: {\"$hybrid\": \"query text\"} to use the same"
                  + " query for vector and lexical search, or separate queries via $vectorize /"
                  + " $lexical / $vector.",
              """
              {"$hybrid": {"$vectorize": "vectorize sort query", "$lexical": "lexical sort"}}
              """));

  /** Fallback for {@link JsonDefinition} subtypes without an entry in the map above. */
  private static final PlainObjectSchema GENERIC_PLAIN_OBJECT_SCHEMA =
      new PlainObjectSchema(
          "A plain JSON object using the same syntax as the equivalent Data API command clause.",
          null);

  @Override
  public void customize(SchemaGeneratorConfigBuilder builder) {
    builder
        .forTypesInGeneral()
        .withCustomDefinitionProvider(
            (javaType, context) -> {
              var plainObjectSchema = plainObjectSchemaFor(javaType.getErasedType());
              if (plainObjectSchema == null) {
                return null;
              }

              ObjectNode schemaNode = context.getGeneratorConfig().createObjectNode();
              schemaNode.put(
                  context.getKeyword(SchemaKeyword.TAG_TYPE),
                  context.getKeyword(SchemaKeyword.TAG_TYPE_OBJECT));
              schemaNode.put("description", plainObjectSchema.description());
              JsonNode example =
                  parseExample(
                      context.getGeneratorConfig().getObjectMapper(),
                      plainObjectSchema.exampleJson());
              if (example != null) {
                schemaNode.putArray("examples").add(example);
              }
              return new CustomDefinition(
                  schemaNode,
                  CustomDefinition.DefinitionType.INLINE,
                  CustomDefinition.AttributeInclusion.YES);
            });
  }

  private static PlainObjectSchema plainObjectSchemaFor(Class<?> erasedType) {
    var plainObjectSchema = PLAIN_OBJECT_SCHEMAS.get(erasedType);
    if (plainObjectSchema == null && JsonDefinition.class.isAssignableFrom(erasedType)) {
      return GENERIC_PLAIN_OBJECT_SCHEMA;
    }
    return plainObjectSchema;
  }

  private static JsonNode parseExample(ObjectMapper objectMapper, String exampleJson) {
    if (exampleJson == null) {
      return null;
    }
    try {
      return objectMapper.readTree(exampleJson);
    } catch (Exception e) {
      // An unparseable example is a bug in PLAIN_OBJECT_SCHEMAS (covered by unit tests); omit
      // the example rather than fail tool listing.
      return null;
    }
  }
}
