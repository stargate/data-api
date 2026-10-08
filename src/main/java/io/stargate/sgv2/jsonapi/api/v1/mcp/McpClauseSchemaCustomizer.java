package io.stargate.sgv2.jsonapi.api.v1.mcp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.CustomDefinition;
import com.github.victools.jsonschema.generator.CustomPropertyDefinition;
import com.github.victools.jsonschema.generator.SchemaGenerationContext;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import io.quarkiverse.mcp.server.runtime.SchemaGeneratorConfigCustomizer;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import jakarta.inject.Singleton;
import java.util.Map;

/**
 * Advertises the Data API wire format for clauses whose JSON shape differs from their Java fields.
 * Custom deserializers and delegating creators cannot be described by reflecting their fields.
 */
@Singleton
public class McpClauseSchemaCustomizer implements SchemaGeneratorConfigCustomizer {
  private static final Map<Class<?>, String> CLAUSE_SCHEMAS =
      Map.of(
          FilterDefinition.class,
          """
          {
            "type": ["object", "null"],
            "description": "A Data API filter object mapping document paths to values or operator expressions, such as $eq, $gt, $in, $and, and $or.",
            "additionalProperties": true,
            "examples": [{"age": {"$gt": 40}}, {"name": "Aaron", "country": {"$eq": "NZ"}}]
          }
          """,
          SortDefinition.class,
          """
          {
            "type": ["object", "null"],
            "description": "A Data API sort object mapping paths to 1 (ascending) or -1 (descending). Collections also accept a single $vector, $vectorize, or $lexical sort. Tables use the vector or lexical column name.",
            "additionalProperties": true,
            "examples": [{"user.age": -1, "user.name": 1}, {"$vector": [0.1, 0.2, 0.3]}, {"$vectorize": "search text"}]
          }
          """,
          FindAndRerankSort.class,
          """
          {
            "type": ["object", "null"],
            "description": "Use $hybrid with a query string or an object containing $vectorize or $vector, and optionally $lexical. $vector and $vectorize cannot be used together. Query text and required collection capabilities are checked when resolving the command.",
            "properties": {
              "$hybrid": {
                "anyOf": [
                  {"type": "string"},
                  {
                    "type": "object",
                    "properties": {
                      "$vectorize": {"type": ["string", "null"]},
                      "$lexical": {"type": ["string", "null"]},
                      "$vector": {
                        "anyOf": [
                          {"type": "array", "items": {"type": "number"}},
                          {"type": "object", "properties": {"$binary": {"type": "string"}}, "required": ["$binary"], "additionalProperties": false},
                          {"type": "null"}
                        ]
                      }
                    },
                    "additionalProperties": false,
                    "not": {"required": ["$vector", "$vectorize"]}
                  }
                ]
              }
            },
            "additionalProperties": false,
            "examples": [
              {"$hybrid": "search text"},
              {"$hybrid": {"$vectorize": "vector query", "$lexical": "lexical query"}},
              {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "lexical query"}},
              {"$hybrid": {"$vector": {"$binary": "P4AAAEAAAABAQAAA"}, "$lexical": "lexical query"}}
            ]
          }
          """,
          FindAndRerankCommand.HybridLimits.class,
          """
          {
            "description": "Candidate read limits as one integer for both legs, or an object containing both $vector and $lexical integer limits. Accepted bounds are determined by server configuration.",
            "anyOf": [
              {"type": "integer"},
              {
                "type": "object",
                "properties": {"$vector": {"type": "integer"}, "$lexical": {"type": "integer"}},
                "required": ["$vector", "$lexical"],
                "additionalProperties": false
              },
              {"type": "null"}
            ],
            "examples": [50, {"$vector": 50, "$lexical": 10}]
          }
          """);

  // This override is field-specific: collection creation uses the same service record but may
  // advertise authentication and parameters separately from the findAndRerank request override.
  private static final String RERANK_OVERRIDE_SCHEMA =
      """
      {
        "type": ["object", "null"],
        "description": "Optional reranking service override. Supply both provider and modelName.",
        "properties": {"provider": {"type": "string"}, "modelName": {"type": "string"}},
        "required": ["provider", "modelName"],
        "additionalProperties": false,
        "examples": [{"provider": "nvidia", "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"}]
      }
      """;

  @Override
  public void customize(SchemaGeneratorConfigBuilder builder) {
    builder
        .forFields()
        .withIgnoreCheck(field -> field.getType().getErasedType() == CommandFeatures.class)
        .withCustomDefinitionProvider(
            (field, context) -> {
              if (field.getDeclaringType().getErasedType() == FindAndRerankCommand.Options.class
                  && field.getDeclaredName().equals("rerankServiceOverride")) {
                return new CustomPropertyDefinition(
                    parseSchema(RERANK_OVERRIDE_SCHEMA, context),
                    CustomDefinition.AttributeInclusion.NO);
              }
              return null;
            })
        .withPropertyNameOverrideResolver(
            field -> {
              var property = field.getAnnotationConsideringFieldAndGetter(JsonProperty.class);
              return property != null && !property.value().isEmpty() ? property.value() : null;
            });

    builder
        .forTypesInGeneral()
        .withCustomDefinitionProvider(
            (javaType, context) -> {
              var schemaJson = CLAUSE_SCHEMAS.get(javaType.getErasedType());
              if (schemaJson == null) {
                return null;
              }
              return new CustomDefinition(
                  parseSchema(schemaJson, context),
                  CustomDefinition.DefinitionType.INLINE,
                  CustomDefinition.AttributeInclusion.NO);
            });
  }

  private static ObjectNode parseSchema(String schemaJson, SchemaGenerationContext context) {
    try {
      return (ObjectNode) context.getGeneratorConfig().getObjectMapper().readTree(schemaJson);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Invalid MCP clause schema", e);
    }
  }
}
