package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import io.stargate.sgv2.jsonapi.metrics.CommandFeature;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Map;
import java.util.Objects;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;

/**
 * The sort of a findAndRerank command, as the user sent it. It can be any JSON value. This class
 * does not check it.
 *
 * <p>NOTE: the {@link Schema} is named <code>FindAndRerankSort</code>, not after this type, so the
 * OpenAPI and MCP schemas keep their name.
 *
 * @param json The sort as the user sent it. {@link MissingNode} when the command has no sort.
 */
@Schema(
    name = "FindAndRerankSort",
    type = SchemaType.OBJECT,
    implementation = Map.class,
    nullable = true,
    additionalProperties = Schema.False.class,
    properties =
        @SchemaProperty(
            name = HYBRID_FIELD,
            anyOf = {String.class, FindAndRerankSortDefinition.HybridSortSchema.class}),
    description =
        "Use $hybrid with a query string or an object containing $vectorize or $vector, and optionally $lexical."
            + " $vector and $vectorize cannot be used together.",
    examples = {
      """
      {"$hybrid": "Same query for vectorize and BM25 sorting"}
      """,
      """
      {"$hybrid": {"$vectorize": "vectorize sort query", "$lexical": "lexical sort"}}
      """,
      """
      {"$hybrid": {"$vector": [1, 2, 3], "$lexical": "lexical sort"}}
      """,
      """
      {"$hybrid": {"$vector": {"$binary": "P4AAAEAAAABAQAAA"}, "$lexical": "lexical query"}}
      """
    })
public record FindAndRerankSortDefinition(@JsonIgnore JsonNode json) implements Recordable {

  /** A command with no sort. */
  public static final FindAndRerankSortDefinition NO_ARG_SORT =
      new FindAndRerankSortDefinition(MissingNode.getInstance());

  /** The metrics feature of each sort field that has one. */
  private static final Map<String, CommandFeature> FIELD_FEATURES =
      Map.of(
          VECTOR_EMBEDDING_TEXT_FIELD, CommandFeature.VECTORIZE,
          VECTOR_EMBEDDING_FIELD, CommandFeature.VECTOR,
          LEXICAL_CONTENT_FIELD, CommandFeature.LEXICAL);

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public FindAndRerankSortDefinition {
    Objects.requireNonNull(json, "json must not be null");
  }

  /**
   * The metrics features of the sort, from its field names only. A field set to <code>null</code>
   * still counts.
   */
  public CommandFeatures commandFeatures() {
    var commandFeatures = CommandFeatures.create();
    var hybrid = json.get(HYBRID_FIELD);
    if (hybrid == null) {
      // a single leg sort, e.g. {"$vectorize": "cheese"}
      addFieldFeatures(json, commandFeatures);
    } else {
      // e.g. {"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}
      commandFeatures.addFeature(CommandFeature.HYBRID);
      addFieldFeatures(hybrid, commandFeatures);
    }
    return commandFeatures;
  }

  private static void addFieldFeatures(JsonNode node, CommandFeatures commandFeatures) {
    FIELD_FEATURES.forEach(
        (field, feature) -> {
          if (node.has(field)) {
            commandFeatures.addFeature(feature);
          }
        });
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder.append("json", json);
  }

  // Schema-only classes. @SchemaProperty can point at a nested schema only through a class, so
  // these empty classes carry the @Schema for each nested part of the sort. Nothing creates them.

  /** The object form of $hybrid, with $vector or $vectorize and an optional $lexical. */
  @Schema(
      name = "HybridSort",
      type = SchemaType.OBJECT,
      implementation = Object.class,
      additionalProperties = Schema.False.class,
      properties = {
        @SchemaProperty(
            name = VECTOR_EMBEDDING_TEXT_FIELD,
            type = SchemaType.STRING,
            nullable = true),
        @SchemaProperty(name = LEXICAL_CONTENT_FIELD, type = SchemaType.STRING, nullable = true),
        @SchemaProperty(
            name = VECTOR_EMBEDDING_FIELD,
            nullable = true,
            anyOf = {float[].class, BinaryVectorSchema.class})
      },
      not = ConflictingVectorSortSchema.class)
  public static class HybridSortSchema {}

  /** A binary encoded vector such as {"$binary": "P4AAAEAAAABAQAAA"}. */
  @Schema(
      name = "BinaryVector",
      type = SchemaType.OBJECT,
      implementation = Object.class,
      nullable = true,
      additionalProperties = Schema.False.class,
      requiredProperties = BINARY_VECTOR_TEXT_FIELD,
      properties = @SchemaProperty(name = BINARY_VECTOR_TEXT_FIELD, type = SchemaType.STRING))
  public static class BinaryVectorSchema {}

  /**
   * Matches an object that has both $vector and $vectorize. HybridSort uses it as its {@code not}
   * schema, so a sort that sets both fails validation.
   */
  @Schema(
      name = "ConflictingVectorSort",
      type = SchemaType.OBJECT,
      implementation = Object.class,
      requiredProperties = {VECTOR_EMBEDDING_FIELD, VECTOR_EMBEDDING_TEXT_FIELD})
  public static class ConflictingVectorSortSchema {}
}
