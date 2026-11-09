package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.MissingNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Map;
import java.util.Objects;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;

/**
 * The sort clause of a findAndRerank command as the command stores it, read by {@link
 * FindAndRerankSortClauseDeserializer}. {@link FindAndRerankSortParser} parses it into a {@link
 * FindAndRerankSort} when the command is resolved.
 *
 * <ul>
 *   <li>{@link FindAndRerankSort.MultiLeg}: a sort with $hybrid, already parsed by the
 *       deserializer.
 *   <li>{@link SingleLegDefinition}: any other sort, kept as the user sent it.
 * </ul>
 *
 * <p>TODO: parse $hybrid in {@link FindAndRerankSortParser} as well, then this type only holds the
 * sort as the user sent it, like {@link SortDefinition} does for find. Not done yet because moving
 * $hybrid changes its error codes and the order of its errors, so it needs its own change.
 *
 * <p>NOTE: the {@link Schema} is named <code>FindAndRerankSort</code>, not after this type, so the
 * OpenAPI and MCP schemas keep the name they had before the parsed sort got its own type. It only
 * describes $hybrid. Single leg sorts are added to it when they are supported, see GH #2604.
 */
@JsonDeserialize(using = FindAndRerankSortClauseDeserializer.class)
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
public sealed interface FindAndRerankSortDefinition extends Recordable
    permits FindAndRerankSort.MultiLeg, FindAndRerankSortDefinition.SingleLegDefinition {

  /**
   * A command with no sort, a null sort or an empty sort.
   *
   * <p>NOTE: uses {@link MissingNode} because it cannot be changed, and this instance is shared.
   */
  SingleLegDefinition NO_ARG_SORT =
      new SingleLegDefinition(MissingNode.getInstance(), CommandFeatures.EMPTY);

  /**
   * The metrics features of the sort, recorded when the sort is read. The command adds them to its
   * features before it is resolved, so before the sort is parsed.
   */
  CommandFeatures commandFeatures();

  /**
   * Any sort without $hybrid, e.g. <code>{"$vectorize": "cheese"}</code>, <code>{"$vector": [0.1,
   * 0.2]}</code>, <code>{"$lexical": "cows"}</code> or <code>{"age": -1}</code>. It is kept as the
   * user sent it, {@link FindAndRerankSortParser} checks and parses it.
   *
   * @param sort The sort as the user sent it: an object, or {@link MissingNode} when the command
   *     has no sort, a null sort or an empty sort.
   * @param commandFeatures The metrics features, from the field names of the sort only.
   */
  record SingleLegDefinition(@JsonIgnore JsonNode sort, @JsonIgnore CommandFeatures commandFeatures)
      implements FindAndRerankSortDefinition {

    public SingleLegDefinition {
      Objects.requireNonNull(sort, "sort must not be null");
      Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
      // the parser reads the sort as an object, see FindAndRerankSortParser
      if (!sort.isObject() && !sort.isMissingNode()) {
        throw new IllegalArgumentException(
            "sort must be an object or MissingNode, sort.getNodeType(): " + sort.getNodeType());
      }
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("sort", sort).append("commandFeatures", commandFeatures);
    }
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
  class HybridSortSchema {}

  /** A binary encoded vector such as {"$binary": "P4AAAEAAAABAQAAA"}. */
  @Schema(
      name = "BinaryVector",
      type = SchemaType.OBJECT,
      implementation = Object.class,
      nullable = true,
      additionalProperties = Schema.False.class,
      requiredProperties = BINARY_VECTOR_TEXT_FIELD,
      properties = @SchemaProperty(name = BINARY_VECTOR_TEXT_FIELD, type = SchemaType.STRING))
  class BinaryVectorSchema {}

  /**
   * Matches an object that has both $vector and $vectorize. HybridSort uses it as its {@code not}
   * schema, so a sort that sets both fails validation.
   */
  @Schema(
      name = "ConflictingVectorSort",
      type = SchemaType.OBJECT,
      implementation = Object.class,
      requiredProperties = {VECTOR_EMBEDDING_FIELD, VECTOR_EMBEDDING_TEXT_FIELD})
  class ConflictingVectorSortSchema {}
}
