package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;

/**
 * @param vectorizeSort If <code>null</code> then the $vectorize field was not present in the sort
 *     clause, OR was present but set to JSON null OR it was blank string, otherwise it is the value
 *     provided.
 * @param lexicalSort If <code>null</code> then the $lexical field was not present in the sort
 *     clause, OR was present but set to JSON null OR it was blank string, otherwise it is the value
 *     provided.
 * @param vectorSort If <code>null</code> then the vector field was not present in the sort clause,
 *     or present and JSON null, otherwise it is the value provided.
 */
@JsonDeserialize(using = FindAndRerankSortClauseDeserializer.class)
@Schema(
    name = "FindAndRerankSort",
    type = SchemaType.OBJECT,
    implementation = Object.class,
    nullable = true,
    additionalProperties = Schema.False.class,
    properties =
        @SchemaProperty(
            name = HYBRID_FIELD,
            anyOf = {String.class, FindAndRerankSort.HybridSortSchema.class}),
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
public record FindAndRerankSort(
    @Schema(hidden = true) String vectorizeSort,
    @Schema(hidden = true) String lexicalSort,
    @Schema(hidden = true) float[] vectorSort,
    @JsonIgnore CommandFeatures commandFeatures)
    implements Recordable {

  // These classes carry nested schema annotations and are never instantiated by the parser.
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

  @Schema(
      name = "BinaryVector",
      type = SchemaType.OBJECT,
      implementation = Object.class,
      nullable = true,
      additionalProperties = Schema.False.class,
      requiredProperties = BINARY_VECTOR_TEXT_FIELD,
      properties = @SchemaProperty(name = BINARY_VECTOR_TEXT_FIELD, type = SchemaType.STRING))
  public static class BinaryVectorSchema {}

  @Schema(
      name = "ConflictingVectorSort",
      type = SchemaType.OBJECT,
      implementation = Object.class,
      requiredProperties = {VECTOR_EMBEDDING_FIELD, VECTOR_EMBEDDING_TEXT_FIELD})
  public static class ConflictingVectorSortSchema {}

  public static final FindAndRerankSort NO_ARG_SORT =
      new FindAndRerankSort(null, null, null, CommandFeatures.EMPTY);

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("vectorizeSort", vectorizeSort)
        .append("lexicalSort", lexicalSort)
        .append("vectorSort", Arrays.toString(vectorSort))
        .append("commandFeatures", commandFeatures);
  }

  /**
   * Override to do a value equality check on the vector
   *
   * @param obj the reference object with which to compare.
   * @return
   */
  @Override
  public boolean equals(Object obj) {
    return (obj instanceof FindAndRerankSort other)
        && Objects.equals(vectorizeSort, other.vectorizeSort)
        && Objects.equals(lexicalSort, other.lexicalSort)
        && Arrays.equals(vectorSort, other.vectorSort)
        && Objects.equals(commandFeatures, other.commandFeatures);
  }

  /** Override to do a value equality hash on the vector */
  @Override
  public int hashCode() {
    return Objects.hash(vectorizeSort, lexicalSort, Arrays.hashCode(vectorSort), commandFeatures);
  }
}
