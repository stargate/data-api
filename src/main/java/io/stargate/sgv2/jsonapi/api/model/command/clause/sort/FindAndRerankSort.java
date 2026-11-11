package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.FieldSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.LexicalSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorLegSortExpression;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;

/**
 * The sort clause of a findAndRerank command, read by {@link FindAndRerankSortClauseDeserializer},
 * with one record for each form of sort.
 *
 * <ul>
 *   <li>{@link MultiLeg}: a sort with $hybrid, the command may read with the vector leg and the
 *       lexical leg.
 *   <li>{@link SingleLeg}: any other sort, the command reads with one leg: the vector leg, the
 *       lexical leg or the filter leg.
 * </ul>
 *
 * <p>NOTE: the {@link Schema} only describes $hybrid. Single leg sorts are added to it when they
 * are supported, see GH #2604.
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
public sealed interface FindAndRerankSort extends Recordable
    permits FindAndRerankSort.MultiLeg, FindAndRerankSort.SingleLeg {

  /** A command with no sort, a null sort or an empty sort: the filter leg, without sorting. */
  FilterLeg NO_ARG_SORT = new FilterLeg(List.of(), CommandFeatures.EMPTY);

  /** The metrics features of the sort, recorded when the sort is read. */
  CommandFeatures commandFeatures();

  /**
   * A sort with $hybrid. The command may read with more than one leg, it does not have to: e.g.
   * <code>{"$hybrid": "cheese"}</code> on a collection without a lexical index only reads with the
   * vector leg.
   */
  sealed interface MultiLeg extends FindAndRerankSort permits HybridText, HybridObject {}

  /**
   * <code>{"$hybrid": "cheese"}</code>: the same text for the vector leg and the lexical leg.
   *
   * @param text The trimmed text, <code>null</code> when the user sent blank text.
   */
  record HybridText(@JsonIgnore String text, @JsonIgnore CommandFeatures commandFeatures)
      implements MultiLeg {

    public HybridText {
      Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("text", text).append("commandFeatures", commandFeatures);
    }
  }

  /**
   * <code>{"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}</code>: a separate value for
   * the vector leg and the lexical leg. The vector leg can use $vector instead of $vectorize.
   *
   * <p>Each value can be <code>null</code>. The planner checks that the sort has $vectorize or
   * $vector.
   */
  record HybridObject(
      @JsonIgnore String vectorize,
      @JsonIgnore String lexical,
      @JsonIgnore float[] vector,
      @JsonIgnore CommandFeatures commandFeatures)
      implements MultiLeg {

    public HybridObject {
      Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("vectorize", vectorize)
          .append("lexical", lexical)
          .append("vector", Arrays.toString(vector))
          .append("commandFeatures", commandFeatures);
    }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof HybridObject other)
          && Objects.equals(vectorize, other.vectorize)
          && Objects.equals(lexical, other.lexical)
          && Arrays.equals(vector, other.vector)
          && Objects.equals(commandFeatures, other.commandFeatures);
    }

    @Override
    public int hashCode() {
      return Objects.hash(vectorize, lexical, Arrays.hashCode(vector), commandFeatures);
    }
  }

  /** A sort without $hybrid, the command reads with one leg. */
  sealed interface SingleLeg extends FindAndRerankSort permits VectorLeg, LexicalLeg, FilterLeg {}

  /**
   * <code>{"$vectorize": "cheese"}</code> or <code>{"$vector": [0.1, 0.2]}</code>: reads with the
   * vector leg.
   */
  record VectorLeg(
      @JsonIgnore VectorLegSortExpression sort, @JsonIgnore CommandFeatures commandFeatures)
      implements SingleLeg {

    public VectorLeg {
      Objects.requireNonNull(sort, "sort must not be null");
      Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("sort", sort).append("commandFeatures", commandFeatures);
    }
  }

  /** <code>{"$lexical": "cows"}</code>: reads with the lexical leg. */
  record LexicalLeg(
      @JsonIgnore LexicalSortExpression sort, @JsonIgnore CommandFeatures commandFeatures)
      implements SingleLeg {

    public LexicalLeg {
      Objects.requireNonNull(sort, "sort must not be null");
      Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("sort", sort).append("commandFeatures", commandFeatures);
    }
  }

  /**
   * <code>{"age": -1, "name": 1}</code>: reads the documents the filter matches with the filter
   * leg, sorted by the fields.
   */
  record FilterLeg(
      @JsonIgnore List<FieldSortExpression> sorts, @JsonIgnore CommandFeatures commandFeatures)
      implements SingleLeg {

    public FilterLeg {
      sorts = List.copyOf(Objects.requireNonNull(sorts, "sorts must not be null"));
      Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("sorts", Recordable.copyOf(sorts))
          .append("commandFeatures", commandFeatures);
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
