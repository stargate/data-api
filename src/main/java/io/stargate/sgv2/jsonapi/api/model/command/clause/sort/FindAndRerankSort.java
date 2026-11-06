package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.NullNode;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;

/** The sort clause of a findAndRerank command. */
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

  @JsonIgnore
  CommandFeatures commandFeatures();

  sealed interface MultiLeg extends FindAndRerankSort permits HybridText, HybridObject {}

  record HybridText(@JsonIgnore String text, @JsonIgnore CommandFeatures commandFeatures)
      implements MultiLeg {
    public HybridText {
      Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
    }
  }

  record HybridObject(
      @JsonIgnore String vectorize,
      @JsonIgnore String lexical,
      @JsonIgnore float[] vector,
      @JsonIgnore CommandFeatures commandFeatures)
      implements MultiLeg {
    public HybridObject {
      Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
      vector = vector == null ? null : vector.clone();
    }

    @Override
    public float[] vector() {
      return vector == null ? null : vector.clone();
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof HybridObject other
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

  sealed interface SingleLeg extends FindAndRerankSort
      permits UnresolvedSingleLeg, LexicalLeg, VectorLeg, FilterLeg {}

  /** Values remain raw until the planner validates them with find's sort builder. */
  record UnresolvedSingleLeg(@JsonIgnore JsonNode sort, @JsonIgnore CommandFeatures commandFeatures)
      implements SingleLeg {
    public UnresolvedSingleLeg {
      Objects.requireNonNull(sort, "sort must not be null");
      Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
    }
  }

  record LexicalLeg(
      @JsonIgnore List<SortExpression2> sortExpressions,
      @JsonIgnore CommandFeatures commandFeatures)
      implements SingleLeg {
    public LexicalLeg {
      sortExpressions = List.copyOf(sortExpressions);
      Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
      if (sortExpressions.size() != 1
          || !(sortExpressions.getFirst() instanceof SortExpression2.LexicalSe)) {
        throw new IllegalArgumentException("LexicalLeg requires one lexical expression");
      }
    }
  }

  record VectorLeg(
      @JsonIgnore List<SortExpression2> sortExpressions,
      @JsonIgnore CommandFeatures commandFeatures)
      implements SingleLeg {
    public VectorLeg {
      sortExpressions = List.copyOf(sortExpressions);
      Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
      if (sortExpressions.size() != 1
          || !(sortExpressions.getFirst() instanceof SortExpression2.VectorSe
              || sortExpressions.getFirst() instanceof SortExpression2.VectorizeSe)) {
        throw new IllegalArgumentException("VectorLeg requires one vector or vectorize expression");
      }
    }
  }

  record FilterLeg(
      @JsonIgnore List<SortExpression2> sortExpressions,
      @JsonIgnore CommandFeatures commandFeatures)
      implements SingleLeg {
    public FilterLeg {
      sortExpressions = List.copyOf(sortExpressions);
      Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
      if (sortExpressions.stream()
          .anyMatch(expression -> !(expression instanceof SortExpression2.FieldSe))) {
        throw new IllegalArgumentException("FilterLeg accepts only field expressions");
      }
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

  FindAndRerankSort NO_ARG_SORT = new UnresolvedSingleLeg(NullNode.instance, CommandFeatures.EMPTY);

  @Override
  default DataRecorder recordTo(DataRecorder dataRecorder) {
    dataRecorder.append("sortType", getClass().getSimpleName());
    switch (this) {
      case HybridText sort -> dataRecorder.append("text", sort.text());
      case HybridObject sort ->
          dataRecorder
              .append("vectorize", sort.vectorize())
              .append("lexical", sort.lexical())
              .append("vector", Arrays.toString(sort.vector()));
      case UnresolvedSingleLeg sort -> dataRecorder.append("sort", sort.sort());
      case LexicalLeg sort -> dataRecorder.append("sortExpressions", sort.sortExpressions());
      case VectorLeg sort -> dataRecorder.append("sortExpressions", sort.sortExpressions());
      case FilterLeg sort -> dataRecorder.append("sortExpressions", sort.sortExpressions());
    }
    return dataRecorder.append("commandFeatures", commandFeatures());
  }
}
