package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.FieldSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.LexicalSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorLegSortExpression;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The sort of a findAndRerank command after {@link FindAndRerankSortParser} parsed it, with one
 * record for each form of sort.
 *
 * <ul>
 *   <li>{@link MultiLeg}: a sort with $hybrid, the command may read with the vector leg and the
 *       lexical leg.
 *   <li>{@link SingleLeg}: any other sort, the command reads with one leg: the vector leg, the
 *       lexical leg or the filter leg.
 * </ul>
 */
public sealed interface FindAndRerankSort extends Recordable
    permits FindAndRerankSort.MultiLeg, FindAndRerankSort.SingleLeg {

  /**
   * A sort with $hybrid. The command may read with more than one leg, it does not have to: e.g.
   * <code>{"$hybrid": "cheese"}</code> on a collection without a lexical index only reads with the
   * vector leg.
   */
  sealed interface MultiLeg extends FindAndRerankSort, FindAndRerankSortDefinition
      permits HybridText, HybridObject {}

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
   * <code>{"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}</code>, or with $vector: a
   * value for each leg. Which values must be set is checked when the command is planned.
   *
   * @param vectorize <code>null</code> when $vectorize was left out, null or blank, otherwise the
   *     trimmed text.
   * @param lexical <code>null</code> when $lexical was left out, null or blank, otherwise the
   *     trimmed text.
   * @param vector <code>null</code> when $vector was left out or null, otherwise the vector.
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

    /** Override to do a value equality check on the vector. */
    @Override
    public boolean equals(Object obj) {
      return (obj instanceof HybridObject other)
          && Objects.equals(vectorize, other.vectorize)
          && Objects.equals(lexical, other.lexical)
          && Arrays.equals(vector, other.vector)
          && Objects.equals(commandFeatures, other.commandFeatures);
    }

    /** Override to do a value equality hash on the vector. */
    @Override
    public int hashCode() {
      return Objects.hash(vectorize, lexical, Arrays.hashCode(vector), commandFeatures);
    }
  }

  /**
   * A sort without $hybrid, the command reads with one leg. It has the syntax of the find sort, see
   * {@link FindAndRerankSortParser}. The planner rejects every single leg sort until GH #2602 and
   * GH #2603.
   */
  sealed interface SingleLeg extends FindAndRerankSort permits VectorLeg, LexicalLeg, FilterLeg {}

  /**
   * <code>{"$vectorize": "cheese"}</code> or <code>{"$vector": [0.1, 0.2]}</code>: reads with the
   * vector leg.
   */
  record VectorLeg(VectorLegSortExpression sort) implements SingleLeg {

    public VectorLeg {
      Objects.requireNonNull(sort, "sort must not be null");
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("sort", sort);
    }
  }

  /** <code>{"$lexical": "cows"}</code>: reads with the lexical leg. */
  record LexicalLeg(LexicalSortExpression sort) implements SingleLeg {

    public LexicalLeg {
      Objects.requireNonNull(sort, "sort must not be null");
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("sort", sort);
    }
  }

  /**
   * <code>{"age": -1, "name": 1}</code>: reads the documents the filter matches with the filter
   * leg, sorted by the fields.
   *
   * @param sorts The field sorts in the order the user sent them. Empty when the command has no
   *     sort, a null sort or an empty sort.
   */
  record FilterLeg(List<FieldSortExpression> sorts) implements SingleLeg {

    public FilterLeg {
      sorts = List.copyOf(Objects.requireNonNull(sorts, "sorts must not be null"));
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("sorts", Recordable.copyOf(sorts));
    }
  }
}
