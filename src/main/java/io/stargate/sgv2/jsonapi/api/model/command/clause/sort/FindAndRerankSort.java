package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.FieldSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.LexicalSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorLegSortExpression;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The parsed sort of a findAndRerank command, with one record for each form of sort.
 *
 * <ul>
 *   <li>{@link MultiLeg}: a sort with $hybrid.
 *   <li>{@link SingleLeg}: any other sort.
 * </ul>
 */
public sealed interface FindAndRerankSort extends Recordable
    permits FindAndRerankSort.MultiLeg, FindAndRerankSort.SingleLeg {

  /** A sort with $hybrid. It always has a value for the vector leg. */
  sealed interface MultiLeg extends FindAndRerankSort permits HybridText, HybridObject {}

  /**
   * <code>{"$hybrid": "cheese"}</code>: the same text for the vector leg and the lexical leg.
   *
   * @param text The trimmed text. It is never blank.
   */
  record HybridText(String text) implements MultiLeg {

    public HybridText {
      Objects.requireNonNull(text, "text must not be null");
      if (text.isBlank()) {
        throw new IllegalArgumentException("text must not be blank");
      }
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("text", text);
    }
  }

  /**
   * <code>{"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}</code>: a separate value for
   * the vector leg and the lexical leg. The vector leg can use $vector instead of $vectorize.
   *
   * <p>At least one of $vectorize and $vector is set. $lexical can be <code>null</code>.
   */
  record HybridObject(String vectorize, String lexical, float[] vector) implements MultiLeg {

    public HybridObject {
      if (vectorize == null && vector == null) {
        throw new IllegalArgumentException("vectorize or vector must not be null");
      }
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("vectorize", vectorize)
          .append("lexical", lexical)
          .append("vector", Arrays.toString(vector));
    }

    /** Override to do a value equality check on the vector. */
    @Override
    public boolean equals(Object obj) {
      return (obj instanceof HybridObject other)
          && Objects.equals(vectorize, other.vectorize)
          && Objects.equals(lexical, other.lexical)
          && Arrays.equals(vector, other.vector);
    }

    /** Override to do a value equality hash on the vector. */
    @Override
    public int hashCode() {
      return Objects.hash(vectorize, lexical, Arrays.hashCode(vector));
    }
  }

  /** A sort without $hybrid, the command reads with one leg. */
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
