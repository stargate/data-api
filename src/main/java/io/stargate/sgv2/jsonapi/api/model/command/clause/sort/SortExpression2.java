package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

import io.stargate.sgv2.jsonapi.util.StringUtil;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Arrays;
import java.util.Objects;

/**
 * One expression in a sort clause. There is one type for each kind of sort:
 *
 * <ul>
 *   <li>{@link VectorSimilaritySortExpression}: by vector similarity.
 *       <ul>
 *         <li>{@link VectorizeSortExpression}: for $vectorize.
 *         <li>{@link VectorSortExpression}: for $vector.
 *       </ul>
 *   <li>{@link LexicalSortExpression}: by lexical relevance.
 *   <li>{@link FieldSortExpression}: by the value of a document field.
 * </ul>
 *
 * <p>NOTE: this type will replace {@link SortExpression}.
 *
 * <ul>
 *   <li>Each kind of sort is its own type, so a switch over the expression must handle every kind.
 *   <li><code>SortExpression2</code> is a temporary name. Today only the findAndRerank single leg
 *       sorts use this type. Find and the other commands still use {@link SortExpression}.
 *   <li>When they move to this type, {@link SortExpression} is deleted and this type takes its
 *       name.
 * </ul>
 */
public sealed interface SortExpression2 extends Recordable
    permits SortExpression2.VectorSimilaritySortExpression,
        SortExpression2.LexicalSortExpression,
        SortExpression2.FieldSortExpression {

  /**
   * The path the expression sorts on: $vectorize, $vector or $lexical, or the path of a document
   * field such as <code>address.city</code>.
   */
  String path();

  /** A sort by vector similarity: to the vector of a text, or to a vector. */
  sealed interface VectorSimilaritySortExpression extends SortExpression2
      permits VectorizeSortExpression, VectorSortExpression {}

  /** <code>{"$vectorize": "cheese"}</code> */
  record VectorizeSortExpression(String text) implements VectorSimilaritySortExpression {

    public VectorizeSortExpression {
      StringUtil.requireNonBlank(text, "text");
    }

    @Override
    public String path() {
      return VECTOR_EMBEDDING_TEXT_FIELD;
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("text", text);
    }
  }

  /**
   * <code>{"$vector": [0.1, 0.2, 0.3]}</code> Its dimension is not checked here, that needs the
   * collection.
   */
  record VectorSortExpression(float[] vector) implements VectorSimilaritySortExpression {

    public VectorSortExpression {
      Objects.requireNonNull(vector, "vector must not be null");
    }

    @Override
    public String path() {
      return VECTOR_EMBEDDING_FIELD;
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("vector", Arrays.toString(vector));
    }

    // NOTE: we override equals() and hashCode() to compare the vector by value, not by reference.
    @Override
    public boolean equals(Object obj) {
      return (obj instanceof VectorSortExpression other) && Arrays.equals(vector, other.vector);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(vector);
    }
  }

  /** <code>{"$lexical": "cows"}</code> */
  record LexicalSortExpression(String text) implements SortExpression2 {

    public LexicalSortExpression {
      StringUtil.requireNonBlank(text, "text");
    }

    @Override
    public String path() {
      return LEXICAL_CONTENT_FIELD;
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("text", text);
    }
  }

  /** <code>{"age": -1}</code>: sort by the value of a document field. */
  record FieldSortExpression(String path, boolean ascending) implements SortExpression2 {

    public FieldSortExpression {
      Objects.requireNonNull(path, "path must not be null");
      // NOTE: not isBlank(), a blank name such as " " is a valid field name, see FieldNamingRule
      if (path.isEmpty()) {
        throw new IllegalArgumentException("path must not be empty");
      }
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("path", path).append("ascending", ascending);
    }
  }
}
