package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

import io.stargate.sgv2.jsonapi.util.StringUtil;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Arrays;
import java.util.Objects;

/**
 * One expression in a sort clause, with one record for each kind of sort: by vector similarity, by
 * lexical relevance, or by the value of a document field. {@link FindAndRerankSortParser} makes
 * them for the single leg sorts of findAndRerank.
 *
 * <p>NOTE: this replaces {@link SortExpression}, one class with a nullable field for each kind of
 * sort: users of it must check the fields in the right order to know what kind of sort they have.
 * Here the kind of sort is the type, each record has only the values of its kind, and a switch over
 * the expression must handle every kind or it does not compile.
 *
 * <p>NOTE: <code>SortExpression2</code> is a temporary name while both types are in use: the
 * findAndRerank single leg sorts use this type, find and the other commands still use {@link
 * SortExpression}. When they move to this type, {@link SortExpression} is deleted and this type
 * takes its name.
 */
public sealed interface SortExpression2 extends Recordable
    permits SortExpression2.VectorLegSortExpression,
        SortExpression2.LexicalSortExpression,
        SortExpression2.FieldSortExpression {

  /**
   * The path the expression sorts on: $vectorize, $vector or $lexical, or the path of a document
   * field such as <code>address.city</code>.
   */
  String path();

  /** A sort by vector similarity: to the vector of a text, or to a vector. */
  sealed interface VectorLegSortExpression extends SortExpression2
      permits VectorizeSortExpression, VectorSortExpression {}

  /**
   * <code>{"$vectorize": "cheese"}</code>: sort by similarity to the vector of the text, made by
   * the vectorize service of the collection.
   *
   * @param text The text to vectorize, not blank.
   */
  record VectorizeSortExpression(String text) implements VectorLegSortExpression {

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
   * <code>{"$vector": [0.1, 0.2, 0.3]}</code>: sort by similarity to the vector.
   *
   * @param vector The vector. Its dimension is not checked here, that needs the collection.
   */
  record VectorSortExpression(float[] vector) implements VectorLegSortExpression {

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

    /** Override to do a value equality check on the vector. */
    @Override
    public boolean equals(Object obj) {
      return (obj instanceof VectorSortExpression other) && Arrays.equals(vector, other.vector);
    }

    /** Override to do a value equality hash on the vector. */
    @Override
    public int hashCode() {
      return Arrays.hashCode(vector);
    }
  }

  /**
   * <code>{"$lexical": "cows"}</code>: sort by the lexical (BM25) relevance of the documents to the
   * text.
   *
   * @param text The text to match, not blank.
   */
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

  /**
   * <code>{"age": -1}</code>: sort by the value of a document field.
   *
   * @param path The path of the field, e.g. <code>address.city</code>.
   * @param ascending <code>true</code> for ascending, from <code>1</code> in the sort clause,
   *     <code>false</code> for descending, from <code>-1</code>.
   */
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
