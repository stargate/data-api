package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import jakarta.validation.constraints.NotBlank;
import java.util.Objects;
import javax.annotation.Nullable;

public record SortExpression(

    // TODO correct typing for the path, we could use some kind of a common class
    //  we also need a validation of a correct path here
    @NotBlank String path,

    // this can be modeled in different ways, would this be enough for now
    boolean ascending,
    @Nullable float[] vector,
    @Nullable String vectorize,
    @Nullable String bm25Query) {

  // TODO: either remove the static factories or make this a class, as a record the ctor is public

  public static SortExpression sort(String path, boolean ascending) {
    return new SortExpression(path, ascending, null, null, null);
  }

  public static SortExpression vsearch(float[] vector) {
    return new SortExpression(VECTOR_EMBEDDING_FIELD, false, vector, null, null);
  }

  public static SortExpression vectorizeSearch(String vectorize) {
    return new SortExpression(VECTOR_EMBEDDING_TEXT_FIELD, false, null, vectorize, null);
  }

  public static SortExpression collectionLexicalSort(String bm25Query) {
    Objects.requireNonNull(bm25Query, "Lexical query cannot be null");
    return new SortExpression(LEXICAL_CONTENT_FIELD, false, null, null, bm25Query);
  }

  public static SortExpression tableLexicalSort(String path, String bm25Query) {
    Objects.requireNonNull(path, "Path cannot be null");
    Objects.requireNonNull(bm25Query, "Lexical query cannot be null");
    return new SortExpression(path, false, null, null, bm25Query);
  }

  /**
   * Create a sort that is used when sorting a table column by a vector.
   *
   * <p>aaron -30-oct, this is a bit of a hack, but it is a quick way to support sorting by a vector
   * for tables
   */
  public static SortExpression tableVectorSort(String path, float[] vector) {
    return new SortExpression(path, false, vector, null, null);
  }

  /**
   * Create a sort that is used when sorting a table column by a vectorize string .
   *
   * <p>aaron -4-nov, this is a bit of a hack, but it is a quick way to support sorting by a
   * vectorize for tables
   */
  public static SortExpression tableVectorizeSort(String path, String vectorize) {
    return new SortExpression(path, false, null, vectorize, null);
  }

  public CqlIdentifier pathAsCqlIdentifier() {
    return cqlIdentifierFromUserInput(path);
  }

  public boolean isBM25Search() {
    return bm25Query != null;
  }

  public boolean isTableVectorSort() {
    return !pathIs$VectorNames() && vector != null;
  }

  public boolean isTableVectorizeSort() {
    return !pathIs$VectorNames() && vectorize != null;
  }

  private boolean pathIs$VectorNames() {
    return (Objects.equals(path, VECTOR_EMBEDDING_FIELD)
        || Objects.equals(path, VECTOR_EMBEDDING_TEXT_FIELD));
  }
}
