package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import jakarta.validation.constraints.NotBlank;
import javax.annotation.Nullable;

import java.util.Objects;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;

public record SortExpression(

    // TODO correct typing for the path, we could use some kind of a common class
    //  we also need a validation of a correct path here
    @NotBlank String path,

    // this can be modeled in different ways, would this be enough for now
    boolean ascending,
    @Nullable float[] vector,
    @Nullable String vectorize) {

  // TODO: either remove the static factories or make this a class, as a record the ctor is public

  public static SortExpression sort(String path, boolean ascending) {
    return new SortExpression(path, ascending, null, null);
  }

  public static SortExpression vsearch(float[] vector) {
    return new SortExpression(VECTOR_EMBEDDING_FIELD, false, vector, null);
  }

  public static SortExpression vectorizeSearch(String vectorize) {
    return new SortExpression(VECTOR_EMBEDDING_TEXT_FIELD, false, null, vectorize);
  }

  public static SortExpression tableVectorSort(String path, float[] vector) {
    return new SortExpression(
        path, false, vector, null);
  }

  public boolean isTableVectorSort() {
    return !pathIs$Name() && vector != null;
  }

  private boolean pathIs$Name() {
    return (!Objects.equals(path, VECTOR_EMBEDDING_FIELD) && !Objects.equals(path, VECTOR_EMBEDDING_TEXT_FIELD));
  }
}
