package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import jakarta.validation.constraints.NotBlank;
import javax.annotation.Nullable;

public record SortExpression(

    // TODO correct typing for the path, we could use some kind of a common class
    //  we also need a validation of a correct path here
    @NotBlank String path,

    // this can be modeled in different ways, would this be enough for now
    boolean ascending,
    @Nullable float[] vector) {

  public static SortExpression sort(String path, boolean ascending) {
    return new SortExpression(path, ascending, null);
  }

  public static SortExpression vsearch(float[] vector) {
    return new SortExpression(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, false, vector);
  }
}
