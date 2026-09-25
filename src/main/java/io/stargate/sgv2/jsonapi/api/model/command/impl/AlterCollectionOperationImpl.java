package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** Each operation that {@link AlterCollectionCommand} understands is represented by a record. */
public class AlterCollectionOperationImpl {

  @Schema(description = "Operation to enable the lexical search feature on a collection.")
  @JsonTypeName("enableLexical")
  public record EnableLexical(
      @Schema(
              description =
                  "Analyzer to use for '$lexical' field: either String (name of a pre-defined analyzer), or JSON Object to specify custom one. Default: 'standard'.",
              defaultValue = "standard",
              oneOf = {String.class, Map.class})
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @JsonProperty("analyzer")
          @Nullable
          JsonNode analyzerDef)
      implements AlterCollectionOperation {}
}
