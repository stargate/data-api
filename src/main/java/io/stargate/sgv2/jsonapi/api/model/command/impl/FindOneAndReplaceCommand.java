package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.ModifyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.Projectable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that finds a single JSON document from a collection and replaces it with the replacement document.")
@JsonTypeName("findOneAndReplace")
public record FindOneAndReplaceCommand(
    @Valid @JsonProperty("filter") FilterClause filterClause,
    @Valid @JsonProperty("sort") SortClause sortClause,
    @JsonProperty("projection") JsonNode projectionDefinition,
    @NotNull @Valid @JsonProperty("replacement") ObjectNode replacementDocument,
    @Valid @Nullable Options options)
    implements ModifyCommand, Filterable, Projectable {

  @Schema(
      name = "FindOneAndReplaceCommand.Options",
      description = "Options for `findOneAndReplace` command.")
  public record Options(
      @Nullable
          @Pattern(
              regexp = "(after|before)",
              message = "returnDocument value can only be 'before' or 'after'")
          @Schema(
              description =
                  "Specifies which document to perform the projection on. If `before` the projection is performed on the document before the replacement is applied, if `after` the document projection is from the document after the replacement.",
              defaultValue = "before")
          String returnDocument) {}
}
