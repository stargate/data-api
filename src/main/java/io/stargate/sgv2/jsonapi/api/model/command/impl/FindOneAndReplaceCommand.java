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
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import javax.annotation.Nullable;
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
          String returnDocument,
      @Schema(
              description =
                  "When `true`, if no documents match the `filter` clause the command will create a new _empty_ document and apply all _id filter and replacement document to the empty document.",
              defaultValue = "false")
          boolean upsert) {}
}
