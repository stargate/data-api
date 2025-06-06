package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that finds a single JSON document from a collection and replaces it with the replacement document.")
@JsonTypeName(CommandName.Names.FIND_ONE_AND_REPLACE)
public record FindOneAndReplaceCommand(
    @Valid @JsonProperty("filter") FilterDefinition filterDefinition,
    @Valid @JsonProperty("sort") SortDefinition sortDefinition,
    @JsonProperty("projection") JsonNode projectionDefinition,
    @NotNull @Valid @JsonProperty("replacement") ObjectNode replacementDocument,
    @Valid @Nullable Options options)
    implements ModifyCommand, Filterable, Projectable, Sortable {

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

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND_ONE_AND_REPLACE;
  }
}
