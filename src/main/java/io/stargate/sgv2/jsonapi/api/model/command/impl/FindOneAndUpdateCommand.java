package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.ReadCommand;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that finds a single JSON document from a collection and updates the value provided in the update clause.")
@JsonTypeName("findOneAndUpdate")
public record FindOneAndUpdateCommand(
    @Valid @JsonProperty("filter") FilterClause filterClause,
    @JsonProperty("projection") JsonNode projectionDefinition,
    @NotNull @Valid @JsonProperty("update") UpdateClause updateClause,
    @Valid @Nullable Options options)
    implements ReadCommand, Filterable {

  @Schema(
      name = "FindOneAndUpdateCommand.Options",
      description = "Options for `findOneAndUpdate` command.")
  public record Options(
      @Nullable
          @Pattern(
              regexp = "(after|before)",
              message = "returnDocument value can only be 'before' or 'after'")
          @Schema(
              description =
                  "Specifies which document to perform the projection on. If `before` the projection is performed on the document before the update is applied, if `after` the document projection is from the document after the update.",
              defaultValue = "before")
          String returnDocument,
      @Schema(
              description =
                  "When `true`, if no documents match the `filter` clause the command will create a new _empty_ document and apply the `update` clause and all equality filters to the empty document.",
              defaultValue = "false")
          boolean upsert) {}
}
