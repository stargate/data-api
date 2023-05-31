package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.ReadCommand;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that finds documents from a collection and updates it with the values provided in the update clause.")
@JsonTypeName("updateMany")
public record UpdateManyCommand(
    @Valid @JsonProperty("filter") FilterClause filterClause,
    @NotNull @Valid @JsonProperty("update") UpdateClause updateClause,
    @Nullable Options options)
    implements ReadCommand, Filterable {

  @Schema(name = "UpdateManyCommand.Options", description = "Options for updating many documents.")
  public record Options(
      @Schema(
              description =
                  "When `true`, if no documents match the `filter` clause the command will create a new _empty_ document and apply the `update` clause and all equality filters to the empty document.",
              defaultValue = "false")
          boolean upsert) {}
}
