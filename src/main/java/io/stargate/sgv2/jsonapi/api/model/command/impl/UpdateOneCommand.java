package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.ReadCommand;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that finds a single JSON document from a collection and updates the value provided in the update clause.")
@JsonTypeName("updateOne")
public record UpdateOneCommand(
    @Valid @JsonProperty("filter") FilterClause filterClause,
    @NotNull @Valid @JsonProperty("update") UpdateClause updateClause,
    @Valid @JsonProperty("sort") SortClause sortClause,
    @Nullable Options options)
    implements ReadCommand, Filterable {

  @Schema(name = "UpdateOneCommand.Options", description = "Options for updating a document.")
  public record Options(
      @Schema(
              description =
                  "When `true`, if no documents match the `filter` clause the command will create a new _empty_ document and apply the `update` clause and all equality filters to the empty document.",
              defaultValue = "false")
          boolean upsert) {}
}
