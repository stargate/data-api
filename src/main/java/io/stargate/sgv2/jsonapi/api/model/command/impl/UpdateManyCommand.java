package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.ReadCommand;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import javax.annotation.Nullable;
import javax.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that finds documents from a collection and updates it with the values provided in the update clause.")
@JsonTypeName("updateMany")
public record UpdateManyCommand(
    @Valid @JsonProperty("filter") FilterClause filterClause,
    @Valid @JsonProperty("update") UpdateClause updateClause,
    @Nullable Options options)
    implements ReadCommand, Filterable {
  public record Options(boolean upsert) {}
}
