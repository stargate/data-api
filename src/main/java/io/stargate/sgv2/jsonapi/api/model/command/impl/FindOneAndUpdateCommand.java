package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.ReadCommand;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Pattern;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that finds a single JSON document from a collection and updates the value provided in the update clause.")
@JsonTypeName("findOneAndUpdate")
public record FindOneAndUpdateCommand(
    @Valid @JsonProperty("filter") FilterClause filterClause,
    @Valid @JsonProperty("update") UpdateClause updateClause,
    @Valid @Nullable Options options)
    implements ReadCommand, Filterable {
  public record Options(
      @Valid
          @Nullable
          @Pattern(
              regexp = "(after|before)",
              message = "returnDocument value can only be before or after")
          String returnDocument,
      boolean upsert) {}
}
