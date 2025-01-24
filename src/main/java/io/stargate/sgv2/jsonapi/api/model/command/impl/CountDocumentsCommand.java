package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.ReadCommand;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterSpec;
import jakarta.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that returns count of documents in a collection based on the collection.")
@JsonTypeName(CommandName.Names.COUNT_DOCUMENTS)
public record CountDocumentsCommand(@Valid @JsonProperty("filter") FilterSpec filterClause)
    implements ReadCommand, NoOptionsCommand, Filterable {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.COUNT_DOCUMENTS;
  }
}
