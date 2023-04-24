package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.ModifyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.Projectable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import javax.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that finds a single JSON document from a collection and deletes it. The deleted document is returned")
@JsonTypeName("findOneAndDelete")
public record FindOneAndDeleteCommand(
    @Valid @JsonProperty("filter") FilterClause filterClause,
    @Valid @JsonProperty("sort") SortClause sortClause,
    @JsonProperty("projection") JsonNode projectionDefinition)
    implements ModifyCommand, Filterable, Projectable {}
