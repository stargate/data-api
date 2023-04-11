package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.ModifyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Representation of the deleteOne API {@link Command}.
 *
 * @param filterClause {@link FilterClause} used to identify a document.
 */
@Schema(description = "Command that finds a single document and deletes it from a collection")
@JsonTypeName("deleteOne")
public record DeleteOneCommand(
    @NotNull
        @Schema(
            description = "Filter clause based on which document is identified",
            implementation = FilterClause.class)
        @Valid
        @JsonProperty("filter")
        FilterClause filterClause)
    implements ModifyCommand, NoOptionsCommand, Filterable {}
