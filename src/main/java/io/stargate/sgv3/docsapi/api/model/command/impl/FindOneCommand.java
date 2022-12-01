package io.stargate.sgv3.docsapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv3.docsapi.api.model.command.ReadCommand;
import io.stargate.sgv3.docsapi.api.model.command.clause.sort.SortClause;
import javax.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that finds a single JSON document from a collection.")
@JsonTypeName("findOne")
public record FindOneCommand(@Valid @JsonProperty("sort") SortClause sortClause)
    implements ReadCommand {}
