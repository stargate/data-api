package io.stargate.sgv3.docsapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv3.docsapi.api.model.command.Filterable;
import io.stargate.sgv3.docsapi.api.model.command.ReadCommand;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.sort.SortClause;
import javax.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that finds a single JSON document from a collection.")
@JsonTypeName("find")
public record FindCommand(
    @Valid @JsonProperty("filter") FilterClause filterClause,
    @Valid @JsonProperty("sort") SortClause sortClause,
    @Valid @JsonProperty("options") Options options)
    implements ReadCommand, Filterable {

  public record Options(@Valid int limit, @Valid String pagingState, @Valid int pageSize) {}
}
