package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.ReadCommand;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Command that runs OpenSearch aggregation queries against a collection that has an OpenSearch
 * index. The {@code aggregations} field is an OpenSearch aggregation definition map (required); the
 * optional {@code query} field scopes the aggregation to a matching subset of documents.
 *
 * <p>The result is returned as {@code status.aggregations} — a flat map of aggregation-name → raw
 * OpenSearch aggregation result — rather than a document list.
 */
@Schema(
    description =
        "Command that runs OpenSearch aggregation queries against an OpenSearch-enabled collection")
@JsonTypeName(CommandName.Names.SEARCH_AGGREGATE)
public record SearchAggregateCommand(
    /**
     * OpenSearch aggregation definition map. Each key is the aggregation name; each value is the
     * OpenSearch aggregation body (e.g. {@code {"terms": {"field": "category.keyword"}}}).
     * Required.
     */
    @NotNull @JsonProperty("aggregations") JsonNode aggregations,
    /**
     * Optional OpenSearch query DSL to scope the aggregation. When omitted the aggregation runs
     * over all indexed documents. Same syntax as the {@code $openSearch} filter value.
     */
    @JsonProperty("query") JsonNode query)
    implements CollectionCommand, ReadCommand {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.SEARCH_AGGREGATE;
  }
}
