package io.stargate.sgv2.jsonapi.api.v1.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.quarkiverse.mcp.server.ToolResponse;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import jakarta.inject.Inject;

/**
 * MCP tool provider for collection-level (and table-level) commands {@link
 * io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand}. These correspond to the {@link
 * io.stargate.sgv2.jsonapi.api.v1.CollectionResource} REST endpoint.
 */
public class CollectionCommandTools {

  @Inject McpResource mcpResource;

  @Tool(description = "Find a single document in a collection matching a filter")
  public Uni<ToolResponse> find(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "filter", required = false) FilterDefinition filter,
      @ToolArg(description = "projection", required = false) JsonNode projection,
      @ToolArg(description = "sort", required = false) SortDefinition sort,
      @ToolArg(description = "options", required = false) FindCommand.Options options) {

    var command = new FindCommand(filter, projection, sort, options);
    return mcpResource.processCommand(
        mcpResource.buildCollectionContext(keyspace, collection, command), command);
  }
}
