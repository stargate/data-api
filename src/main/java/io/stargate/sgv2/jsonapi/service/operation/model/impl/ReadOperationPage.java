package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** FindOperation response implementing the {@link CommandResult} */
public record ReadOperationPage(List<ReadDocument> docs, String pagingState)
    implements Supplier<CommandResult> {

  @Override
  public CommandResult get() {
    final List<JsonNode> jsonNodes = new ArrayList<>();
    docs.stream().forEach(doc -> jsonNodes.add(doc.document()));
    final CommandResult.ResponseData responseData =
        new CommandResult.ResponseData(jsonNodes, pagingState);
    return new CommandResult(responseData);
  }
}
