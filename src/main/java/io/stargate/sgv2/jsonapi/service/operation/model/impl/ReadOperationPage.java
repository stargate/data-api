package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** FindOperation response implementing the {@link CommandResult} */
public record ReadOperationPage(List<ReadDocument> docs, int count, String pagingState)
    implements Supplier<CommandResult> {

  public ReadOperationPage(List<ReadDocument> docs, String pagingState) {
    this(docs, docs.size(), pagingState);
  }

  public ReadOperationPage(int count) {
    this(null, count, null);
  }

  @Override
  public CommandResult get() {
    if (docs == null) {
      return new CommandResult(Map.of(CommandStatus.COUNTED_DOCUMENT, count()));
    } else {
      final List<JsonNode> jsonNodes = new ArrayList<>();
      docs.stream().forEach(doc -> jsonNodes.add(doc.document()));
      final CommandResult.ResponseData responseData =
          new CommandResult.ResponseData(jsonNodes, pagingState);
      return new CommandResult(responseData);
    }
  }
}
