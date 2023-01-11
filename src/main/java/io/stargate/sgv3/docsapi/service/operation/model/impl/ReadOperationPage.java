package io.stargate.sgv3.docsapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import javax.inject.Inject;

/** FindOperation response implementing the {@link CommandResult} */
public class ReadOperationPage implements Supplier<CommandResult> {
  @Inject ObjectMapper objectMapper;

  private List<ReadDocument> docs;
  private String pagingState;

  public ReadOperationPage(List<ReadDocument> docs, String pagingState) {
    this.docs = docs;
    this.pagingState = pagingState;
  }

  @Override
  public CommandResult get() {
    final List<JsonNode> jsonNodes = new ArrayList<>();
    docs.stream().forEach(doc -> jsonNodes.add(doc.document()));
    final CommandResult.ResponseData responseData =
        new CommandResult.ResponseData(jsonNodes, pagingState);
    return new CommandResult(responseData);
  }
}
