package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * FindOperation response implementing the {@link CommandResult}.
 *
 * @param docs list of documents
 * @param pageState page state
 * @param singleResponse if the response data should be a single document response
 * @param includeSortVector if the response data should include the sort vector
 * @param vector sort clause vector
 */
public record ReadOperationPage(
    List<ReadDocument> docs,
    String pageState,
    boolean singleResponse,
    boolean includeSortVector,
    float[] vector)
    implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {
    // difference if we have single response target or not
    if (singleResponse) {
      // extract first document from docs with list size check
      JsonNode jsonNode = docs.size() > 0 ? docs.get(0).document() : null;
      return new CommandResult(new CommandResult.SingleResponseData(jsonNode));
    } else {
      // transform docs to json nodes
      final List<JsonNode> jsonNodes = new ArrayList<>();
      for (ReadDocument doc : docs) {
        jsonNodes.add(doc.document());
      }
      Map<CommandStatus, Object> status = null;
      if (includeSortVector) {
        // add sort vector to the response
        status = Map.of(CommandStatus.SORT_VECTOR, vector);
      }
      return new CommandResult(new CommandResult.MultiResponseData(jsonNodes, pageState), status);
    }
  }
}
