package io.stargate.sgv2.jsonapi.service.operation.model;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Holds the documents froma read operation to create the {@link CommandResult}
 *
 * @param documentSources The source documents to be included the results, may be empty but never
 *     null. If singleResponse only the first {@link DocumentSource} will be used.
 * @param singleResponse if the response data should be a single document response. Needed in
 *     addition to the list because we need to know if we got zero results for a findOne or a
 *     findMany.
 * @param nextPageState pagination state, maybe null
 * @param includeSortVector if the response data should include the sort vector, used in conjunction
 *     with the vector param so we always include the {@link CommandStatus#SORT_VECTOR} status when
 *     set.
 * @param vector sort clause vector, no checking done.
 */
public record ReadOperationPage(
    List<? extends DocumentSource> documentSources,
    boolean singleResponse,
    String nextPageState,
    boolean includeSortVector,
    float[] vector)
    implements Supplier<CommandResult> {

  @Override
  public CommandResult get() {

    Map<CommandStatus, Object> status =
        includeSortVector ? Collections.singletonMap(CommandStatus.SORT_VECTOR, vector) : null;

    List<JsonNode> jsonDocs =
        documentSources.stream()
            .limit(singleResponse ? 1 : Long.MAX_VALUE)
            .map(DocumentSource::get)
            .toList();

    var responseData =
        singleResponse
            ? new CommandResult.SingleResponseData(jsonDocs.isEmpty() ? null : jsonDocs.get(0))
            : new CommandResult.MultiResponseData(jsonDocs, nextPageState);

    return new CommandResult(responseData, status);
  }
}
