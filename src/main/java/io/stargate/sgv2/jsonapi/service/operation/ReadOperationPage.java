package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import java.util.List;
import java.util.function.Supplier;

/**
 * NOTE: AARON FEB 5 2025 - Used by collections leave in place.
 * 
 * Holds the documents from a read operation to create the {@link CommandResult}
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

    var builder =
        singleResponse
            ? CommandResult.singleDocumentBuilder(false, false)
            : CommandResult.multiDocumentBuilder(false, false);

    if (includeSortVector) {
      builder.addStatus(CommandStatus.SORT_VECTOR, vector);
    } else {
      builder.nextPageState(nextPageState);
    }

    documentSources.stream()
        .limit(singleResponse ? 1 : Long.MAX_VALUE)
        .map(DocumentSource::get)
        .forEach(builder::addDocument);

    return builder.build();
  }
}
