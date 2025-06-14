package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.util.ExceptionUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public record UpdateCollectionOperationPage(
    int matchedCount,
    int modifiedCount,
    List<ReadAndUpdateCollectionOperation.UpdatedDocument> updatedDocuments,
    boolean returnDocs,
    String pagingState)
    implements Supplier<CommandResult> {

  private static final String ERROR = "Failed to update documents with _id %s: %s";

  @Override
  public CommandResult get() {
    final DocumentId[] upsertedId = new DocumentId[1];
    List<JsonNode> updatedDocs = new ArrayList<>(updatedDocuments().size());

    var builder =
        returnDocs
            ? CommandResult.singleDocumentBuilder(false, RequestTracing.NO_OP)
            : CommandResult.statusOnlyBuilder(false, RequestTracing.NO_OP);

    // aggregate the errors by error code or error class
    Multimap<String, ReadAndUpdateCollectionOperation.UpdatedDocument> groupedErrorUpdates =
        ArrayListMultimap.create();
    updatedDocuments.forEach(
        update -> {
          if (update.upserted()) {
            upsertedId[0] = update.id();
          }
          if (returnDocs) {
            updatedDocs.add(update.document());
          }
          //
          if (update.error() != null) {
            String key = ExceptionUtil.getThrowableGroupingKey(update.error());
            groupedErrorUpdates.put(key, update);
          }
        });
    // Create error by error code or error class
    List<CommandResult.Error> errors = new ArrayList<>(groupedErrorUpdates.size());
    groupedErrorUpdates
        .keySet()
        .forEach(
            key -> {
              final Collection<ReadAndUpdateCollectionOperation.UpdatedDocument> updatedDocuments =
                  groupedErrorUpdates.get(key);
              final List<DocumentId> documentIds =
                  updatedDocuments.stream().map(update -> update.id()).collect(Collectors.toList());
              errors.add(
                  ExceptionUtil.getError(
                      ERROR, documentIds, updatedDocuments.stream().findFirst().get().error()));
            });

    if (upsertedId[0] != null) {
      builder.addStatus(CommandStatus.UPSERTED_ID, upsertedId[0]);
    }
    builder.addStatus(CommandStatus.MATCHED_COUNT, matchedCount());
    builder.addStatus(CommandStatus.MODIFIED_COUNT, modifiedCount());

    if (pagingState != null) {
      builder.addStatus(CommandStatus.MORE_DATA, true);
      builder.addStatus(CommandStatus.PAGE_STATE, pagingState);
    }

    // aaron - 9-oct-2024 - these two line comments were below....
    // note that we always target a single document to be returned
    // thus fixed to the SingleResponseData
    // ... but I think they are wrong, because it would previously pass null for the data if
    // returnedDocs was false
    // so at the top of the function we make the appropriate builder
    // (comment could have been about how it handles have zero docs and returnDocs is true)
    if (returnDocs && !updatedDocs.isEmpty()) {
      builder.addDocument(updatedDocs.getFirst());
    }
    builder.addCommandResultError(errors);
    return builder.build();
  }
}
