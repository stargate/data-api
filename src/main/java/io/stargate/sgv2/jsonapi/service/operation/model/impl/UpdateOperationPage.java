package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.util.ExceptionUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public record UpdateOperationPage(
    int matchedCount,
    int modifiedCount,
    List<ReadAndUpdateOperation.UpdatedDocument> updatedDocuments,
    boolean returnDocs,
    String pagingState)
    implements Supplier<CommandResult> {

  private static final String ERROR = "Failed to update documents with _id %s: %s";

  @Override
  public CommandResult get() {
    final DocumentId[] upsertedId = new DocumentId[1];
    List<JsonNode> updatedDocs = new ArrayList<>(updatedDocuments().size());

    // aggregate the errors by error code or error class
    Multimap<String, ReadAndUpdateOperation.UpdatedDocument> groupedErrorUpdates =
        ArrayListMultimap.create();
    updatedDocuments.forEach(
        update -> {
          if (update.upserted()) upsertedId[0] = update.id();
          if (returnDocs) updatedDocs.add(update.document());
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
              final Collection<ReadAndUpdateOperation.UpdatedDocument> updatedDocuments =
                  groupedErrorUpdates.get(key);
              final List<DocumentId> documentIds =
                  updatedDocuments.stream().map(update -> update.id()).collect(Collectors.toList());
              errors.add(
                  ExceptionUtil.getError(
                      ERROR, documentIds, updatedDocuments.stream().findFirst().get().error()));
            });
    EnumMap<CommandStatus, Object> updateStatus = new EnumMap<>(CommandStatus.class);
    if (upsertedId[0] != null) updateStatus.put(CommandStatus.UPSERTED_ID, upsertedId[0]);
    updateStatus.put(CommandStatus.MATCHED_COUNT, matchedCount());
    updateStatus.put(CommandStatus.MODIFIED_COUNT, modifiedCount());

    if (pagingState != null) {
      updateStatus.put(CommandStatus.MORE_DATA, true);
      updateStatus.put(CommandStatus.PAGE_STATE, pagingState);
    }

    // note that we always target a single document to be returned
    // thus fixed to the SingleResponseData
    if (returnDocs) {
      JsonNode node = updatedDocs.size() > 0 ? updatedDocs.get(0) : null;
      return new CommandResult(
          new CommandResult.SingleResponseData(node),
          updateStatus,
          errors.isEmpty() ? null : errors);
    } else {
      return new CommandResult(null, updateStatus, errors.isEmpty() ? null : errors);
    }
  }
}
