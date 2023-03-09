package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public record UpdateOperationPage(
    int matchedCount,
    int modifiedCount,
    List<ReadAndUpdateOperation.UpdatedDocument> updatedDocuments,
    boolean returnDocs,
    boolean moreDataFlag)
    implements Supplier<CommandResult> {
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
            String key = update.error().getClass().getSimpleName();
            if (update.error() instanceof JsonApiException jae) key = jae.getErrorCode().name();
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
                  getError(documentIds, updatedDocuments.stream().findFirst().get().error()));
            });
    EnumMap<CommandStatus, Object> updateStatus = new EnumMap<>(CommandStatus.class);
    if (upsertedId[0] != null) updateStatus.put(CommandStatus.UPSERTED_ID, upsertedId[0]);
    updateStatus.put(CommandStatus.MATCHED_COUNT, matchedCount());
    updateStatus.put(CommandStatus.MODIFIED_COUNT, modifiedCount());

    if (moreDataFlag) updateStatus.put(CommandStatus.MORE_DATA, moreDataFlag);
    if (returnDocs) {
      return new CommandResult(
          new CommandResult.ResponseData(updatedDocs),
          updateStatus,
          errors.isEmpty() ? null : errors);
    } else {
      return new CommandResult(null, updateStatus, errors.isEmpty() ? null : errors);
    }
  }

  private CommandResult.Error getError(List<DocumentId> documentIds, Throwable throwable) {
    String message =
        "Failed to update documents with _id %s: %s".formatted(documentIds, throwable.getMessage());

    Map<String, Object> fields = new HashMap<>();
    fields.put("exceptionClass", throwable.getClass().getSimpleName());
    if (throwable instanceof JsonApiException jae) {
      fields.put("errorCode", jae.getErrorCode().name());
    }
    return new CommandResult.Error(message, fields);
  }
}
