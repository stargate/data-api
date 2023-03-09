package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.smallrye.mutiny.tuples.Tuple3;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This represents the response for a delete operation.
 *
 * @param deletedInformation - List of Tuple3, each tuple 3 corresponds to document tried for
 *     deletion. Item1 boolean states if document is deleted, item2 contains the throwable and item3
 *     has the document id
 * @param moreData - if `true` means more documents available in DB for the provided condition
 */
public record DeleteOperationPage(
    List<Tuple3<Boolean, Throwable, DocumentId>> deletedInformation, boolean moreData)
    implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {
    int deletedCount =
        (int)
            deletedInformation.stream()
                .filter(
                    deletedDocument ->
                        deletedDocument.getItem1() != null && deletedDocument.getItem1())
                .count();
    Multimap<String, Tuple3<Boolean, Throwable, DocumentId>> groupedErrorDeletes =
        ArrayListMultimap.create();

    deletedInformation.forEach(
        deletedData -> {
          if (deletedData.getItem2() != null) {
            String key = deletedData.getItem2().getClass().getSimpleName();
            if (deletedData.getItem2() instanceof JsonApiException jae)
              key = jae.getErrorCode().name();
            groupedErrorDeletes.put(key, deletedData);
          }
        });

    List<CommandResult.Error> errors = new ArrayList<>(groupedErrorDeletes.size());
    groupedErrorDeletes
        .keySet()
        .forEach(
            key -> {
              final Collection<Tuple3<Boolean, Throwable, DocumentId>> deletedDocuments =
                  groupedErrorDeletes.get(key);
              final List<DocumentId> documentIds =
                  deletedDocuments.stream()
                      .map(deletes -> deletes.getItem3())
                      .collect(Collectors.toList());
              errors.add(
                  getError(documentIds, deletedDocuments.stream().findFirst().get().getItem2()));
            });

    if (moreData)
      return new CommandResult(
          null,
          Map.of(CommandStatus.DELETED_COUNT, deletedCount, CommandStatus.MORE_DATA, true),
          errors.isEmpty() ? null : errors);
    else
      return new CommandResult(
          null,
          Map.of(CommandStatus.DELETED_COUNT, deletedCount),
          errors.isEmpty() ? null : errors);
  }

  private CommandResult.Error getError(List<DocumentId> documentIds, Throwable throwable) {
    String message =
        "Failed to delete documents with _id %s: %s".formatted(documentIds, throwable.getMessage());

    Map<String, Object> fields = new HashMap<>();
    fields.put("exceptionClass", throwable.getClass().getSimpleName());
    if (throwable instanceof JsonApiException jae) {
      fields.put("errorCode", jae.getErrorCode().name());
    }
    return new CommandResult.Error(message, fields);
  }
}
