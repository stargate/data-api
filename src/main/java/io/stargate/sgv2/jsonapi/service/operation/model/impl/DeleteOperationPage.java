package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.smallrye.mutiny.tuples.Tuple3;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.util.ExceptionUtil;
import java.util.ArrayList;
import java.util.Collection;
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
    List<Tuple3<Boolean, Throwable, ReadDocument>> deletedInformation,
    boolean moreData,
    boolean returnDocument)
    implements Supplier<CommandResult> {
  private static final String ERROR = "Failed to delete documents with _id %s: %s";

  @Override
  public CommandResult get() {
    int deletedCount =
        (int)
            deletedInformation.stream()
                .filter(deletedDocument -> Boolean.TRUE.equals(deletedDocument.getItem1()))
                .count();
    List<JsonNode> deletedDoc = new ArrayList<>();

    // aggregate the errors by error code or error class
    Multimap<String, Tuple3<Boolean, Throwable, ReadDocument>> groupedErrorDeletes =
        ArrayListMultimap.create();
    deletedInformation.forEach(
        deletedData -> {
          if (deletedData.getItem1() && returnDocument()) {
            deletedDoc.add(deletedData.getItem3().document());
          }
          if (deletedData.getItem2() != null) {
            String key = ExceptionUtil.getThrowableGroupingKey(deletedData.getItem2());
            groupedErrorDeletes.put(key, deletedData);
          }
        });

    // Create error by error code or error class
    List<CommandResult.Error> errors = new ArrayList<>(groupedErrorDeletes.size());
    groupedErrorDeletes
        .keySet()
        .forEach(
            key -> {
              final Collection<Tuple3<Boolean, Throwable, ReadDocument>> deletedDocuments =
                  groupedErrorDeletes.get(key);
              final List<DocumentId> documentIds =
                  deletedDocuments.stream()
                      .map(deletes -> deletes.getItem3().id())
                      .collect(Collectors.toList());
              errors.add(
                  ExceptionUtil.getError(
                      ERROR, documentIds, deletedDocuments.stream().findFirst().get().getItem2()));
            });
    // Return the result

    if (moreData)
      return new CommandResult(
          deletedDoc.isEmpty() ? null : new CommandResult.MultiResponseData(deletedDoc),
          Map.of(CommandStatus.DELETED_COUNT, deletedCount, CommandStatus.MORE_DATA, true),
          errors.isEmpty() ? null : errors);
    else
      return new CommandResult(
          deletedDoc.isEmpty() ? null : new CommandResult.MultiResponseData(deletedDoc),
          Map.of(CommandStatus.DELETED_COUNT, deletedCount),
          errors.isEmpty() ? null : errors);
  }
}
