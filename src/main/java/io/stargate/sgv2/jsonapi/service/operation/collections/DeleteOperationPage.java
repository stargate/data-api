package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.smallrye.mutiny.tuples.Tuple3;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.util.ExceptionUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
    // aaron - 9 octo 2024 - this class had multiple return statements, which is ok but when I
    // changed to
    // use the CommandResultBuilder, I left the structure as it was to reduce the amount of changes.
    // when we move to use OperationAttempt for the collection commands we can refactor
    if (deletedInformation == null) {
      return CommandResult.statusOnlyBuilder(false, false)
          .addStatus(CommandStatus.DELETED_COUNT, -1)
          .build();
    }

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
            deletedDoc.add(deletedData.getItem3().get());
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
                      .map(deletes -> deletes.getItem3().id().orElseThrow())
                      .collect(Collectors.toList());
              errors.add(
                  ExceptionUtil.getError(
                      ERROR, documentIds, deletedDocuments.stream().findFirst().get().getItem2()));
            });

    // return the result
    // note that we always target a single document to be returned
    // thus fixed to the SingleResponseData

    // aaron 9-oct-2024 the original code had this to create the "ResponseData"for the command
    // result
    // which looks like it would be statusOnly if there were no docs, otherwise singleDoc
    // deletedDoc.isEmpty() ? null : new ResponseData.SingleResponseData(deletedDoc.get(0)),
    var builder =
        deletedDoc.isEmpty()
            ? CommandResult.statusOnlyBuilder(false, false)
            : CommandResult.singleDocumentBuilder(false, false).addDocument(deletedDoc.getFirst());

    builder.addStatus(CommandStatus.DELETED_COUNT, deletedCount).addCommandResultError(errors);
    if (moreData) {
      builder.addStatus(CommandStatus.MORE_DATA, true);
    }
    return builder.build();
  }
}
