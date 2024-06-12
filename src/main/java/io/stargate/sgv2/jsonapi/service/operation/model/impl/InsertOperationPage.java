package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.tuples.Tuple2;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The internal to insert operation results, keeping ids of successfully and not-successfully
 * inserted documents.
 *
 * <p>Can serve as an aggregator, using the {@link #aggregate} function.
 *
 * @param successfulInsertions Documents that we successfully inserted.
 * @param failedInsertions Documents that failed to be inserted, along with failure reason.
 */
public record InsertOperationPage(
    boolean returnDocumentPositions,
    List<InsertOperation.WritableDocAndPosition> successfulInsertions,
    List<Tuple2<InsertOperation.WritableDocAndPosition, Throwable>> failedInsertions)
    implements Supplier<CommandResult> {

  /** No-arg constructor, usually used for aggregation. */
  public InsertOperationPage(boolean returnDocumentPositions) {
    this(returnDocumentPositions, new ArrayList<>(), new ArrayList<>());
  }

  /** {@inheritDoc} */
  @Override
  public CommandResult get() {
    // Ensure insertions are in order (wrt unordered insertions)
    Collections.sort(successfulInsertions);
    List<DocumentId> insertedIds =
        successfulInsertions.stream().map(docAndPos -> docAndPos.document().id()).toList();
    // if we have errors, transform
    if (!failedInsertions().isEmpty()) {
      // Also ensure failures are in the input position order
      Collections.sort(
          failedInsertions, Comparator.comparing(tuple -> tuple.getItem1().position()));
      List<CommandResult.Error> errors =
          failedInsertions.stream()
              .map(tuple -> getError(tuple.getItem1().document().id(), tuple.getItem2()))
              .toList();

      return new CommandResult(null, Map.of(CommandStatus.INSERTED_IDS, insertedIds), errors);
    }

    // id no errors, just inserted ids
    return new CommandResult(Map.of(CommandStatus.INSERTED_IDS, insertedIds));
  }

  private static CommandResult.Error getError(DocumentId documentId, Throwable throwable) {
    String message =
        "Failed to insert document with _id %s: %s".formatted(documentId, throwable.getMessage());

    Map<String, Object> fields = new HashMap<>();
    fields.put("exceptionClass", throwable.getClass().getSimpleName());
    if (throwable instanceof JsonApiException jae) {
      fields.put("errorCode", jae.getErrorCode().name());
    }
    return ThrowableToErrorMapper.getMapperWithMessageFunction().apply(throwable, message);
  }

  /**
   * Aggregates the result of the insert operation into this object.
   *
   * @param docWithPosition Document that was inserted (or failed to be inserted)
   * @param failure If not null, means an error occurred during attempted insertion
   */
  public void aggregate(InsertOperation.WritableDocAndPosition docWithPosition, Throwable failure) {
    if (null == failure) {
      successfulInsertions.add(docWithPosition);
    } else {
      failedInsertions.add(Tuple2.of(docWithPosition, failure));
    }
  }
}
