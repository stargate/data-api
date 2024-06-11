package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The internal to insert operation results, keeping ids of successfully and not-successfully
 * inserted documents.
 *
 * <p>Can serve as an aggregator, using the {@link #aggregate(DocumentId, Throwable)} function.
 *
 * @param insertedIds Documents IDs that we successfully inserted.
 * @param failedIds Document IDs that failed to be inserted.
 */
public record InsertOperationPage(
    boolean returnDocumentPositions,
    List<DocumentId> insertedIds,
    Map<DocumentId, Throwable> failedIds)
    implements Supplier<CommandResult> {

  /** No-arg constructor, usually used for aggregation. */
  public InsertOperationPage(boolean returnDocumentPositions) {
    this(returnDocumentPositions, new ArrayList<>(), new HashMap<>());
  }

  /** {@inheritDoc} */
  @Override
  public CommandResult get() {
    // if we have errors, transform
    if (null != failedIds && !failedIds().isEmpty()) {

      List<CommandResult.Error> errors = new ArrayList<>(failedIds.size());
      failedIds.forEach((documentId, throwable) -> errors.add(getError(documentId, throwable)));

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
   * @param id ID of the document that was inserted written.
   * @param failure If not null, means an error occurred during the write.
   */
  public void aggregate(DocumentId id, Throwable failure) {
    if (null != failure) {
      failedIds.put(id, failure);
    } else {
      insertedIds.add(id);
    }
  }
}
