package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
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
    int insertCount,
    boolean returnDocumentResponses,
    List<InsertOperation.WritableDocAndPosition> successfulInsertions,
    List<Tuple2<InsertOperation.WritableDocAndPosition, Throwable>> failedInsertions)
    implements Supplier<CommandResult> {
  enum InsertionStatus {
    OK,
    ERROR,
    SKIPPED
  }

  @JsonPropertyOrder({"_id", "status", "errorsIdx"})
  record InsertionResult(DocumentId _id, InsertionStatus status, Integer errorsIdx) {}

  /** No-arg constructor, usually used for aggregation. */
  public InsertOperationPage(int insertCount, boolean returnDocumentResponses) {
    this(insertCount, returnDocumentResponses, new ArrayList<>(), new ArrayList<>());
  }

  /** {@inheritDoc} */
  @Override
  public CommandResult get() {
    // Ensure insertions and errors are in the input order (wrt unordered insertions),
    // regardless of output format
    Collections.sort(successfulInsertions);
    if (!failedInsertions().isEmpty()) {
      Collections.sort(
          failedInsertions, Comparator.comparing(tuple -> tuple.getItem1().position()));
    }

    if (!returnDocumentResponses()) { // legacy output, limited to ids, error messages
      List<CommandResult.Error> errors;
      if (failedInsertions().isEmpty()) {
        errors = null;
      } else {
        errors =
            failedInsertions.stream()
                .map(tuple -> getError(tuple.getItem1().document().id(), tuple.getItem2()))
                .toList();
      }
      // Old style, simple ids:
      List<DocumentId> insertedIds =
          successfulInsertions.stream().map(docAndPos -> docAndPos.document().id()).toList();
      return new CommandResult(null, Map.of(CommandStatus.INSERTED_IDS, insertedIds), errors);
    }

    // New style output

    List<CommandResult.Error> errors;
    if (failedInsertions().isEmpty()) {
      errors = null;
    } else {
      Collections.sort(
          failedInsertions, Comparator.comparing(tuple -> tuple.getItem1().position()));
      errors =
          failedInsertions.stream()
              .map(tuple -> getError(tuple.getItem1().document().id(), tuple.getItem2()))
              .toList();
    }
    // But with positions added
    List<Object[]> insertedDocs =
        successfulInsertions.stream().map(InsertOperationPage::positionAndId).toList();
    if (errors != null) {
      List<Object[]> failedDocs =
          failedInsertions.stream().map(tuple -> positionAndId(tuple.getItem1())).toList();
      return new CommandResult(
          null,
          Map.of(
              CommandStatus.INSERTED_DOCUMENTS,
              insertedDocs,
              CommandStatus.FAILED_DOCUMENTS,
              failedDocs),
          errors);
    }
    return new CommandResult(null, Map.of(CommandStatus.INSERTED_DOCUMENTS, insertedDocs), null);
  }

  private static Object[] positionAndId(InsertOperation.WritableDocAndPosition docAndPos) {
    return new Object[] {docAndPos.position(), docAndPos.document().id()};
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
