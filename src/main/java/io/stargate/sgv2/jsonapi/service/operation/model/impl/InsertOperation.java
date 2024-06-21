package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.operation.model.ModifyOperation;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Operation that inserts one or more documents.
 *
 * @param commandContext Context that defines namespace and database.
 * @param insertions Document insertion attempts to try.
 * @param ordered If insertions should be attempted sequentially, in order.
 */
public record InsertOperation(
    CommandContext commandContext,
    List<InsertOperationAttempt> insertions,
    boolean ordered,
    boolean offlineMode,
    boolean returnDocumentResponses)
    implements ModifyOperation {
  public record WritableDocAndPosition(int position, WritableShreddedDocument document)
      implements Comparable<WritableDocAndPosition> {
    @Override
    public int compareTo(InsertOperation.WritableDocAndPosition o) {
      // Order by position (only), ascending
      return Integer.compare(position, o.position);
    }
  }

  /**
   * Container for an insert operation attempt: used to keep track of the original document (if
   * available), its id (if available), possible processing error and the position of the operation
   * in input List
   */
  static class InsertOperationAttempt {
    public final int position;

    public WritableShreddedDocument document;
    public DocumentId documentId;

    public Throwable failure;

    public InsertOperationAttempt(int position, DocumentId documentId, Throwable failure) {
      this.position = position;
      this.document = null;
      this.documentId = documentId;
      this.failure = failure;
    }

    public InsertOperationAttempt(int position, WritableShreddedDocument document) {
      this.position = position;
      this.document = document;
      this.documentId = document.id();
    }

    public static InsertOperationAttempt from(int position, WritableShreddedDocument document) {
      return new InsertOperationAttempt(position, document);
    }

    public static List<InsertOperationAttempt> from(List<WritableShreddedDocument> documents) {
      final int count = documents.size();
      List<InsertOperationAttempt> result = new ArrayList<>(count);
      for (int i = 0; i < count; ++i) {
        result.add(from(i, documents.get(i)));
      }
      return result;
    }

    public boolean hasVectorValues() {
      return (document != null) && (document.queryVectorValues() != null);
    }
  }

  public static InsertOperation create(
      CommandContext commandContext,
      List<WritableShreddedDocument> documents,
      boolean ordered,
      boolean offlineMode,
      boolean returnDocumentResponses) {
    return new InsertOperation(
        commandContext,
        InsertOperationAttempt.from(documents),
        ordered,
        offlineMode,
        returnDocumentResponses);
  }

  public static InsertOperation create(
      CommandContext commandContext,
      List<WritableShreddedDocument> documents,
      boolean ordered,
      boolean returnDocumentResponses) {
    return new InsertOperation(
        commandContext,
        InsertOperationAttempt.from(documents),
        ordered,
        false,
        returnDocumentResponses);
  }

  public static InsertOperation create(
      CommandContext commandContext, WritableShreddedDocument document) {
    return new InsertOperation(
        commandContext, List.of(InsertOperationAttempt.from(0, document)), false, false, false);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    final boolean vectorEnabled = commandContext().isVectorEnabled();
    if (!vectorEnabled && insertions.stream().anyMatch(doc -> doc.hasVectorValues())) {
      throw new JsonApiException(
          ErrorCode.VECTOR_SEARCH_NOT_SUPPORTED,
          ErrorCode.VECTOR_SEARCH_NOT_SUPPORTED.getMessage() + commandContext().collection());
    }
    // create json doc write metrics
    if (commandContext.jsonProcessingMetricsReporter() != null) {
      commandContext
          .jsonProcessingMetricsReporter()
          .reportJsonWrittenDocsMetrics(commandContext().commandName(), insertions.size());
    }
    final List<WritableDocAndPosition> docsWithPositions = new ArrayList<>(insertions.size());
    int pos = 0;
    for (InsertOperationAttempt insert : insertions) {
      docsWithPositions.add(new WritableDocAndPosition(pos++, insert.document));
    }
    if (ordered) {
      return insertOrdered(dataApiRequestInfo, queryExecutor, vectorEnabled, docsWithPositions);
    } else {
      return insertUnordered(dataApiRequestInfo, queryExecutor, vectorEnabled, docsWithPositions);
    }
  }

  // implementation for the ordered insert
  private Uni<Supplier<CommandResult>> insertOrdered(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      boolean vectorEnabled,
      List<WritableDocAndPosition> docsWithPositions) {

    // build query once
    final String query = buildInsertQuery(vectorEnabled);

    return Multi.createFrom()
        .iterable(docsWithPositions)

        // concatenate to respect ordered
        .onItem()
        .transformToUni(
            doc ->
                insertDocument(
                        dataApiRequestInfo, queryExecutor, query, doc, vectorEnabled, offlineMode)
                    // wrap item and failure
                    // the collection can decide how to react on failure
                    .onItemOrFailure()
                    .transform((id, t) -> Tuple2.of(doc, t)))
        .concatenate(false)

        // if no failures reduce to the op page
        .collect()
        .in(
            () -> new InsertOperationPage(docsWithPositions, returnDocumentResponses()),
            (agg, in) -> {
              Throwable failure = in.getItem2();
              agg.aggregate(in.getItem1(), failure);

              if (failure != null) {
                throw new FailFastInsertException(agg, failure);
              }
            })

        // in case upstream propagated FailFastInsertException
        // return collected result
        .onFailure(FailFastInsertException.class)
        .recoverWithItem(
            e -> {
              // safe to cast, asserted class in onFailure
              FailFastInsertException failFastInsertException = (FailFastInsertException) e;
              return failFastInsertException.result;
            })

        // use object identity to resolve to Supplier<CommandResult>
        .map(i -> i);
  }

  // implementation for the unordered insert
  private Uni<Supplier<CommandResult>> insertUnordered(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      boolean vectorEnabled,
      List<WritableDocAndPosition> docsWithPositions) {
    // build query once
    String query = buildInsertQuery(vectorEnabled);
    return Multi.createFrom()
        .iterable(docsWithPositions)

        // merge to make it parallel
        .onItem()
        .transformToUniAndMerge(
            doc ->
                insertDocument(
                        dataApiRequestInfo, queryExecutor, query, doc, vectorEnabled, offlineMode)
                    // handle errors fail silent mode
                    .onItemOrFailure()
                    .transform((id, t) -> Tuple2.of(doc, t)))

        // then reduce here
        .collect()
        .in(
            () -> new InsertOperationPage(docsWithPositions, returnDocumentResponses()),
            (agg, in) -> agg.aggregate(in.getItem1(), in.getItem2()))

        // use object identity to resolve to Supplier<CommandResult>
        .map(i -> i);
  }

  // inserts a single document
  private static Uni<DocumentId> insertDocument(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      String query,
      WritableDocAndPosition docWithPosition,
      boolean vectorEnabled,
      boolean offlineMode) {
    // bind and execute
    final WritableShreddedDocument doc = docWithPosition.document();
    SimpleStatement boundStatement = bindInsertValues(query, doc, vectorEnabled, offlineMode);
    return queryExecutor
        .executeWrite(dataApiRequestInfo, boundStatement)

        // ensure document was written, if no applied continue with error
        .onItem()
        .transformToUni(
            result -> {
              if (result.wasApplied()) {
                return Uni.createFrom().item(doc.id());
              } else {
                final UUID txId = result.one().getUuid("tx_id");
                if (doc.nextTxID().equals(txId)) {
                  return Uni.createFrom().item(doc.id());
                }
                Exception failure = new JsonApiException(ErrorCode.DOCUMENT_ALREADY_EXISTS);
                return Uni.createFrom().failure(failure);
              }
            });
  }

  // utility for building the insert query
  public String buildInsertQuery(boolean vectorEnabled) {
    if (vectorEnabled) {
      String insertWithVector =
          "INSERT INTO \"%s\".\"%s\""
              + " (key, tx_id, doc_json, exist_keys, array_size, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values, query_timestamp_values, query_vector_value)"
              + " VALUES"
              + " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
              + (offlineMode ? "" : " IF NOT EXISTS");
      // The offline mode SSTableWriter does not support conditional inserts, so it can not have the
      // IF NOT EXISTS clause
      return String.format(
          insertWithVector, commandContext.namespace(), commandContext.collection());
    } else {
      String insert =
          "INSERT INTO \"%s\".\"%s\""
              + " (key, tx_id, doc_json, exist_keys, array_size, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values, query_timestamp_values)"
              + " VALUES"
              + " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
              + (offlineMode ? "" : " IF NOT EXISTS");
      // The offline mode SSTableWriter does not support conditional inserts, so it can not have the
      // IF NOT EXISTS clause
      return String.format(insert, commandContext.namespace(), commandContext.collection());
    }
  }

  // utility for query binding
  private static SimpleStatement bindInsertValues(
      String query, WritableShreddedDocument doc, boolean vectorEnabled, boolean offlineMode) {
    // respect the order in the DocsApiConstants.ALL_COLUMNS_NAMES
    if (vectorEnabled) {
      return SimpleStatement.newInstance(
          query,
          CQLBindValues.getDocumentIdValue(doc.id()),
          doc.nextTxID(),
          doc.docJson(),
          CQLBindValues.getSetValue(doc.existKeys()),
          CQLBindValues.getIntegerMapValues(doc.arraySize()),
          CQLBindValues.getStringSetValue(doc.arrayContains()),
          CQLBindValues.getBooleanMapValues(doc.queryBoolValues()),
          CQLBindValues.getDoubleMapValues(doc.queryNumberValues()),
          CQLBindValues.getStringMapValues(doc.queryTextValues()),
          CQLBindValues.getSetValue(doc.queryNullValues()),
          // The offline SSTableWriter component expects the timestamp as a Date object instead of
          // Instant for Date data type
          offlineMode
              ? CQLBindValues.getTimestampAsDateMapValues(doc.queryTimestampValues())
              : CQLBindValues.getTimestampMapValues(doc.queryTimestampValues()),
          CQLBindValues.getVectorValue(doc.queryVectorValues()));
    } else {
      return SimpleStatement.newInstance(
          query,
          CQLBindValues.getDocumentIdValue(doc.id()),
          doc.nextTxID(),
          doc.docJson(),
          CQLBindValues.getSetValue(doc.existKeys()),
          CQLBindValues.getIntegerMapValues(doc.arraySize()),
          CQLBindValues.getStringSetValue(doc.arrayContains()),
          CQLBindValues.getBooleanMapValues(doc.queryBoolValues()),
          CQLBindValues.getDoubleMapValues(doc.queryNumberValues()),
          CQLBindValues.getStringMapValues(doc.queryTextValues()),
          CQLBindValues.getSetValue(doc.queryNullValues()),
          // The offline SSTableWriter component expects the timestamp as a Date object instead of
          // Instant for Date data type
          offlineMode
              ? CQLBindValues.getTimestampAsDateMapValues(doc.queryTimestampValues())
              : CQLBindValues.getTimestampMapValues(doc.queryTimestampValues()));
    }
  }

  // simple exception to propagate fail fast
  private static class FailFastInsertException extends RuntimeException {

    private final InsertOperationPage result;

    public FailFastInsertException(InsertOperationPage result, Throwable cause) {
      super(cause);
      this.result = result;
    }
  }
}
