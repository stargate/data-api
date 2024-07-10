package io.stargate.sgv2.jsonapi.service.operation.model.collections;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.operation.model.InsertOperationPage;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import java.util.*;
import java.util.function.Supplier;

/**
 * Operation that inserts one or more documents.
 *
 * @param commandContext Context that defines namespace and database.
 * @param insertions Document insertion attempts to try.
 * @param ordered If insertions should be attempted sequentially, in order.
 */
public record InsertOperation(
    CommandContext<CollectionSchemaObject> commandContext,
    List<CollectionInsertAttempt> insertions,
    boolean ordered,
    boolean offlineMode,
    boolean returnDocumentResponses)
    implements CollectionModifyOperation {

  public static InsertOperation create(
      CommandContext commandContext,
      List<WritableShreddedDocument> documents,
      boolean ordered,
      boolean offlineMode,
      boolean returnDocumentResponses) {
    return new InsertOperation(
        commandContext,
        CollectionInsertAttempt.from(documents),
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
        CollectionInsertAttempt.from(documents),
        ordered,
        false,
        returnDocumentResponses);
  }

  public static InsertOperation create(
      CommandContext commandContext, WritableShreddedDocument document) {
    return new InsertOperation(
        commandContext,
        Collections.singletonList(CollectionInsertAttempt.from(0, document)),
        false,
        false,
        false);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    final boolean vectorEnabled = commandContext().schemaObject().isVectorEnabled();
    if (!vectorEnabled && insertions.stream().anyMatch(insertion -> insertion.hasVectorValues())) {
      throw new JsonApiException(
          ErrorCode.VECTOR_SEARCH_NOT_SUPPORTED,
          ErrorCode.VECTOR_SEARCH_NOT_SUPPORTED.getMessage()
              + commandContext().schemaObject().name.table());
    }
    // create json doc write metrics
    if (commandContext.jsonProcessingMetricsReporter() != null) {
      commandContext
          .jsonProcessingMetricsReporter()
          .reportJsonWrittenDocsMetrics(commandContext().commandName(), insertions.size());
    }
    if (ordered) {
      return insertOrdered(dataApiRequestInfo, queryExecutor, vectorEnabled, insertions);
    } else {
      return insertUnordered(dataApiRequestInfo, queryExecutor, vectorEnabled, insertions);
    }
  }

  // implementation for the ordered insert
  private Uni<Supplier<CommandResult>> insertOrdered(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      boolean vectorEnabled,
      List<CollectionInsertAttempt> insertions) {

    // build query once
    final String query = buildInsertQuery(vectorEnabled);

    return Multi.createFrom()
        .iterable(insertions)

        // concatenate to respect ordered
        .onItem()
        .transformToUni(
            insertion ->
                insertDocument(
                        dataApiRequestInfo,
                        queryExecutor,
                        query,
                        insertion,
                        vectorEnabled,
                        offlineMode)
                    // wrap item and failure
                    // the collection can decide how to react on failure
                    .onItemOrFailure()
                    .transform((id, t) -> insertion.maybeAddFailure(t)))
        .concatenate(false)

        // if no failures reduce to the op page
        .collect()
        .in(
            () -> new InsertOperationPage(insertions, returnDocumentResponses()),
            (insertPage, insertAttempt) -> {
              insertPage.aggregate(insertAttempt);
              insertAttempt
                  .failure()
                  .ifPresent(
                      failure -> {
                        throw new FailFastInsertException(insertPage, failure);
                      });
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
      List<CollectionInsertAttempt> insertions) {
    // build query once
    String query = buildInsertQuery(vectorEnabled);
    return Multi.createFrom()
        .iterable(insertions)

        // merge to make it parallel
        .onItem()
        .transformToUniAndMerge(
            insertion ->
                insertDocument(
                        dataApiRequestInfo,
                        queryExecutor,
                        query,
                        insertion,
                        vectorEnabled,
                        offlineMode)
                    // handle errors fail silent mode
                    .onItemOrFailure()
                    .transform((id, t) -> insertion.maybeAddFailure(t)))
        // then reduce here
        .collect()
        .in(
            () -> new InsertOperationPage(insertions, returnDocumentResponses()),
            (agg, in) -> {
              agg.aggregate(in);
            })
        // use object identity to resolve to Supplier<CommandResult>
        .map(i -> i);
  }

  // inserts a single document
  private static Uni<DocumentId> insertDocument(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      String query,
      CollectionInsertAttempt insertion,
      boolean vectorEnabled,
      boolean offlineMode) {
    // First things first: did we already fail? If so, propagate
    if (insertion.failure().isPresent()) {
      return Uni.createFrom().failure(insertion.failure().get());
    }

    // bind and execute
    final WritableShreddedDocument doc = insertion.document;
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
          insertWithVector,
          commandContext.schemaObject().name.keyspace(),
          commandContext.schemaObject().name.table());
    } else {
      String insert =
          "INSERT INTO \"%s\".\"%s\""
              + " (key, tx_id, doc_json, exist_keys, array_size, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values, query_timestamp_values)"
              + " VALUES"
              + " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
              + (offlineMode ? "" : " IF NOT EXISTS");
      // The offline mode SSTableWriter does not support conditional inserts, so it can not have the
      // IF NOT EXISTS clause
      return String.format(
          insert,
          commandContext.schemaObject().name.keyspace(),
          commandContext.schemaObject().name.table());
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
