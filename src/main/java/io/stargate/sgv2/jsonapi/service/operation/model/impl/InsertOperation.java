package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
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
import java.util.Arrays;
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
    List<InsertAttempt> insertions,
    boolean ordered,
    boolean offlineMode,
    boolean returnDocumentResponses)
    implements ModifyOperation {

  /**
   * Container for an individual Document insertion attempt: used to keep track of the original
   * input position; document (if available), its id (if available) and possible processing error.
   * Information will be needed to build optional detail response (returnDocumentResponses).
   */
  public static class InsertAttempt implements Comparable<InsertAttempt> {
    public final int position;

    public WritableShreddedDocument document;
    public DocumentId documentId;

    public Throwable failure;

    public InsertAttempt(int position, DocumentId documentId, Throwable failure) {
      this.position = position;
      this.document = null;
      this.documentId = documentId;
      this.failure = failure;
    }

    public InsertAttempt(int position, WritableShreddedDocument document) {
      this.position = position;
      this.document = document;
      this.documentId = document.id();
    }

    public static InsertAttempt from(int position, WritableShreddedDocument document) {
      return new InsertAttempt(position, document);
    }

    public static List<InsertAttempt> from(List<WritableShreddedDocument> documents) {
      final int count = documents.size();
      List<InsertAttempt> result = new ArrayList<>(count);
      for (int i = 0; i < count; ++i) {
        result.add(from(i, documents.get(i)));
      }
      return result;
    }

    public InsertAttempt addFailure(Throwable failure) {
      if (failure != null) {
        this.failure = failure;
      }
      return this;
    }

    public boolean hasVectorValues() {
      return (document != null) && (document.queryVectorValues() != null);
    }

    @Override
    public int compareTo(InsertOperation.InsertAttempt o) {
      return Integer.compare(position, o.position);
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
        InsertAttempt.from(documents),
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
        commandContext, InsertAttempt.from(documents), ordered, false, returnDocumentResponses);
  }

  public static InsertOperation create(
      CommandContext commandContext, WritableShreddedDocument document) {
    return new InsertOperation(
        commandContext, Arrays.asList(InsertAttempt.from(0, document)), false, false, false);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    final boolean vectorEnabled = commandContext().isVectorEnabled();
    if (!vectorEnabled && insertions.stream().anyMatch(insertion -> insertion.hasVectorValues())) {
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
      List<InsertAttempt> insertions) {

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
                    .transform((id, t) -> insertion.addFailure(t)))
        .concatenate(false)

        // if no failures reduce to the op page
        .collect()
        .in(
            () -> new InsertOperationPage(insertions, returnDocumentResponses()),
            (agg, in) -> {
              Throwable failure = in.failure;
              agg.aggregate(in);

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
      List<InsertAttempt> insertions) {
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
                    .transform((id, t) -> insertion.addFailure(t)))
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
      InsertAttempt insertion,
      boolean vectorEnabled,
      boolean offlineMode) {
    // First things first: did we already fail? If so, propagate
    if (insertion.failure != null) {
      return Uni.createFrom().failure(insertion.failure);
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
