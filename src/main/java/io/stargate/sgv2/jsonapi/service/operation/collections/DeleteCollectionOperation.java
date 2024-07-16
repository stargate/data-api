package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Executes readOperation to get the documents ids based on filter condition. All the ids are
 * deleted as LWT based on the id and tx_id.
 */
public record DeleteCollectionOperation(
    CommandContext<CollectionSchemaObject> commandContext,
    FindCollectionOperation findCollectionOperation,
    /**
     * Added parameter to pass number of document to be deleted, this is needed because read
     * documents limit changed to deleteLimit + 1
     */
    int deleteLimit,
    int retryLimit,
    /** return deleted document in response if `true`. */
    boolean returnDocumentInResponse,
    DocumentProjector resultProjection)
    implements CollectionModifyOperation {

  public static DeleteCollectionOperation deleteOneAndReturn(
      CommandContext<CollectionSchemaObject> commandContext,
      FindCollectionOperation findCollectionOperation,
      int retryLimit,
      DocumentProjector resultProjection) {
    return new DeleteCollectionOperation(
        commandContext, findCollectionOperation, 1, retryLimit, true, resultProjection);
  }

  public static DeleteCollectionOperation delete(
      CommandContext<CollectionSchemaObject> commandContext,
      FindCollectionOperation findCollectionOperation,
      int deleteLimit,
      int retryLimit) {
    return new DeleteCollectionOperation(
        commandContext, findCollectionOperation, deleteLimit, retryLimit, false, null);
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    final AtomicBoolean moreData = new AtomicBoolean(false);
    final String delete = buildDeleteQuery();
    AtomicInteger totalCount = new AtomicInteger(0);
    final int retryAttempt = retryLimit - 2;
    // Read the required records to be deleted
    return Multi.createBy()
        .repeating()
        .uni(
            () -> new AtomicReference<String>(null),
            stateRef -> {
              Uni<CollectionReadOperation.FindResponse> docsToDelete =
                  findCollectionOperation()
                      .getDocuments(dataApiRequestInfo, queryExecutor, stateRef.get(), null);
              return docsToDelete
                  .onItem()
                  .invoke(findResponse -> stateRef.set(findResponse.pageState()));
            })

        // Documents read until pageState available, max records read is deleteLimit + 1
        .whilst(findResponse -> findResponse.pageState() != null)

        // Get the deleteLimit # of documents to be delete and set moreData flag true if extra
        // document is read.
        .onItem()
        .transformToMulti(
            findResponse -> {
              final List<ReadDocument> docs = findResponse.docs();
              // Below conditionality is because we read up to deleteLimit +1 record.
              if (totalCount.get() + docs.size() <= deleteLimit) {
                totalCount.addAndGet(docs.size());
                return Multi.createFrom().items(docs.stream());
              } else {
                int needed = deleteLimit - totalCount.get();
                totalCount.addAndGet(needed);
                moreData.set(true);
                return Multi.createFrom().items(findResponse.docs().subList(0, needed).stream());
              }
            })
        .concatenate()

        // Run delete for selected documents and retry in case of
        .onItem()
        .transformToUniAndMerge(
            document -> {
              return deleteDocument(dataApiRequestInfo, queryExecutor, delete, document)
                  // Retry `retryLimit` times in case of LWT failure
                  .onFailure(LWTException.class)
                  .recoverWithUni(
                      () -> {
                        return Uni.createFrom()
                            .item(document)
                            .flatMap(
                                prevDoc -> {
                                  return readDocumentAgain(
                                          dataApiRequestInfo, queryExecutor, prevDoc)
                                      .onItem()
                                      // Try deleting the document
                                      .transformToUni(
                                          reReadDocument ->
                                              deleteDocument(
                                                  dataApiRequestInfo,
                                                  queryExecutor,
                                                  delete,
                                                  reReadDocument));
                                })
                            .onFailure(LWTException.class)
                            .retry()
                            // because it's already run twice before this
                            // check.
                            .atMost(retryLimit - 1);
                      })
                  .onItemOrFailure()
                  .transform(
                      (deleted, error) ->
                          Tuple3.of(
                              deleted != null ? deleted.getItem1() : false,
                              error,
                              error == null
                                      && deleted != null
                                      && deleted.getItem2() != null
                                      && returnDocumentInResponse
                                  ? applyProjection(deleted.getItem2())
                                  : document));
            })
        .collect()
        .asList()
        .onItem()
        .transform(
            deletedInformation -> {
              commandContext
                  .jsonProcessingMetricsReporter()
                  .reportJsonReadDocsMetrics(
                      commandContext().commandName(), deletedInformation.size());
              return new DeleteOperationPage(
                  deletedInformation, moreData.get(), returnDocumentInResponse);
            });
  }

  private ReadDocument applyProjection(ReadDocument document) {
    resultProjection().applyProjection(document.get());
    return document;
  }

  private String buildDeleteQuery() {
    String delete = "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?";
    return String.format(
        delete,
        commandContext.schemaObject().name.keyspace(),
        commandContext.schemaObject().name.table());
  }

  /**
   * When delete is run with LWT, applied field is always the first field and in case the
   * transaction id mismatch the latest transaction id is returned as second field Eg:
   * cassandra@cqlsh:jsonapi> delete from jsonapi.test1 where key = 'doc2' IF tx_id =
   * 13659a90-9361-11ed-92df-515ba7f99655 ;
   *
   * <p>[applied] | tx_id -----------+-------------------------------------- False |
   * 13659a90-9361-11ed-92df-515ba7f99654
   *
   * <p>cassandra@cqlsh:jsonapi> delete from jsonapi.test1 where key = 'doc2' IF tx_id =
   * 13659a90-9361-11ed-92df-515ba7f99654 ;
   *
   * <p>[applied] ----------- True
   *
   * @param queryExecutor
   * @param query
   * @param doc
   * @return Uni<Tuple2<Boolean, ReadDocument>> where boolean `true` if deleted successfully, else
   *     `false` if data changed and no longer match the conditions and throws JsonApiException if
   *     LWT failure. ReadDocument is the document that was deleted.
   */
  private Uni<Tuple2<Boolean, ReadDocument>> deleteDocument(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      String query,
      ReadDocument doc)
      throws JsonApiException {
    return Uni.createFrom()
        .item(doc)
        // Read again if retryAttempt >`0`
        .onItem()
        .transformToUni(
            document -> {
              if (document == null) {
                return Uni.createFrom().item(Tuple2.of(false, document));
              } else {
                SimpleStatement deleteStatement = bindDeleteQuery(query, document);
                return queryExecutor
                    .executeWrite(dataApiRequestInfo, deleteStatement)
                    .onItem()
                    .transform(
                        result -> {
                          // LWT returns `true` for successful transaction, false on failure.
                          if (result.wasApplied()) {
                            // In case of successful document delete
                            return Tuple2.of(true, document);
                          } else {
                            // In case of successful document delete

                            throw new LWTException(ErrorCode.CONCURRENCY_FAILURE);
                          }
                        });
              }
            });
  }

  private Uni<ReadDocument> readDocumentAgain(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      ReadDocument prevReadDoc) {
    // Read again if retry flag is `true`
    return findCollectionOperation()
        .getDocuments(
            dataApiRequestInfo,
            queryExecutor,
            null,
            new IDCollectionFilter(IDCollectionFilter.Operator.EQ, prevReadDoc.id().orElseThrow()))
        .onItem()
        .transform(
            response -> {
              if (!response.docs().isEmpty()) {
                return response.docs().get(0);
              } else {
                // If data changed and doesn't satisfy filter conditions
                return null;
              }
            });
  }

  private static SimpleStatement bindDeleteQuery(String query, ReadDocument doc) {
    SimpleStatement deleteStatement =
        SimpleStatement.newInstance(
            query,
            CQLBindValues.getDocumentIdValue(doc.id().orElseThrow()),
            doc.txnId().orElse(null));
    return deleteStatement;
  }
}
