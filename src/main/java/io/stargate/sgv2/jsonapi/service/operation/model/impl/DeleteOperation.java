package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ModifyOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Executes readOperation to get the documents ids based on filter condition. All the ids are
 * deleted as LWT based on the id and tx_id.
 */
public record DeleteOperation(
    CommandContext commandContext,
    ReadOperation readOperation,
    /**
     * Added parameter to pass number of document to be deleted, this is needed because read
     * documents limit changed to deleteLimit + 1
     */
    int deleteLimit,
    int retryLimit)
    implements ModifyOperation {
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    final AtomicBoolean moreData = new AtomicBoolean(false);
    final QueryOuterClass.Query delete = buildDeleteQuery();
    AtomicInteger totalCount = new AtomicInteger(0);
    // Read the required records to be deleted
    return Multi.createBy()
        .repeating()
        .uni(
            () -> new AtomicReference<String>(null),
            stateRef -> {
              Uni<ReadOperation.FindResponse> docsToDelete =
                  readOperation().getDocuments(queryExecutor, stateRef.get(), null);
              return docsToDelete
                  .onItem()
                  .invoke(findResponse -> stateRef.set(findResponse.pagingState()));
            })

        // Documents read until pagingState available, max records read is deleteLimit + 1
        .whilst(findResponse -> findResponse.pagingState() != null)

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
        .transformToUniAndConcatenate(
            document -> {
              AtomicInteger retryAttempt = new AtomicInteger(0);
              return deleteDocument(queryExecutor, delete, document, retryAttempt)

                  // Retry `retryLimit` times in case of LWT failure
                  .onFailure()
                  .retry()
                  .until(
                      error ->
                          error instanceof JsonApiException && retryAttempt.get() < retryLimit);
            })
        .collect()

        // Count the successful deletes
        .in(
            AtomicInteger::new,
            (atomicCounter, flag) -> {
              if (flag) {
                atomicCounter.incrementAndGet();
              }
            })
        .onItem()
        .transform(deletedCounter -> new DeleteOperationPage(deletedCounter.get(), moreData.get()));
  }

  private QueryOuterClass.Query buildDeleteQuery() {
    String delete = "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?";
    return QueryOuterClass.Query.newBuilder()
        .setCql(String.format(delete, commandContext.namespace(), commandContext.collection()))
        .build();
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
   * @param retryAttempt
   * @return Uni<Boolean> `true` if deleted successfully, else `false` if data changed and no longer
   *     match the conditions and throws JsonApiException if LWT failure.
   */
  private Uni<Boolean> deleteDocument(
      QueryExecutor queryExecutor,
      QueryOuterClass.Query query,
      ReadDocument doc,
      AtomicInteger retryAttempt)
      throws JsonApiException {

    return Uni.createFrom()
        .item(doc)
        // Read again if retryAttempt >`0`
        .onItem()
        .transformToUni(
            document -> {
              if (retryAttempt.get() > 0) {
                retryAttempt.incrementAndGet();
                return readDocumentAgain(queryExecutor, document);
              } else {
                retryAttempt.incrementAndGet();
                return Uni.createFrom().item(document);
              }
            })
        .onItem()
        .transformToUni(
            docToDelete -> {
              // In case document resolved after the retry read
              if (docToDelete == null) {
                return Uni.createFrom().item(false);
              } else {
                QueryOuterClass.Query boundQuery = bindDeleteQuery(query, docToDelete);
                return queryExecutor
                    .executeWrite(boundQuery)
                    .onItem()
                    .transform(
                        result -> {
                          // LWT returns `true` for successful transaction, false on failure.
                          if (result.getRows(0).getValues(0).getBoolean()) {
                            // In case of successful document delete
                            return true;
                          } else {
                            // In case of successful document delete

                            throw new JsonApiException(
                                ErrorCode.CONCURRENCY_FAILURE,
                                "Delete failed for document with id %s because of concurrent transaction"
                                    .formatted(docToDelete.id().value()));
                          }
                        });
              }
            });
  }

  private Uni<? extends ReadDocument> readDocumentAgain(
      QueryExecutor queryExecutor, ReadDocument prevReadDoc) {
    // Read again if retry flag is `true`
    return readOperation()
        .getDocuments(
            queryExecutor,
            null,
            new DBFilterBase.IDFilter(DBFilterBase.IDFilter.Operator.EQ, prevReadDoc.id()))
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

  private static QueryOuterClass.Query bindDeleteQuery(
      QueryOuterClass.Query builtQuery, ReadDocument doc) {
    QueryOuterClass.Values.Builder values =
        QueryOuterClass.Values.newBuilder()
            .addValues(Values.of(CustomValueSerializers.getDocumentIdValue(doc.id())))
            .addValues(Values.of(doc.txnId()));
    return QueryOuterClass.Query.newBuilder(builtQuery).setValues(values).build();
  }
}
