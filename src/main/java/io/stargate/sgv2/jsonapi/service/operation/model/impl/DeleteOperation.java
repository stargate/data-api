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
    final Multi<ReadOperation.FindResponse> findResponses =
        Multi.createBy()
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
            .whilst(findResponse -> findResponse.pagingState() != null);
    AtomicInteger totalCount = new AtomicInteger(0);
    final Uni<AtomicInteger> counter =
        findResponses
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
                    return Multi.createFrom()
                        .items(findResponse.docs().subList(0, needed).stream());
                  }
                })
            .concatenate()
            .onItem()
            .transformToUniAndConcatenate(
                readDocument -> {
                  AtomicInteger attempt = new AtomicInteger(0);
                  return Multi.createBy()
                      .repeating()
                      .uni(() -> deleteDocument(queryExecutor, delete, readDocument, attempt))
                      .whilst(
                          respVal ->
                              (respVal == DeleteResponse.CONCURRENCY_FAILURE
                                  && attempt.incrementAndGet() < retryLimit))
                      .collect()
                      .last()
                      .onItem()
                      .transform(
                          respVal -> {
                            switch (respVal) {
                              case DELETED:
                                return true;
                              case MODIFIED_BY_CONCURRENT_PROCESS:
                                return false;
                              case CONCURRENCY_FAILURE:
                              default:
                                throw new JsonApiException(
                                    ErrorCode.CONCURRENCY_FAILURE,
                                    "Delete failed for %s because of concurrent transaction"
                                        .formatted(readDocument.id().toString()));
                            }
                          });
                })
            .collect()
            .in(
                AtomicInteger::new,
                (atomicCounter, flag) -> {
                  if (flag) {
                    atomicCounter.incrementAndGet();
                  }
                });

    return counter
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
   * @param attempt
   * @return Uni<DeleteResponse>
   */
  private Uni<DeleteResponse> deleteDocument(
      QueryExecutor queryExecutor,
      QueryOuterClass.Query query,
      ReadDocument doc,
      AtomicInteger attempt) {
    final Uni<ReadDocument> documentToDelete =
        Uni.createFrom()
            .item(attempt.get())
            .onItem()
            .transformToUni(
                attemptValue -> {
                  if (attemptValue > 0) {
                    return readDocumentAgain(queryExecutor, doc);
                  } else {
                    return Uni.createFrom().item(doc);
                  }
                });
    return documentToDelete
        .onItem()
        .transformToUni(
            docToDelete -> {
              if (docToDelete == null) {
                return Uni.createFrom().item(DeleteResponse.MODIFIED_BY_CONCURRENT_PROCESS);
              } else {
                QueryOuterClass.Query boundQuery = bindDeleteQuery(query, docToDelete);
                return queryExecutor
                    .executeWrite(boundQuery)
                    .onItem()
                    .transform(
                        result -> {
                          if (result.getRows(0).getValues(0).getBoolean()) {
                            return DeleteResponse.DELETED;
                          } else {
                            return DeleteResponse.CONCURRENCY_FAILURE;
                          }
                        });
              }
            });
  }

  private Uni<? extends ReadDocument> readDocumentAgain(
      QueryExecutor queryExecutor, ReadDocument prevReadDoc) {
    // Read again if retry flag is `true`
    final Uni<ReadOperation.FindResponse> findResponse =
        readOperation()
            .getDocuments(
                queryExecutor,
                null,
                new DBFilterBase.IDFilter(DBFilterBase.IDFilter.Operator.EQ, prevReadDoc.id()));
    return findResponse
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

  public enum DeleteResponse {
    /** Successfully deleted a document */
    DELETED,
    /**
     * Document modified by concurrent process and doesn't match the condition Could have changed
     * value or deleted
     */
    MODIFIED_BY_CONCURRENT_PROCESS,

    /** Failed because of concurrent process */
    CONCURRENCY_FAILURE;
  }
}
