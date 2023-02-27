package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
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
    int deleteLimit)
    implements ModifyOperation {
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    final AtomicBoolean boolenFlag = new AtomicBoolean(false);
    final QueryOuterClass.Query delete = buildDeleteQuery();
    final Multi<ReadOperation.FindResponse> findResponses =
        Multi.createBy()
            .repeating()
            .uni(
                () -> new AtomicReference<String>(null),
                stateRef -> {
                  Uni<ReadOperation.FindResponse> docsToDelete =
                      readOperation().getDocuments(queryExecutor, stateRef.get());
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
                    boolenFlag.set(true);
                    return Multi.createFrom()
                        .items(findResponse.docs().subList(0, needed).stream());
                  }
                })
            .concatenate()
            .onItem()
            .transformToUniAndConcatenate(
                readDocument -> deleteDocument(queryExecutor, delete, readDocument))
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
        .transform(
            deletedCounter -> new DeleteOperationPage(deletedCounter.get(), boolenFlag.get()));
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
   * @return
   */
  private static Uni<Boolean> deleteDocument(
      QueryExecutor queryExecutor, QueryOuterClass.Query query, ReadDocument doc) {
    query = bindDeleteQuery(query, doc);
    return queryExecutor
        .executeWrite(query)
        .onItem()
        .transformToUni(
            result -> {
              if (result.getRows(0).getValues(0).getBoolean()) {
                return Uni.createFrom().item(true);
              } else {
                return Uni.createFrom().item(false);
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
