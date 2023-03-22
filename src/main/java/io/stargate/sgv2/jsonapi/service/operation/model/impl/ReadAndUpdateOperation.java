package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ModifyOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * This operation method is used for 3 commands findOneAndUpdate, updateOne and updateMany
 *
 * @param commandContext
 * @param findOperation
 * @param documentUpdater
 * @param returnDocumentInResponse - if `true` return document
 * @param returnUpdatedDocument - if `true` return after update document, else before document
 * @param upsert - flag to suggest insert as new document if no documents in DB matches the
 *     condition
 * @param shredder
 * @param updateLimit - Number of documents to be updated
 * @param retryLimit - Number of times retry to happen in case of lwt failure
 */
public record ReadAndUpdateOperation(
    CommandContext commandContext,
    FindOperation findOperation,
    DocumentUpdater documentUpdater,
    boolean returnDocumentInResponse,
    boolean returnUpdatedDocument,
    boolean upsert,
    Shredder shredder,
    int updateLimit,
    int retryLimit)
    implements ModifyOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    final AtomicBoolean moreDataFlag = new AtomicBoolean(false);
    final AtomicInteger matchedCount = new AtomicInteger(0);
    final AtomicInteger modifiedCount = new AtomicInteger(0);
    return Multi.createBy()
        .repeating()
        .uni(
            () -> new AtomicReference<String>(null),
            stateRef -> {
              Uni<ReadOperation.FindResponse> docsToUpdate =
                  findOperation().getDocuments(queryExecutor, stateRef.get(), null);
              return docsToUpdate
                  .onItem()
                  .invoke(findResponse -> stateRef.set(findResponse.pagingState()));
            })
        // Read document while pagingState exists, limit for read is set at updateLimit +1
        .whilst(findResponse -> findResponse.pagingState() != null)
        // Transform to get only the updateLimit records, if more set `moreData` to true
        .onItem()
        .transformToMulti(
            findResponse -> {
              final List<ReadDocument> docs = findResponse.docs();
              if (upsert() && docs.size() == 0 && matchedCount.get() == 0) {
                return Multi.createFrom().item(findOperation().getNewDocument());
              } else {
                // Below conditionality is because we read up to updateLimit +1 record.
                if (matchedCount.get() + docs.size() <= updateLimit) {
                  matchedCount.addAndGet(docs.size());
                  return Multi.createFrom().items(docs.stream());
                } else {
                  int needed = updateLimit - matchedCount.get();
                  matchedCount.addAndGet(needed);
                  moreDataFlag.set(true);
                  return Multi.createFrom().items(findResponse.docs().subList(0, needed).stream());
                }
              }
            })
        .concatenate()
        // Update the read documents
        .onItem()
        .transformToUniAndMerge(
            readDocument ->
                processUpdate(readDocument, queryExecutor, modifiedCount)
                    .onFailure(LWTException.class)
                    .recoverWithUni(
                        () -> {
                          // Retry `retryLimit` times in case of LWT failure
                          return Uni.createFrom()
                              .item(readDocument)
                              .flatMap(
                                  prevDoc -> {
                                    // read the document again
                                    return readDocumentAgain(queryExecutor, prevDoc)
                                        .onItem()
                                        // Try updating the document
                                        .transformToUni(
                                            reReadDocument ->
                                                processUpdate(
                                                    reReadDocument, queryExecutor, modifiedCount));
                                  })
                              .onFailure(LWTException.class)
                              .retry()
                              // because it's already run twice before this
                              // check.
                              .atMost(retryLimit - 1)
                              .onFailure()
                              .recoverWithItem(
                                  error -> {
                                    return new UpdatedDocument(
                                        readDocument.id(), false, null, error);
                                  });
                        }))
        .collect()
        .asList()
        .onItem()
        .transform(
            updates ->
                new UpdateOperationPage(
                    matchedCount.get(),
                    modifiedCount.get(),
                    updates,
                    returnDocumentInResponse(),
                    moreDataFlag.get()));
  }

  private Uni<UpdatedDocument> processUpdate(
      ReadDocument document, QueryExecutor queryExecutor, AtomicInteger modifiedCount) {
    return Uni.createFrom()
        .item(document)

        // perform update operation and save only if data is modified.
        .flatMap(
            readDocument -> {

              // if there is no document return null item
              if (readDocument == null) {
                return Uni.createFrom().nullItem();
              }

              // upsert if we have no transaction if before
              boolean upsert = readDocument.txnId() == null;
              JsonNode originalDocument = upsert ? null : readDocument.document();

              // apply document updates
              // if no changes return null item
              DocumentUpdater.DocumentUpdaterResponse documentUpdaterResponse =
                  documentUpdater().applyUpdates(readDocument.document().deepCopy(), upsert);
              if (!documentUpdaterResponse.modified()) {
                return Uni.createFrom().nullItem();
              }

              // otherwise shred
              JsonNode updatedDocument = documentUpdaterResponse.document();
              final WritableShreddedDocument writableShreddedDocument =
                  shredder().shred(updatedDocument, readDocument.txnId());

              // update the document
              return updatedDocument(queryExecutor, writableShreddedDocument)

                  // send result back depending on the input
                  .onItem()
                  .ifNotNull()
                  .transform(
                      v -> {
                        // if not insert increment modified count
                        if (!upsert) modifiedCount.incrementAndGet();

                        // resolve doc to return
                        JsonNode documentToReturn = null;
                        if (returnDocumentInResponse) {
                          documentToReturn =
                              returnUpdatedDocument ? updatedDocument : originalDocument;
                        }
                        return new UpdatedDocument(
                            writableShreddedDocument.id(), upsert, documentToReturn, null);
                      });
            });
  }

  private Uni<DocumentId> updatedDocument(
      QueryExecutor queryExecutor, WritableShreddedDocument writableShreddedDocument) {
    final QueryOuterClass.Query updateQuery =
        bindUpdateValues(buildUpdateQuery(), writableShreddedDocument);
    return queryExecutor
        .executeWrite(updateQuery)
        .onItem()
        .transformToUni(
            result -> {
              if (result.getRows(0).getValues(0).getBoolean()) {
                return Uni.createFrom().item(writableShreddedDocument.id());
              } else {
                throw new LWTException(ErrorCode.CONCURRENCY_FAILURE);
              }
            });
  }

  private QueryOuterClass.Query buildUpdateQuery() {
    String update =
        "UPDATE %s.%s "
            + "        SET"
            + "            tx_id = now(),"
            + "            exist_keys = ?,"
            + "            sub_doc_equals = ?,"
            + "            array_size = ?,"
            + "            array_equals = ?,"
            + "            array_contains = ?,"
            + "            query_bool_values = ?,"
            + "            query_dbl_values = ?,"
            + "            query_text_values = ?,"
            + "            query_null_values = ?,"
            + "            doc_json  = ?"
            + "        WHERE "
            + "            key = ?"
            + "        IF "
            + "            tx_id = ?";
    return QueryOuterClass.Query.newBuilder()
        .setCql(String.format(update, commandContext.namespace(), commandContext.collection()))
        .build();
  }

  protected static QueryOuterClass.Query bindUpdateValues(
      QueryOuterClass.Query builtQuery, WritableShreddedDocument doc) {
    // respect the order in the DocsApiConstants.ALL_COLUMNS_NAMES
    QueryOuterClass.Values.Builder values =
        QueryOuterClass.Values.newBuilder()
            .addValues(Values.of(CustomValueSerializers.getSetValue(doc.existKeys())))
            .addValues(Values.of(CustomValueSerializers.getStringMapValues(doc.subDocEquals())))
            .addValues(Values.of(CustomValueSerializers.getIntegerMapValues(doc.arraySize())))
            .addValues(Values.of(CustomValueSerializers.getStringMapValues(doc.arrayEquals())))
            .addValues(Values.of(CustomValueSerializers.getStringSetValue(doc.arrayContains())))
            .addValues(Values.of(CustomValueSerializers.getBooleanMapValues(doc.queryBoolValues())))
            .addValues(
                Values.of(CustomValueSerializers.getDoubleMapValues(doc.queryNumberValues())))
            .addValues(Values.of(CustomValueSerializers.getStringMapValues(doc.queryTextValues())))
            .addValues(Values.of(CustomValueSerializers.getSetValue(doc.queryNullValues())))
            .addValues(Values.of(doc.docJson()))
            .addValues(Values.of(CustomValueSerializers.getDocumentIdValue(doc.id())))
            .addValues(doc.txID() == null ? Values.NULL : Values.of(doc.txID()));
    return QueryOuterClass.Query.newBuilder(builtQuery).setValues(values).build();
  }

  /**
   * Utility method to read the document again, in case of lwt error
   *
   * @param queryExecutor
   * @param prevReadDoc
   * @return
   */
  private Uni<ReadDocument> readDocumentAgain(
      QueryExecutor queryExecutor, ReadDocument prevReadDoc) {
    return findOperation()
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

  record UpdatedDocument(DocumentId id, boolean upserted, JsonNode document, Throwable error) {}
}
