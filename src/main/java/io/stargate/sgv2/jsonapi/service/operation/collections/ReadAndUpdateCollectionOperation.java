package io.stargate.sgv2.jsonapi.service.operation.collections;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.exception.unchecked.LWTFailureException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObjectName;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizerService;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * This operation method is used for 3 commands findOneAndUpdate, updateOne and updateMany
 *
 * @param commandContext
 * @param findCollectionOperation
 * @param documentUpdater
 * @param returnDocumentInResponse - if `true` return document
 * @param returnUpdatedDocument - if `true` return after update document, else before document
 * @param upsert - flag to suggest insert as new document if no documents in DB matches the
 *     condition
 * @param documentShredder
 * @param updateLimit - Number of documents to be updated
 * @param retryLimit - Number of times retry to happen in case of lwt failure
 */
public record ReadAndUpdateCollectionOperation(
    CommandContext<CollectionSchemaObject> commandContext,
    FindCollectionOperation findCollectionOperation,
    DocumentUpdater documentUpdater,
    DataVectorizerService dataVectorizerService,
    boolean returnDocumentInResponse,
    boolean returnUpdatedDocument,
    boolean upsert,
    DocumentShredder documentShredder,
    /**
     * Projection used on document to return (whether before or after updates), if projection
     * needed: if not, an identity projection.
     */
    DocumentProjector resultProjection,
    int updateLimit,
    int retryLimit)
    implements CollectionModifyOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) {
    final AtomicReference<String> pageStateReference = new AtomicReference<>();
    final AtomicInteger matchedCount = new AtomicInteger(0);
    final AtomicInteger modifiedCount = new AtomicInteger(0);
    Uni<CollectionReadOperation.FindResponse> docsToUpdate =
        findCollectionOperation()
            .getDocuments(
                dataApiRequestInfo, queryExecutor, findCollectionOperation().pageState(), null);
    return docsToUpdate
        .onItem()
        .transformToMulti(
            findResponse -> {
              pageStateReference.set(findResponse.pageState());
              final List<ReadDocument> docs = findResponse.docs();
              if (upsert() && docs.isEmpty() && matchedCount.get() == 0) {
                // TODO: creating the new document here, with the defaults from the filter, makes it
                // harder because the new document created here may nto have an _id if there was
                // none
                // in the filter. A better approach may be to have the documentUpdater create
                // the upsert document totally in once place.
                // Currently creating to upsert document is in multiple places. To do this we would
                // create
                // UpdateOperations from the filter and give them to the document updated when it is
                // created.
                return Multi.createFrom().item(findCollectionOperation().getNewDocument());
              } else {
                matchedCount.addAndGet(docs.size());
                return Multi.createFrom().items(docs.stream());
              }
            })
        .onItem()
        .transformToUniAndConcatenate(
            readDocument ->
                processUpdate(dataApiRequestInfo, readDocument, queryExecutor, modifiedCount)
                    .onFailure(LWTFailureException.class)
                    .recoverWithUni(
                        () -> {
                          // Retry `retryLimit` times in case of LWT failure
                          return Uni.createFrom()
                              .item(readDocument)
                              .flatMap(
                                  prevDoc -> {
                                    // read the document again
                                    return readDocumentAgain(
                                            dataApiRequestInfo, queryExecutor, prevDoc)
                                        .onItem()
                                        // Try updating the document
                                        .transformToUni(
                                            reReadDocument ->
                                                processUpdate(
                                                    dataApiRequestInfo,
                                                    reReadDocument,
                                                    queryExecutor,
                                                    modifiedCount));
                                  })
                              .onFailure(LWTFailureException.class)
                              .retry()
                              // because it's already run twice before this
                              // check.
                              .atMost(retryLimit - 1)
                              .onFailure()
                              .recoverWithItem(
                                  error -> {
                                    // AJM - GH #2309 - this means we failed all retries to get the
                                    // LWT to apply
                                    // we now need to create the error to return to the user, and we
                                    // track these
                                    // per document we are trying to update.
                                    var dbError =
                                        DatabaseException.Code.FAILED_CONCURRENT_OPERATIONS.get(
                                            errVars(commandContext().schemaObject()));
                                    return new UpdatedDocument(
                                        readDocument.id().orElseThrow(), false, null, dbError);
                                  });
                        }))
        .collect()
        .asList()
        .onItem()
        .transform(
            updates -> {
              // create json doc read/write metrics
              commandContext
                  .jsonProcessingMetricsReporter()
                  .reportJsonReadDocsMetrics(commandContext().commandName(), matchedCount.get());
              commandContext
                  .jsonProcessingMetricsReporter()
                  .reportJsonWrittenDocsMetrics(
                      commandContext().commandName(), modifiedCount.get());
              return new UpdateCollectionOperationPage(
                  matchedCount.get(),
                  modifiedCount.get(),
                  updates,
                  returnDocumentInResponse(),
                  pageStateReference.get());
            });
  }

  private Uni<UpdatedDocument> processUpdate(
      RequestContext dataApiRequestInfo,
      ReadDocument document,
      QueryExecutor queryExecutor,
      AtomicInteger modifiedCount) {
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
              boolean upsert = readDocument.txnId().isEmpty();
              JsonNode originalDocument = upsert ? null : readDocument.get();

              DocumentUpdater.DocumentUpdaterResponse documentUpdaterResponse =
                  documentUpdater().apply(readDocument.get().deepCopy(), upsert);

              return documentUpdaterResponse
                  .updateEmbeddingVector(
                      documentUpdaterResponse, dataVectorizerService, commandContext)
                  .onItem()
                  .transformToUni(
                      vectorizedDocumentUpdaterResponse -> {
                        // In case no change to document and not an upsert document, short circuit
                        // and return
                        if (!vectorizedDocumentUpdaterResponse.modified() && !upsert) {
                          // If no change return the original document Issue #390
                          if (returnDocumentInResponse) {
                            resultProjection.applyProjection(originalDocument);
                            return Uni.createFrom()
                                .item(
                                    new UpdatedDocument(
                                        readDocument.id().orElseThrow(),
                                        upsert,
                                        originalDocument,
                                        null));
                          } else {
                            return Uni.createFrom().nullItem();
                          }
                        }

                        final WritableShreddedDocument writableShreddedDocument =
                            documentShredder()
                                .shred(
                                    commandContext(),
                                    vectorizedDocumentUpdaterResponse.document(),
                                    readDocument
                                        .txnId()
                                        .orElse(
                                            null)); // will be empty when this is an upserted doc

                        // Have to do this because shredder adds _id field to the document if it
                        // doesn't exist
                        JsonNode updatedDocument = writableShreddedDocument.docJsonNode();
                        // update the document
                        return updatedDocument(
                                dataApiRequestInfo, queryExecutor, writableShreddedDocument)

                            // send result back depending on the input
                            .onItem()
                            .ifNotNull()
                            .transform(
                                v -> {
                                  // if not insert increment modified count
                                  if (!upsert) {
                                    modifiedCount.incrementAndGet();
                                  }

                                  // resolve doc to return
                                  JsonNode documentToReturn = null;
                                  if (returnDocumentInResponse) {
                                    documentToReturn =
                                        returnUpdatedDocument ? updatedDocument : originalDocument;
                                    // Some operations (findOneAndUpdate) define projection to apply
                                    // to
                                    // result:
                                    if (documentToReturn != null) { // null for some Operation tests
                                      resultProjection.applyProjection(documentToReturn);
                                    }
                                  }
                                  return new UpdatedDocument(
                                      writableShreddedDocument.id(),
                                      upsert,
                                      documentToReturn,
                                      null);
                                });
                      });
            });
  }

  private Uni<DocumentId> updatedDocument(
      RequestContext dataApiRequestInfo,
      QueryExecutor queryExecutor,
      WritableShreddedDocument writableShreddedDocument) {
    final boolean vectorEnabled = commandContext().schemaObject().vectorConfig().vectorEnabled();
    final boolean lexicalEnabled = commandContext().schemaObject().lexicalConfig().enabled();

    final SimpleStatement updateQuery =
        bindUpdateValues(
            buildUpdateQuery(vectorEnabled, lexicalEnabled),
            writableShreddedDocument,
            vectorEnabled,
            lexicalEnabled);
    return queryExecutor
        .executeWrite(dataApiRequestInfo, updateQuery)
        .onItem()
        .transformToUni(
            result -> {
              if (result.wasApplied()) {
                return Uni.createFrom().item(writableShreddedDocument.id());
              } else {
                throw new LWTFailureException(updateQuery);
              }
            });
  }

  private String buildUpdateQuery(boolean vectorEnabled, boolean lexicalEnabled) {
    final SchemaObjectName tableName = commandContext.schemaObject().name();
    return buildUpdateQuery(tableName.keyspace(), tableName.table(), vectorEnabled, lexicalEnabled);
  }

  // NOTE: This method is used in the test code (to avoid having to copy query Strings verbatim),
  // so it should not be changed to private or non-static
  static String buildUpdateQuery(
      String keyspaceName, String collectionName, boolean vectorEnabled, boolean lexicalEnabled) {
    StringBuilder updateQuery = new StringBuilder(200);
    updateQuery
        .append("UPDATE \"")
        .append(keyspaceName)
        .append("\".\"")
        .append(collectionName)
        .append("\" SET ")
        .append(
            """
tx_id = now(),
exist_keys = ?,
array_size = ?,
array_contains = ?,
query_bool_values = ?,
query_dbl_values = ?,
query_text_values = ?,
query_null_values = ?,
query_timestamp_values = ?,
""");
    if (vectorEnabled) {
      updateQuery.append("\nquery_vector_value = ?,");
    }
    if (lexicalEnabled) {
      updateQuery.append("\nquery_lexical_value = ?,");
    }
    updateQuery.append(
        """
doc_json  = ?
WHERE key = ?
IF tx_id = ?
""");

    return updateQuery.toString();
  }

  protected static SimpleStatement bindUpdateValues(
      String builtQuery,
      WritableShreddedDocument doc,
      boolean vectorEnabled,
      boolean lexicalEnabled) {
    // Note: must match the order in query string constructed with `buildUpdateQuery()`
    // Build dynamically due to number of permutations
    List<Object> positional = new ArrayList<>(16); // from 12 to 14 entries currently

    positional.add(CQLBindValues.getSetValue(doc.existKeys()));
    positional.add(CQLBindValues.getIntegerMapValues(doc.arraySize()));
    positional.add(CQLBindValues.getStringSetValue(doc.arrayContains()));
    positional.add(CQLBindValues.getBooleanMapValues(doc.queryBoolValues()));
    positional.add(CQLBindValues.getDoubleMapValues(doc.queryNumberValues()));
    positional.add(CQLBindValues.getStringMapValues(doc.queryTextValues()));
    positional.add(CQLBindValues.getSetValue(doc.queryNullValues()));
    positional.add(CQLBindValues.getTimestampMapValues(doc.queryTimestampValues()));
    if (vectorEnabled) {
      positional.add(CQLBindValues.getVectorValue(doc.queryVectorValues()));
    }
    if (lexicalEnabled) {
      positional.add(doc.queryLexicalValue());
    }
    positional.add(doc.docJson());
    positional.add(CQLBindValues.getDocumentIdValue(doc.id()));
    positional.add(doc.txID());
    return SimpleStatement.newInstance(builtQuery, positional.toArray(new Object[0]));
  }

  /** Utility method to read the document again, in case of lwt error */
  private Uni<ReadDocument> readDocumentAgain(
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor, ReadDocument prevReadDoc) {
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
                return response.docs().getFirst();
              }
              // If data changed and doesn't satisfy filter conditions
              return null;
            });
  }

  /**
   * Container for the status of updating a document, callers should wrap any throwable that is not
   * a {@link APIException} into a {@link
   * io.stargate.sgv2.jsonapi.exception.ServerException.Code#UNEXPECTED_SERVER_ERROR}
   */
  record UpdatedDocument(DocumentId id, boolean upserted, JsonNode document, APIException error) {}
}
