package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ModifyOperation;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import java.util.List;
import java.util.function.Supplier;

/**
 * Operation that inserts one or more documents.
 *
 * @param commandContext Context that defines namespace and database.
 * @param documents Documents to insert.
 * @param ordered If insert should be ordered.
 */
public record InsertOperation(
    CommandContext commandContext, List<WritableShreddedDocument> documents, boolean ordered)
    implements ModifyOperation {

  public InsertOperation(CommandContext commandContext, WritableShreddedDocument document) {
    this(commandContext, List.of(document), true);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    if (ordered) {
      return insertOrdered(queryExecutor);
    } else {
      return insertUnordered(queryExecutor);
    }
  }

  // implementation for the ordered insert
  private Uni<Supplier<CommandResult>> insertOrdered(QueryExecutor queryExecutor) {
    // build query once
    QueryOuterClass.Query query = buildInsertQuery();

    return Multi.createFrom()
        .iterable(documents)

        // concatenate to respect ordered
        .onItem()
        .transformToUni(
            doc ->
                insertDocument(queryExecutor, query, doc)

                    // wrap item and failure
                    // the collection can decide how to react on failure
                    .onItemOrFailure()
                    .transform((id, t) -> Tuple2.of(doc, t)))
        .concatenate(false)

        // if no failures reduce to the op page
        .collect()
        .in(
            InsertOperationPage::new,
            (agg, in) -> {
              Throwable failure = in.getItem2();
              agg.aggregate(in.getItem1().id(), failure);

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
  private Uni<Supplier<CommandResult>> insertUnordered(QueryExecutor queryExecutor) {
    // build query once
    QueryOuterClass.Query query = buildInsertQuery();

    return Multi.createFrom()
        .iterable(documents)

        // merge to make it parallel
        .onItem()
        .transformToUniAndMerge(
            doc ->
                insertDocument(queryExecutor, query, doc)

                    // handle errors fail silent mode
                    .onItemOrFailure()
                    .transform((id, t) -> Tuple2.of(doc, t)))

        // then reduce here
        .collect()
        .in(InsertOperationPage::new, (agg, in) -> agg.aggregate(in.getItem1().id(), in.getItem2()))

        // use object identity to resolve to Supplier<CommandResult>
        .map(i -> i);
  }

  // inserts a single document
  private static Uni<DocumentId> insertDocument(
      QueryExecutor queryExecutor, QueryOuterClass.Query query, WritableShreddedDocument doc) {
    // bind and execute
    QueryOuterClass.Query bindedQuery = bindInsertValues(query, doc);

    return queryExecutor
        .executeWrite(bindedQuery)

        // ensure document was written, if no applied continue with error
        .onItem()
        .transformToUni(
            result -> {
              if (result.getRows(0).getValues(0).getBoolean()) {
                return Uni.createFrom().item(doc.id());
              } else {
                Exception failure = new JsonApiException(ErrorCode.DOCUMENT_ALREADY_EXISTS);
                return Uni.createFrom().failure(failure);
              }
            });
  }

  // utility for building the insert query
  private QueryOuterClass.Query buildInsertQuery() {
    String insert =
        "INSERT INTO \"%s\".\"%s\""
            + " (key, tx_id, doc_json, exist_keys, sub_doc_equals, array_size, array_equals, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values, query_timestamp_values)"
            + " VALUES"
            + " (?, now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  IF NOT EXISTS";

    return QueryOuterClass.Query.newBuilder()
        .setCql(String.format(insert, commandContext.namespace(), commandContext.collection()))
        .build();
  }

  // utility for query binding
  private static QueryOuterClass.Query bindInsertValues(
      QueryOuterClass.Query builtQuery, WritableShreddedDocument doc) {
    // respect the order in the DocsApiConstants.ALL_COLUMNS_NAMES
    QueryOuterClass.Values.Builder values =
        QueryOuterClass.Values.newBuilder()
            .addValues(Values.of(CustomValueSerializers.getDocumentIdValue(doc.id())))
            .addValues(Values.of(doc.docJson()))
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
            .addValues(
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(doc.queryTimestampValues())));
    return QueryOuterClass.Query.newBuilder(builtQuery).setValues(values).build();
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
