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
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import java.util.List;
import java.util.function.Supplier;

/** Operation that inserts one or more documents. */
public record InsertOperation(
    CommandContext commandContext, List<WritableShreddedDocument> documents)
    implements ModifyOperation {

  public InsertOperation(CommandContext commandContext, WritableShreddedDocument document) {
    this(commandContext, List.of(document));
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    QueryOuterClass.Query query = buildInsertQuery();
    final Uni<List<DocumentId>> ids =
        Multi.createFrom()
            .items(documents.stream())
            .onItem()
            .transformToUniAndConcatenate(doc -> insertDocument(queryExecutor, query, doc))
            .collect()
            .asList();
    return ids.onItem().transform(insertedIds -> new InsertOperationPage(insertedIds, documents));
  }

  private static Uni<DocumentId> insertDocument(
      QueryExecutor queryExecutor, QueryOuterClass.Query query, WritableShreddedDocument doc) {
    query = bindInsertValues(query, doc);
    return queryExecutor.executeWrite(query).onItem().transform(result -> doc.id());
  }

  private QueryOuterClass.Query buildInsertQuery() {
    String insert =
        "INSERT INTO %s.%s"
            + "            (key, tx_id, doc_json, doc_properties, exist_keys, sub_doc_equals, array_size, array_equals, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values)"
            + "        VALUES"
            + "            (?, now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    return QueryOuterClass.Query.newBuilder()
        .setCql(String.format(insert, commandContext.namespace(), commandContext.collection()))
        .build();
  }

  private static QueryOuterClass.Query bindInsertValues(
      QueryOuterClass.Query builtQuery, WritableShreddedDocument doc) {
    // respect the order in the DocsApiConstants.ALL_COLUMNS_NAMES
    QueryOuterClass.Values.Builder values =
        QueryOuterClass.Values.newBuilder()
            .addValues(Values.of(CustomValueSerializers.getDocumentIdValue(doc.id())))
            .addValues(Values.of(doc.docJson()))
            .addValues(Values.of(CustomValueSerializers.getIntegerMapValues(doc.docProperties())))
            .addValues(Values.of(CustomValueSerializers.getSetValue(doc.existKeys())))
            .addValues(Values.of(CustomValueSerializers.getStringMapValues(doc.subDocEquals())))
            .addValues(Values.of(CustomValueSerializers.getIntegerMapValues(doc.arraySize())))
            .addValues(Values.of(CustomValueSerializers.getStringMapValues(doc.arrayEquals())))
            .addValues(Values.of(CustomValueSerializers.getStringSetValue(doc.arrayContains())))
            .addValues(Values.of(CustomValueSerializers.getBooleanMapValues(doc.queryBoolValues())))
            .addValues(
                Values.of(CustomValueSerializers.getDoubleMapValues(doc.queryNumberValues())))
            .addValues(Values.of(CustomValueSerializers.getStringMapValues(doc.queryTextValues())))
            .addValues(Values.of(CustomValueSerializers.getSetValue(doc.queryNullValues())));
    return QueryOuterClass.Query.newBuilder(builtQuery).setValues(values).build();
  }
}
