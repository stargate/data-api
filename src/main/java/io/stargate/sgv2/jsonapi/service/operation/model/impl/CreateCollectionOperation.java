package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public record CreateCollectionOperation(CommandContext commandContext, String name)
    implements Operation {

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    final Uni<QueryOuterClass.ResultSet> execute =
        queryExecutor.executeSchemaChange(getCreateTable(commandContext.database(), name));
    final Uni<Boolean> indexResult =
        execute
            .onItem()
            .transformToUni(
                res -> {
                  final List<QueryOuterClass.Query> indexStatements =
                      getIndexStatements(commandContext.database(), name);
                  List<Uni<QueryOuterClass.ResultSet>> indexes = new ArrayList<>(10);
                  indexStatements.stream()
                      .forEach(index -> indexes.add(queryExecutor.executeSchemaChange(index)));
                  return Uni.combine().all().unis(indexes).combinedWith(results -> true);
                });
    return indexResult.onItem().transform(res -> new SchemaChangeResult(res));
  }

  protected QueryOuterClass.Query getCreateTable(String keyspace, String table) {
    String createTable =
        "CREATE TABLE IF NOT EXISTS %s.%s ("
            + "    key                 tuple<tinyint,text>,"
            + "    tx_id               timeuuid, "
            + "    doc_json            text,"
            + "    doc_properties      map<text, int>,"
            + "    exist_keys          set<text>,"
            + "    sub_doc_equals      set<text>,"
            + "    array_size          map<text, int>,"
            + "    array_equals        map<text, text>,"
            + "    array_contains      set<text>,"
            + "    query_bool_values   map<text, tinyint>,"
            + "    query_dbl_values    map<text, decimal>,"
            + "    query_text_values   map<text, text>, "
            + "    query_null_values   set<text>,     "
            + "    PRIMARY KEY (key))";
    return QueryOuterClass.Query.newBuilder()
        .setCql(String.format(createTable, keyspace, table))
        .build();
  }

  protected List<QueryOuterClass.Query> getIndexStatements(String keyspace, String table) {
    List<QueryOuterClass.Query> statements = new ArrayList<>(10);

    String propertyIndex =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_doc_properties ON %s.%s (entries(doc_properties)) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(propertyIndex, table, keyspace, table))
            .build());

    String existKeys =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_exists_keys ON %s.%s (exist_keys) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(existKeys, table, keyspace, table))
            .build());

    String subDocEquals =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_sub_doc_equals ON %s.%s (sub_doc_equals) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(subDocEquals, table, keyspace, table))
            .build());

    String arraySize =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_size ON %s.%s (entries(array_size)) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(arraySize, table, keyspace, table))
            .build());

    String arrayEquals =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_equals ON %s.%s (entries(array_equals)) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(arrayEquals, table, keyspace, table))
            .build());

    String arrayContains =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_contains ON %s.%s (array_contains) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(arrayContains, table, keyspace, table))
            .build());

    String boolQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_bool_values ON %s.%s (entries(query_bool_values)) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(boolQuery, table, keyspace, table))
            .build());

    String dblQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_dbl_values ON %s.%s (entries(query_dbl_values)) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(dblQuery, table, keyspace, table))
            .build());

    String textQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_text_values ON %s.%s (entries(query_text_values)) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(textQuery, table, keyspace, table))
            .build());

    String nullQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_null_values ON %s.%s (query_null_values) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(nullQuery, table, keyspace, table))
            .build());

    return statements;
  }
}
