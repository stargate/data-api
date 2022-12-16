package io.stargate.sgv3.docsapi.operations;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv3.docsapi.bridge.query.QueryExecutor;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.exception.CollectionCreationException;
import java.util.ArrayList;
import java.util.List;

public class CreateCollectionOperation extends SchemaChangeOperation {
  private final String name;

  public CreateCollectionOperation(CommandContext commandContext, String name) {
    super(commandContext);
    this.name = name;
  }

  @Override
  protected Uni<SchemaChangeResult> executeInternal(QueryExecutor queryExecutor) {
    final Uni<QueryOuterClass.ResultSet> execute =
        queryExecutor.execute(getCreateTable(getCommandContext().database, name));
    final Uni<Boolean> indexResult =
        execute
            .onItem()
            .transformToUni(
                res -> {
                  final List<QueryOuterClass.Query> indexStatements =
                      getIndexStatements(getCommandContext().database, name);
                  List<Uni<QueryOuterClass.ResultSet>> indexes = new ArrayList<>(10);
                  indexStatements.stream()
                      .forEach(index -> indexes.add(queryExecutor.execute(index)));
                  return Uni.combine()
                      .all()
                      .unis(indexes)
                      .collectFailures()
                      .combinedWith(
                          results -> {
                            if (results.size() > 0) return false;
                            else return true;
                          });
                });
    return indexResult
        .onFailure()
        .transform(
            t -> {
              return new CollectionCreationException(t);
            })
        .onItem()
        .transformToUni(res -> Uni.createFrom().item(SchemaChangeResult.from(res)));
  }

  protected QueryOuterClass.Query getCreateTable(String keyspace, String table) {
    String createTable =
        "CREATE TABLE IF NOT EXISTS %s.%s ("
            + "    key                 text,"
            + "    tx_id               timeuuid, "
            + "    doc_properties      map<text, int>,"
            + "    exist_keys          set<text>,"
            + "    sub_doc_equals      set<text>,"
            + "    array_size          map<text, int>,"
            + "    array_equals        map<text, text>,"
            + "    array_contains      set<text>,"
            + "    query_bool_values   map<text, boolean>,"
            + "    query_dbl_values    map<text, decimal>,"
            + "    query_text_values   map<text, text>, "
            + "    query_null_values   set<text>,     "
            + "    doc_field_order     list<text>, "
            + "    doc_atomic_fields   map<text,tuple<int, blob>>,"
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
    /*statements.add(
    QueryOuterClass.Query.newBuilder()
        .setCql(String.format(boolQuery, table, keyspace, table))
        .build());*/

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

  private QueryOuterClass.Query selectBuilder(CommandContext commandContext, int limit) {
    final QueryBuilder.QueryBuilder__21 from =
        new QueryBuilder()
            .select()
            .column("key", "tx_id", "doc_field_order", "doc_atomic_fields")
            .from(commandContext.database, commandContext.collection);
    if (limit != 0) {
      return from.limit(limit).build();
    } else {
      return from.build();
    }
  }

  @Override
  public OperationPlan getPlan() {
    return new OperationPlan(true);
  }
}
