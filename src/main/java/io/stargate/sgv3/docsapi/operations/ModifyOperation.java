package io.stargate.sgv3.docsapi.operations;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv3.docsapi.bridge.query.QueryExecutor;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.exception.ModifyStatementException;
import io.stargate.sgv3.docsapi.shredding.JSONPath;
import io.stargate.sgv3.docsapi.shredding.JsonType;
import io.stargate.sgv3.docsapi.shredding.WritableShreddedDocument;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.javatuples.Pair;

/** Superclass for operations that modify data, insert, delete, update */
public abstract class ModifyOperation extends Operation {

  protected QueryOuterClass.Query buildInsertQuery(CommandContext commandContext) {
    String insert =
        "INSERT INTO %s.%s\n"
            + "            (key, tx_id, doc_properties, exist_keys, sub_doc_equals, array_size, array_equals, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values, doc_field_order, doc_atomic_fields)\n"
            + "        VALUES\n"
            + "            (?, now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    return QueryOuterClass.Query.newBuilder()
        .setCql(String.format(insert, commandContext.database, commandContext.collection))
        .build();
  }

  protected QueryOuterClass.Query buildUpdateQuery(CommandContext commandContext) {
    String update =
        "UPDATE %s.%s "
            + "        SET"
            + "            tx_id = now(),"
            + "            doc_properties = ?,"
            + "            exist_keys = ?,"
            + "            sub_doc_equals = ?,"
            + "            array_size = ?,"
            + "            array_equals = ?,"
            + "            array_contains = ?,"
            + "            query_bool_values = ?,"
            + "            query_dbl_values = ?,"
            + "            query_text_values = ?,"
            + "            query_null_values = ?,"
            + "            doc_field_order = ?,"
            + "            doc_atomic_fields  = ?"
            + "        WHERE "
            + "            key = ?"
            + "        IF "
            + "            tx_id = ?";
    return QueryOuterClass.Query.newBuilder()
        .setCql(String.format(update, commandContext.database, commandContext.collection))
        .build();
  }

  protected static QueryOuterClass.Query bindUpdateValues(
      QueryOuterClass.Query builtQuery, WritableShreddedDocument doc) {
    // respect the order in the DocsApiConstants.ALL_COLUMNS_NAMES
    QueryOuterClass.Values.Builder values =
        QueryOuterClass.Values.newBuilder()
            .addValues(Values.of(getIntegerMapValues(doc.properties)))
            .addValues(Values.of(getSetValue(doc.existKeys)))
            .addValues(Values.of(getStringMapValues(doc.subDocEquals)))
            .addValues(Values.of(getIntegerMapValues(doc.arraySize)))
            .addValues(Values.of(getStringMapValues(doc.arrayEquals)))
            .addValues(Values.of(getStringMapValues(doc.arrayContains)))
            .addValues(Values.of(getBooleanMapValues(doc.queryBoolValues)))
            .addValues(Values.of(getDoubleMapValues(doc.queryNumberValues)))
            .addValues(Values.of(getStringMapValues(doc.queryTextValues)))
            .addValues(Values.of(getSetValue(doc.queryNullValues)))
            .addValues(Values.of(getListValue(doc.docFieldOrder)))
            .addValues(Values.of(getRawDataValue(doc.docAtomicFields)))
            .addValues(Values.of(doc.id))
            .addValues(Values.of(doc.txID.get()));
    return QueryOuterClass.Query.newBuilder(builtQuery).setValues(values).build();
  }

  protected ModifyOperation(CommandContext commandContext) {
    super(commandContext);
  }

  @Override
  public Uni<OperationResult> execute(QueryExecutor queryExecutor) {
    Uni<ModifyOperationPage> page = executeInternal(queryExecutor);
    return page.onFailure()
        .transform(t -> new ModifyStatementException(t))
        .onItem()
        .transform(p -> p.createOperationResult());
  }

  /**
   * Implementors should do all the work to run the Operation in this function, and return
   * information on what was changed.
   *
   * <p>Implementors should not handle any database errors, they will be handled by execute()
   *
   * @return
   */
  protected abstract Uni<ModifyOperationPage> executeInternal(QueryExecutor queryExecutor);

  /**
   * Binds the query built with this query builder from supplied data.
   *
   * @param builtQuery Prepared query built by this query builder.
   * @param doc {@link WritableShreddedDocument} containing data for the row.
   * @return Bound query.
   */
  protected static QueryOuterClass.Query bindInsertValues(
      QueryOuterClass.Query builtQuery, WritableShreddedDocument doc) {
    // respect the order in the DocsApiConstants.ALL_COLUMNS_NAMES
    QueryOuterClass.Values.Builder values =
        QueryOuterClass.Values.newBuilder()
            .addValues(Values.of(doc.id))
            .addValues(Values.of(getIntegerMapValues(doc.properties)))
            .addValues(Values.of(getSetValue(doc.existKeys)))
            .addValues(Values.of(getStringMapValues(doc.subDocEquals)))
            .addValues(Values.of(getIntegerMapValues(doc.arraySize)))
            .addValues(Values.of(getStringMapValues(doc.arrayEquals)))
            .addValues(Values.of(getStringMapValues(doc.arrayContains)))
            .addValues(Values.of(getBooleanMapValues(doc.queryBoolValues)))
            .addValues(Values.of(getDoubleMapValues(doc.queryNumberValues)))
            .addValues(Values.of(getStringMapValues(doc.queryTextValues)))
            .addValues(Values.of(getSetValue(doc.queryNullValues)))
            .addValues(Values.of(getListValue(doc.docFieldOrder)))
            .addValues(Values.of(getRawDataValue(doc.docAtomicFields)));
    return QueryOuterClass.Query.newBuilder(builtQuery).setValues(values).build();
  }

  private static Map<QueryOuterClass.Value, QueryOuterClass.Value> getRawDataValue(
      Map<JSONPath, Pair<JsonType, ByteBuffer>> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JSONPath, Pair<JsonType, ByteBuffer>> entry : from.entrySet()) {
      QueryOuterClass.Value key = Values.of(entry.getKey().getPath());
      QueryOuterClass.Value valueTuple = getTupleValue(entry.getValue());
      to.put(key, valueTuple);
    }
    return to;
  }

  private static QueryOuterClass.Value getTupleValue(Pair<JsonType, ByteBuffer> value) {
    List<QueryOuterClass.Value> decoded = new ArrayList<>();
    decoded.add(Values.of(value.getValue0().value));
    decoded.add(Values.of(value.getValue1()));
    return Values.of(decoded);
  }

  private static Map<QueryOuterClass.Value, QueryOuterClass.Value> getIntegerMapValues(
      Map<JSONPath, Integer> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JSONPath, Integer> entry : from.entrySet()) {
      to.put(Values.of(entry.getKey().getPath()), Values.of(entry.getValue()));
    }
    return to;
  }

  private static Set<QueryOuterClass.Value> getSetValue(Set<JSONPath> from) {
    return from.stream().map(val -> Values.of(val.getPath())).collect(Collectors.toSet());
  }

  private static List<QueryOuterClass.Value> getListValue(List<JSONPath> from) {
    return from.stream().map(val -> Values.of(val.getPath())).collect(Collectors.toList());
  }

  private static Map<QueryOuterClass.Value, QueryOuterClass.Value> getStringMapValues(
      Map<JSONPath, String> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JSONPath, String> entry : from.entrySet()) {
      to.put(Values.of(entry.getKey().getPath()), Values.of(entry.getValue()));
    }
    return to;
  }

  private static Map<QueryOuterClass.Value, QueryOuterClass.Value> getBooleanMapValues(
      Map<JSONPath, Boolean> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JSONPath, Boolean> entry : from.entrySet()) {
      to.put(Values.of(entry.getKey().getPath()), Values.of(entry.getValue()));
    }
    return to;
  }

  private static Map<QueryOuterClass.Value, QueryOuterClass.Value> getDoubleMapValues(
      Map<JSONPath, BigDecimal> from) {
    final Map<QueryOuterClass.Value, QueryOuterClass.Value> to = new HashMap<>(from.size());
    for (Map.Entry<JSONPath, BigDecimal> entry : from.entrySet()) {
      to.put(Values.of(entry.getKey().getPath()), Values.of(entry.getValue()));
    }
    return to;
  }
}
