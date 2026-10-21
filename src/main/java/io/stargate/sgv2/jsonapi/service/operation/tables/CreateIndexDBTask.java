package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndex;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndexOnTable;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateIndex;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedCreateIndex;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.CQLSAIIndex;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import java.util.Objects;

/*
 An attempt to create index for a table's column.
*/
public class CreateIndexDBTask extends SchemaDBTask<TableSchemaObject> {

  private final ApiIndexDef indexDef;
  private final CQLOptions.CreateIndexStartCQLOptions cqlOptions;

  // a little confusing , we need to tell the query builder to add an option to the create index
  // called "options", we then encode all the options in that see example:
  // https://cassandra.apache.org/doc/latest/cassandra/developing/cql/indexing/sai/sai-working-with.html#create-sai-index
  private static final String CQL_OPTIONS_NAME = "OPTIONS";

  protected CreateIndexDBTask(
      int position,
      TableSchemaObject schemaObject,
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy,
      DefaultDriverExceptionHandler.Factory<TableSchemaObject> exceptionHandlerFactory,
      ApiIndexDef indexDef,
      CQLOptions.CreateIndexStartCQLOptions cqlOptions) {
    super(position, schemaObject, schemaRetryPolicy, exceptionHandlerFactory);

    this.indexDef = Objects.requireNonNull(indexDef, "indexDef cannot be null");
    this.cqlOptions = Objects.requireNonNull(cqlOptions, "cqlOptions cannot be null");
    setStatus(TaskStatus.READY);
  }

  public static CreateIndexDBTaskBuilder builder(TableSchemaObject schemaObject) {
    return new CreateIndexDBTaskBuilder(schemaObject);
  }

  /**
   * Returns the CQL custom index class name to use for this index.
   *
   * <p>SAI indexes use {@link CQLSAIIndex#SAI_CLASS_NAME}; OpenSearch indexes use {@link
   * CQLSAIIndex#OPEN_SEARCH_CLASS_NAME}.
   */
  private String indexClassName() {
    if (indexDef.indexType()
        == io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexType.OPEN_SEARCH) {
      return CQLSAIIndex.OPEN_SEARCH_CLASS_NAME;
    }
    return CQLSAIIndex.SAI_CLASS_NAME;
  }

  @Override
  protected SimpleStatement buildStatement() {

    var createIndexStart = SchemaBuilder.createIndex(indexDef.indexName()).custom(indexClassName());
    // ifNotExists is an option
    createIndexStart = cqlOptions.applyBuilderOptions(createIndexStart);

    var createIndexOnTable =
        createIndexStart.onTable(
            schemaObject.tableMetadata().getKeyspace(), schemaObject.tableMetadata().getName());

    var createIndex = createIndexWithIndexFunction(indexDef, createIndexOnTable);

    // options are things like vector function, or text case sensitivity
    var indexOptions = indexDef.indexOptions();
    if (!indexOptions.isEmpty()) {
      createIndex = createIndex.withOption(CQL_OPTIONS_NAME, indexOptions);
    }

    // Hack code to fix the issue with respect to quoted columns, see class
    var extendedCreateIndex = new ExtendedCreateIndex((DefaultCreateIndex) createIndex);
    return extendedCreateIndex.build();
  }

  /**
   * Index Function keys/values/entries on map/set/list collection columns needs special handling to
   * create the driver CreateIndex. <br>
   * If indexFunction is null, then it is a regular index on a scalar column.
   *
   * <p>For OpenSearch indexes with multiple columns the driver's {@code andColumn()} is called for
   * each column. When the columns list is empty HCD replicates all non-PK columns automatically, so
   * we call {@code andColumn()} with an empty string token to emit {@code ON table ()} — the driver
   * accepts this and produces the correct empty-column-list form.
   */
  private CreateIndex createIndexWithIndexFunction(
      ApiIndexDef apiIndexDef, CreateIndexOnTable createIndexOnTable) {

    // OpenSearch multi-column case
    if (apiIndexDef
        instanceof io.stargate.sgv2.jsonapi.service.schema.tables.ApiOpenSearchIndex osIndex) {
      var cols = osIndex.columns();
      if (cols.isEmpty()) {
        // No column restriction: HCD replicates all non-PK columns. The driver builder has no
        // column, ExtendedCreateIndex renders "ON table ()" for a custom index.
        return (CreateIndex) createIndexOnTable;
      }
      // Each andColumn() returns a new builder holding the columns added so far, so the next
      // column must be added to the returned builder (DefaultCreateIndex implements both
      // CreateIndex and CreateIndexOnTable), not to the original createIndexOnTable.
      CreateIndex ci = createIndexOnTable.andColumn(cols.getFirst());
      for (int i = 1; i < cols.size(); i++) {
        ci = ((CreateIndexOnTable) ci).andColumn(cols.get(i));
      }
      return ci;
    }

    var apiIndexFunction = apiIndexDef.indexFunction();
    var indexColumn = apiIndexDef.targetColumn();

    if (apiIndexFunction == null) {
      return createIndexOnTable.andColumn(indexColumn);
    }

    return switch (apiIndexFunction) {
      case KEYS -> createIndexOnTable.andColumnKeys(indexColumn);
      case VALUES -> createIndexOnTable.andColumnValues(indexColumn);
      case ENTRIES -> createIndexOnTable.andColumnEntries(indexColumn);
    };
  }
}
