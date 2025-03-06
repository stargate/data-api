package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateIndex;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedCreateIndex;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.CQLSAIIndex;
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

  @Override
  protected SimpleStatement buildStatement() {

    var createIndexStart =
        SchemaBuilder.createIndex(indexDef.indexName()).custom(CQLSAIIndex.SAI_CLASS_NAME);
    // ifNotExists is an option
    createIndexStart = cqlOptions.applyBuilderOptions(createIndexStart);

    var createIndexOnTable =
        createIndexStart.onTable(
            schemaObject.tableMetadata().getKeyspace(), schemaObject.tableMetadata().getName());

    var createIndex = createIndexOnTable.andColumn(indexDef.targetColumn());

    // options are things like vector function, or text case sensitivity
    var indexOptions = indexDef.indexOptions();
    if (!indexOptions.isEmpty()) {
      createIndex = createIndex.withOption(CQL_OPTIONS_NAME, indexOptions);
    }

    // Hack code to fix the issue with respect to quoted columns, see class
    var extendedCreateIndex = new ExtendedCreateIndex((DefaultCreateIndex) createIndex);
    return extendedCreateIndex.build();
  }
}
