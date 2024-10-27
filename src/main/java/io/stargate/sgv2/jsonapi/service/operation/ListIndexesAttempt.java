package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.IndexDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.List;

/** Attempt to list indexes for a table. */
public class ListIndexesAttempt extends MetadataAttempt<TableSchemaObject> {

  protected ListIndexesAttempt(int position, TableSchemaObject schemaObject) {
    super(position, schemaObject, RetryPolicy.NO_RETRY);
    setStatus(OperationStatus.READY);
  }

  /**
   * Get indexes names from the table metadata.
   *
   * @return List of index names.
   */
  @Override
  protected List<String> getNames() {
    return schemaObject.indexConfig().indexDefinitions().values().stream()
        .map(IndexDefinition::indexName)
        .toList();
  }

  /**
   * Get indexes schema for the tables.
   *
   * @return List of indexes schema as Object.
   */
  @Override
  protected Object getSchema() {
    return schemaObject.indexConfig().indexDefinitions().values().stream()
        .map(IndexDefinition::getIndexDefinition)
        .toList();
  }
}
