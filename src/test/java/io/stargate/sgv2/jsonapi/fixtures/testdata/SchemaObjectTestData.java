package io.stargate.sgv2.jsonapi.fixtures.testdata;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

public class SchemaObjectTestData extends TestDataSuplier {

  public SchemaObjectTestData(TestData testData) {
    super(testData);
  }

  public TableSchemaObject emptyTableSchemaObject() {
    return new TableSchemaObject(testData.tableMetadata().empty());
  }
}
