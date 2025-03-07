package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

public class SchemaObjectTestData extends TestDataSuplier {

  public SchemaObjectTestData(TestData testData) {
    super(testData);
  }

  public TableSchemaObject emptyTableSchemaObject() {
    return TableSchemaObject.from(testData.tableMetadata().empty(), new ObjectMapper());
  }

  public TableSchemaObject tableWithMapSetList() {
    return TableSchemaObject.from(
        testData.tableMetadata().tableAllDatatypesIndexed(), new ObjectMapper());
  }
}
