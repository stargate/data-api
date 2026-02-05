package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;

public class SchemaObjectTestData extends TestDataSuplier {

  public SchemaObjectTestData(TestData testData) {
    super(testData);
  }

  public TableSchemaObject tableWithMapSetList() {
    return TableSchemaObject.from(
        testData.tenant().defaultTenant(),
        testData.tableMetadata().tableAllDatatypesIndexed(),
        new ObjectMapper());
  }
}
