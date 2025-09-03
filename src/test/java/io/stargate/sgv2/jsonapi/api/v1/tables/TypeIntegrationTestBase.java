package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
class TypeIntegrationTestBase extends AbstractTableIntegrationTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(TypeIntegrationTestBase.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected String tableName(String typeName) {
    return "table_for_udt_" + typeName;
  }

  protected void createTypeAndTable(String typeName, String fields) {
    createTypeAndTable(typeName, fields, null);
  }

  /** Create type with the name and fields, make a table so we can verify the type was created */
  protected void createTypeAndTable(String typeName, String fields, String matchFields) {

    LOGGER.info(
        "Creating type typeName: {}, tableName: {}, fields: {}",
        typeName,
        tableName(typeName),
        fields);
    assertNamespaceCommand(keyspaceName).templated().createType(typeName, fields).wasSuccessful();

    // TODO: NO LIST TYPE, WE CANNOT VERIFY THE TYPE WAS CREATED, SO USING CREATE TABLE
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            tableName(typeName),
            Map.of("id", "text", "udt", Map.of("type", "userDefined", "udtName", typeName)),
            "id")
        .wasSuccessful();

    // TODO: NO LIST TYPE, WE CANNOT VERIFY THE TYPE WAS CREATED AS DEFINED, SO USING READ TABLE TO
    // GET SCHEMA
    assertType(typeName, matchFields == null ? fields : matchFields);
  }

  protected void dropTypeTable(String typeName) {
    LOGGER.info("Dropping type and table for typeName: {}", typeName);

    // Drop the table first
    assertNamespaceCommand(keyspaceName)
        .templated()
        .dropTable(tableName(typeName), false)
        .wasSuccessful();
  }

  /** Reads on the table to check the schema of the type */
  protected void assertType(String typeName, String matchFields) {
    LOGGER.info("Assert Type for typeName: {}, matchFields: {}", typeName, matchFields);

    // TODO: NO LIST TYPE, WE CANNOT VERIFY THE TYPE WAS MODIFIED, SO USING READ TABLE TO GET SCHEMA
    assertTableCommand(keyspaceName, tableName(typeName))
        .templated()
        .findOne(Map.of(), List.of())
        .wasSuccessful()
        .hasJSONField("status.projectionSchema.udt.definition.fields", matchFields);
  }
}
