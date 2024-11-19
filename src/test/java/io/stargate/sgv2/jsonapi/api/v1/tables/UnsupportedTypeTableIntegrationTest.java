package io.stargate.sgv2.jsonapi.api.v1.tables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class UnsupportedTypeTableIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final String TABLE_WITH_UNSUPPORTED_TYPE = "tableWithUnsupportedType";

  /**
   * Create a table with example dataTypes that Data API CURRENTLY does not support e.g.
   *
   * <p>CREATE TABLE '%s'.%s ( order_id text PRIMARY KEY, customer_name text, item_details
   * frozen<map<text, int>>, items frozen<list<text>> );
   */
  @BeforeAll
  public final void createDefaultTablesAndIndexes() {
    // Build the CREATE TABLE statement
    CreateTable createTable =
        SchemaBuilder.createTable(
                CqlIdentifierUtil.cqlIdentifierFromUserInput(keyspaceName),
                CqlIdentifierUtil.cqlIdentifierFromUserInput(TABLE_WITH_UNSUPPORTED_TYPE))
            .withPartitionKey("id", DataTypes.TEXT) // Primary key
            .withColumn("text", DataTypes.TEXT) // Regular column
            .withColumn("frozenSet", DataTypes.setOf(DataTypes.TEXT, true)) // Frozen set column
            .withColumn(
                "frozenMap",
                DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT, true)) // Frozen map column
            .withColumn("frozenList", DataTypes.listOf(DataTypes.TEXT, true)); // Frozen list column

    assertThat(executeCqlStatement(createTable.build())).isTrue();
  }

  /*
  Although countCommand currently does not support table, the command still should fail gracefully as 200,
  when it is executed against a table that has unsupported DataType
   */
  @Test
  public void countDocumentsAgainstTable() {
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_UNSUPPORTED_TYPE)
        .postCount()
        .hasSingleApiError(RequestException.Code.UNSUPPORTED_TABLE_COMMAND, RequestException.class);
  }
}
