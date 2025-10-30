package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

/**
 * Basic testing to see what happens when "Tables" feature is disabled via explicit configuration
 * (other tests will have feature enabled by default or via config)
 */
@QuarkusIntegrationTest
@QuarkusTestResource(
    value = TableFeatureDisabledIntegrationTest.TestResource.class,
    restrictToAnnotatedClass = true)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class TableFeatureDisabledIntegrationTest extends AbstractTableIntegrationTestBase {
  // Need to be able to enable/disable the TABLES feature
  public static class TestResource extends DseTestResource {
    public TestResource() {}

    @Override
    public String getFeatureFlagTables() {
      // Explicitly disable the feature for these tests
      // When config is explicitly set, it takes precedence over per-request headers
      return "false";
    }
  }

  private static final String TABLE_TO_CREATE = "table_with_table_feature";

  // With explicit config disable, table creation should fail
  @Order(1)
  @Test
  public void failCreateWithExplicitlyDisabledFeature() {
    assertNamespaceCommand(keyspaceName)
        .postCreateTable(simpleTableDef(TABLE_TO_CREATE))
        .hasSingleApiError(ErrorCodeV1.TABLE_FEATURE_NOT_ENABLED);
  }

  // With explicit config disable, header cannot override (config takes precedence)
  @Order(2)
  @Test
  public void failCreateEvenWithHeaderWhenExplicitlyDisabled() {
    assertNamespaceCommand(keyspaceName)
        .header(ApiFeature.TABLES.httpHeaderName(), "true")
        .postCreateTable(simpleTableDef(TABLE_TO_CREATE))
        .hasSingleApiError(ErrorCodeV1.TABLE_FEATURE_NOT_ENABLED);
  }

  private static String simpleTableDef(String tableName) {
    return
        """
               {
                   "name": "%s",
                   "definition": {
                       "columns": {
                           "id": { "type": "text" },
                           "name": { "type": "text" }
                       },
                       "primaryKey": "id"
                   }
               }
      """
        .formatted(tableName);
  }
}
