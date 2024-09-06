package io.stargate.sgv2.jsonapi.api.v1.tables;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

/**
 * Basic testing to see what happens when "Tables" feature is disabled (other tests will have
 * feature enabled)
 */
@QuarkusIntegrationTest
@WithTestResource(
    value = TableFeatureDisabledIntegrationTest.TestResource.class,
    restrictToAnnotatedClass = true)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class TableFeatureDisabledIntegrationTest extends AbstractTableIntegrationTestBase {
  // Need to be able to enable/disable the TABLES feature
  public static class TestResource extends DseTestResource {
    public TestResource() {}

    @Override
    public String getFeatureFlagTables() {
      // return empty to leave feature "undefined" (disabled unless per-request header override)
      // ("false" would be "disabled" for all tests, regardless of headers)
      return "";
    }
  }

  // By default, table creation should fail
  @Order(1)
  @Test
  public void failCreateWithoutFeatureEnabled() {
    DataApiCommandSenders.assertNamespaceCommand(namespaceName)
        .postCreateTable(simpleTableDef("failCreateWithoutFeatureEnabled"))
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
