package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class DeleteOneTableIntegrationTest extends AbstractTableIntegrationTestBase {
  static final String TABLE_WITH_BLOB_IN_PARTITION_KEY =
      "table_blob_pk_" + System.currentTimeMillis();

  @BeforeAll
  public final void createTable() {
    final String createTableCommand =
            """
        {
            "name": "%s",
            "definition": {
                "columns": {
                    "p_ascii": "ascii",
                    "p_bigint": "bigint",
                    "p_blob": "blob",
                    "p_boolean": "boolean"
                },
                "primaryKey": {
                    "partitionBy": [
                        "p_ascii",
                        "p_blob"
                    ],
                    "partitionSort": {
                        "p_bigint": 1,
                        "p_boolean": -1
                    }
                }
            }
        }
        """
            .formatted(TABLE_WITH_BLOB_IN_PARTITION_KEY);

    assertNamespaceCommand(keyspaceName).postCreateTable(createTableCommand).wasSuccessful();
  }

  // [data-api#1578]
  @Test
  public void deleteWithBlobInPK() {
    final String base64Blob = "q83vASNFZ4k=";
    String docJSON =
            """
                {
                    "p_ascii": "abc",
                    "p_bigint": 10000,
                    "p_blob": {
                        "$binary": "%s"
                    },
                    "p_boolean": false
                }
        """
            .formatted(base64Blob);
    assertTableCommand(keyspaceName, TABLE_WITH_BLOB_IN_PARTITION_KEY)
        .templated()
        .insertOne(docJSON)
        .wasSuccessful();

    assertTableCommand(keyspaceName, TABLE_WITH_BLOB_IN_PARTITION_KEY)
        .templated()
        .deleteOne(
                """
                        {
                            "p_ascii": "abc",
                            "p_bigint": 10000,
                            "p_blob": {
                                "$binary": "%s"
                            },
                            "p_boolean": false
                       }
                       """
                .formatted(base64Blob))
        .wasSuccessful()
        .hasNoErrors();
  }
}
