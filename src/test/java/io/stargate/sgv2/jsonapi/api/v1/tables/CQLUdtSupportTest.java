package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import org.junit.jupiter.api.*;

/**
 * Data API support for UDT created by CQL user
 *
 * <p>See the big === comment lines, please group tests in the same way.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
public class CQLUdtSupportTest extends AbstractTableIntegrationTestBase {
  private static final String TABLE_NAME = "table_udt";

  @BeforeAll
  public void setupCQL() {

    var createType =
            """
        CREATE TYPE IF NOT EXISTS "%s".address (
            street text,
            city text);
        """
            .formatted(keyspaceName);
    assertThat(executeCqlStatement(createType)).isTrue();

    var createTable =
            """
        CREATE TABLE IF NOT EXISTS "%s"."%s" (
            id text PRIMARY KEY,
            scalar_address address,
            frozen_address frozen<address>,
            frozen_set_address set<frozen<address>>,
            frozen_map_address map<text, frozen<address>>,
            frozen_list_address list<frozen<address>>);
        """
            .formatted(keyspaceName, TABLE_NAME);
    assertThat(executeCqlStatement(createTable)).isTrue();

    var insertUdt =
            """
        INSERT INTO "%s"."%s" (
            id,
            scalar_address,
            frozen_address,
            frozen_set_address,
            frozen_map_address,
            frozen_list_address
        ) VALUES (
            'cql_inserted',
            { street: 'scalar udt', city: 'monkey town' },
            { street: 'frozen udt', city: 'monkey town' },
            { { street: 'frozen set udt', city: 'monkey town' } },
            { 'key1': { street: 'frozen map udt', city: 'monkey town' } },
            [ { street: 'frozen list udt', city: 'monkey town' } ]
        );
        """
            .formatted(keyspaceName, TABLE_NAME);
    assertThat(executeCqlStatement(insertUdt)).isTrue();

    // list tables command to refresh the metadata in the API so it know about the new table
    assertNamespaceCommand(keyspaceName)
        .templated()
        .listTables(true)
        .wasSuccessful()
        .body("status.tables", notNullValue())
        .body("status.tables", hasSize(greaterThan(0)));
  }

  // ============================================================================================================
  // Read CQL written data
  // ============================================================================================================

  private void assertRead(String id, List<String> projection, String matchDoc) {
    assertTableCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName, TABLE_NAME)
        .templated()
        .findOne("id", id, projection)
        .wasSuccessful()
        .hasJSONField("data.document", matchDoc);
  }

  /** Supported to read from an existing non-frozen scalar UDT */
  @Test
  public void readCqlScalarAddress() {
    var matchDoc =
        """
         {
          "id": "cql_inserted",
          "scalar_address": {
               "street": "scalar udt",
               "city": "monkey town"
           }
        }""";
    assertRead("cql_inserted", List.of("id", "scalar_address"), matchDoc);
  }

  /** Supported to read from an existing frozen scalar UDT */
  @Test
  public void readCqlFrozenScalarAddress() {
    var matchDoc =
        """
         {
          "id": "cql_inserted",
          "frozen_address": {
               "street": "frozen udt",
               "city": "monkey town"
           }
        }""";
    assertRead("cql_inserted", List.of("id", "frozen_address"), matchDoc);
  }

  /** Supported to read from an existing frozen set of UDTs */
  @Test
  public void readCqlFrozenSetAddress() {
    var matchDoc =
        """
         {
          "id": "cql_inserted",
          "frozen_set_address": [
            {
              "street": "frozen set udt",
              "city": "monkey town"
            }
          ]
        }""";
    assertRead("cql_inserted", List.of("id", "frozen_set_address"), matchDoc);
  }

  /** Supported to read from an existing frozen list of UDTs */
  @Test
  public void readCqlFrozenListAddress() {
    var matchDoc =
        """
         {
          "id": "cql_inserted",
          "frozen_list_address": [
            {
              "street": "frozen list udt",
              "city": "monkey town"
            }
          ]
        }""";
    assertRead("cql_inserted", List.of("id", "frozen_list_address"), matchDoc);
  }

  /** Supported to read from an existing frozen map of UDTs */
  @Test
  public void readCqlFrozenMapAddress() {
    var matchDoc =
        """
         {
          "id": "cql_inserted",
          "frozen_map_address": {
            "key1": {
              "street": "frozen map udt",
              "city": "monkey town"
            }
          }
        }""";
    assertRead("cql_inserted", List.of("id", "frozen_map_address"), matchDoc);
  }

  // ============================================================================================================
  // Insert into CQL created columns
  // ============================================================================================================

  private void assertWrite(String doc) {
    assertTableCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName, TABLE_NAME)
        .templated()
        .insertOne(doc)
        .wasSuccessful();
  }

  /** Support inserting to a scalar UDT column */
  @Test
  public void insertScalarAddress() {
    var doc =
        """
        {
          "id": "insertScalarAddress",
          "scalar_address": {
               "street": "scalar udt",
               "city": "monkey town"
           }
        }""";
    assertWrite(doc);
  }

  // TODO: XXX: partial insert of a UDT
  /** Support inserting to a frozen UDT column */
  @Test
  public void insertFrozenScalarAddress() {
    var doc =
        """
        {
          "id": "insertFrozenScalarAddress",
          "frozen_address": {
               "street": "scalar udt",
               "city": "monkey town"
           }
        }""";
    assertWrite(doc);
  }

  /** Support inserting to a frozen set of UDTs column */
  @Test
  public void insertFrozenSetAddress() {
    var doc =
        """
      {
        "id": "insertFrozenSetAddress",
        "frozen_set_address": [
          {
            "street": "frozen set udt",
            "city": "monkey town"
          }
        ]
      }""";
    assertWrite(doc);
  }

  /** Support inserting to a frozen list of UDTs column */
  @Test
  public void insertFrozenListAddress() {
    var doc =
        """
      {
        "id": "insertFrozenListAddress",
        "frozen_list_address": [
          {
            "street": "frozen list udt",
            "city": "monkey town"
          }
        ]
      }""";
    assertWrite(doc);
  }

  /** Support inserting to a frozen map of UDTs column */
  @Test
  public void insertFrozenMapAddress() {
    var doc =
        """
      {
        "id": "insertFrozenMapAddress",
        "frozen_map_address": {
          "key1": {
            "street": "frozen map udt",
            "city": "monkey town"
          }
        }
      }""";
    assertWrite(doc);
  }

  // ============================================================================================================
  // Update into CQL created columns
  // ============================================================================================================

  private void assertUpdate(String id, String update, List<String> projection, String matchDoc) {
    var filter =
            """
        {
          "id": "%s"
        }"""
            .formatted(id);

    assertTableCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName, TABLE_NAME)
        .templated()
        .updateOne(filter, update)
        .wasSuccessful();

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .findOne("id", id, projection)
        .wasSuccessful()
        .hasJSONField("data.document", matchDoc);
  }

  private <T extends APIException> void assertUpdateFail(
      String id,
      String update,
      ErrorCode<T> errorCode,
      Class<T> errorClass,
      String... messageSnippet) {
    var filter =
            """
        {
          "id": "%s"
        }"""
            .formatted(id);

    assertTableCommand(AbstractKeyspaceIntegrationTestBase.keyspaceName, TABLE_NAME)
        .templated()
        .updateOne(filter, update)
        .hasSingleApiError(errorCode, errorClass, messageSnippet);
  }

  /** Support updating a single field in a scalar UDT column */
  @Test
  public void updateScalarAddressPartial() {
    var update =
        """
        {
          "$set": {
            "scalar_address.city": "updateScalarAddressPartial"
          }
        }""";

    var matchDoc =
        """
        {
          "id": "cql_inserted",
          "scalar_address": {
               "street": "scalar udt",
               "city": "updateScalarAddressPartial"
           }
        }""";

    assertUpdate("cql_inserted", update, List.of("id", "scalar_address"), matchDoc);
  }

  /** Support updating a full column for a scalar UDT column */
  @Test
  public void updateScalarAddressFull() {
    var update =
        """
        {
          "$set": {
            "scalar_address" : {
              "street": "updateScalarAddressFull",
              "city": "updateScalarAddressFull"
            }
          }
        }""";

    var matchDoc =
        """
        {
          "id": "cql_inserted",
          "scalar_address": {
               "street": "updateScalarAddressFull",
               "city": "updateScalarAddressFull"
           }
        }""";

    assertUpdate("cql_inserted", update, List.of("id", "scalar_address"), matchDoc);
  }

  /** Fail updating a single field in a frozen scalar UDT column */
  @Test
  public void updateFrozenScalarAddressPartial() {
    var update =
        """
        {
          "$set": {
            "frozen_address.city": "updateFrozenScalarAddressPartial"
          }
        }""";

    // TODO: XXX: AARON - I know this is the wrong error code, waiting for the correct one
    assertUpdateFail(
        "cql_inserted",
        update,
        DocumentException.Code.INVALID_COLUMN_VALUES,
        DocumentException.class,
        "Cannot update a field in a frozen UDT, use the full UDT to update the column");
  }

  /** Support updating a frozen UDT column fully */
  @Test
  public void updateFrozenScalarAddressFull() {
    var update =
        """
        {
          "$set": {
            "frozen_address" : {
              "street": "updateFrozenScalarAddressFull",
              "city": "updateFrozenScalarAddressFull"
            }
          }
        }""";

    var matchDoc =
        """
        {
          "id": "cql_inserted",
          "frozen_address": {
               "street": "updateFrozenScalarAddressFull",
               "city": "updateFrozenScalarAddressFull"
           }
        }""";

    assertUpdate("cql_inserted", update, List.of("id", "frozen_address"), matchDoc);
  }
}
