package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data API support for UDT created by CQL user
 *
 * <p>There are a lot of categories of tests, broken into nested classes:
 *
 * <ul>
 *   <li>{@link ReadCqlData}
 *   <li>{@link InsertCqlColumns}
 *   <li>{@link UpdateCqlDataSetOperation}
 *   <li>{@link UpdateCqlDataUnsetOperation}
 *   <li>{@link UpdateCqlDataPushOperation}
 *   <li>{@link UpdateCqlDataPushEachOperation}
 *   <li>{@link UpdateCqlDataPullAllOperation}
 * </ul>
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
public class CQLUdtSupportIntegrationTest extends AbstractTableIntegrationTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(CQLUdtSupportIntegrationTest.class);

  private static final String TABLE_NAME = "table_udt";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static String INSERT_CQL =
      """
              INSERT INTO "%s"."%s" (
                  id,
                  scalar_address,
                  frozen_address,
                  set_frozen_address,
                  map_frozen_address,
                  list_frozen_address,
                  all_frozen_set_address,
                  all_frozen_map_address,
                  all_frozen_list_address
                ) VALUES (
                    '%s',
                    { street: 'scalar udt', city: 'Original City' },
                    { street: 'frozen udt', city: 'Original City' },
                    { { street: 'set frozen udt', city: 'Original City' } },
                    { 'key1': { street: 'map frozen udt', city: 'Original City' } },
                    [ { street: 'list frozen udt', city: 'Original City' } ],
                    { { street: 'all frozen set udt', city: 'Original City' } },
                    { 'key1': { street: 'all frozen map udt', city: 'Original City' } },
                    [ { street: 'all frozen list udt', city: 'Original City' } ]
                );
              """;

  @BeforeAll
  public void setupCQL() {

    var dropType =
            """
        DROP TYPE IF EXISTS "%s".address;
        """
            .formatted(keyspaceName);
    assertThat(executeCqlStatement(dropType)).isTrue();

    var createType =
            """
        CREATE TYPE IF NOT EXISTS "%s".address (
            street text,
            city text);
        """
            .formatted(keyspaceName);
    assertThat(executeCqlStatement(createType)).isTrue();

    var dropTable =
            """
        DROP TABLE IF EXISTS "%s"."%s";
        """
            .formatted(keyspaceName, TABLE_NAME);
    assertThat(executeCqlStatement(dropTable)).isTrue();

    var createTable =
            """
        CREATE TABLE IF NOT EXISTS "%s"."%s" (
            id text PRIMARY KEY,
            scalar_address address,
            frozen_address frozen<address>,
            set_frozen_address set<frozen<address>>,
            map_frozen_address map<text, frozen<address>>,
            list_frozen_address list<frozen<address>>,
            all_frozen_set_address frozen<set<frozen<address>>>,
            all_frozen_map_address frozen<map<text, frozen<address>>>,
            all_frozen_list_address frozen<list<frozen<address>>>);
        """
            .formatted(keyspaceName, TABLE_NAME);
    assertThat(executeCqlStatement(createTable)).isTrue();

    assertThat(executeCqlStatement(INSERT_CQL.formatted(keyspaceName, TABLE_NAME, "cql_inserted")))
        .isTrue();

    // list tables command to refresh the metadata in the API so it know about the new table
    assertNamespaceCommand(keyspaceName)
        .templated()
        .listTables(true)
        .wasSuccessful()
        .body("status.tables", notNullValue())
        .body("status.tables", hasSize(greaterThan(0)));
  }

  private static String prettyJson(String json) {
    try {
      return OBJECT_MAPPER.readTree(json).toPrettyString();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertRead(String id, List<String> projection, String matchDoc) {
    LOGGER.info("Asserting Read for id: {} with projection: {}", id, projection);

    var response =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .findOne("id", id, projection)
            .wasSuccessful()
            .hasSingleDocument(matchDoc)
            .response()
            .extract()
            .body()
            .asString();

    JsonNode node = null;
    try {
      node = OBJECT_MAPPER.readTree(response);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    LOGGER.info("Read for id: {}: response: {}", id, node.toPrettyString());
  }

  private void assertWrite(String doc) {
    LOGGER.info("Asserting Insert for doc: {}", doc);

    assertTableCommand(keyspaceName, TABLE_NAME).templated().insertOne(doc).wasSuccessful();

    JsonNode jsonNode;
    try {
      jsonNode = OBJECT_MAPPER.readTree(doc);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    var id = jsonNode.get("id").asText();
    var fields = new ArrayList<String>();
    jsonNode.fieldNames().forEachRemaining(fields::add);

    // read back the doc to verify it was written correctly
    var response =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .findOne("id", id, fields)
            .wasSuccessful()
            .hasSingleDocument(doc)
            .response()
            .extract()
            .body()
            .asString();

    JsonNode node = null;
    try {
      node = OBJECT_MAPPER.readTree(response);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    LOGGER.info("Read after Insert for id: {}: response: {}", id, node.toPrettyString());
  }

  private void assertUpdate(String id, String update, List<String> projection, String matchDoc) {
    assertUpdate(id, update, projection, matchDoc, true);
  }

  private <T extends APIException> void assertUpdate(
      String id, String update, List<String> projection, String matchDoc, boolean insertBefore) {
    assertUpdate(id, update, projection, matchDoc, insertBefore, null, null, null);
  }

  private <T extends APIException> void assertUpdate(
      String id,
      String update,
      List<String> projection,
      String matchDoc,
      boolean insertBefore,
      ErrorCode<T> errorCode,
      Class<T> errorClass,
      String errorMatch) {
    var filter =
            """
          {
            "id": "%s"
          }"""
            .formatted(id);

    LOGGER.info("Asserting Update for id: {}: update: {}", id, update);

    if (insertBefore) {
      LOGGER.info("Running pre insert for id: {}", id);
      assertThat(executeCqlStatement(INSERT_CQL.formatted(keyspaceName, TABLE_NAME, id))).isTrue();
    }

    LOGGER.info("Running read BEFORE update for id: {}", id);
    var responseBefore =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .findOne("id", id, projection)
            .wasSuccessful()
            .hasSingleDocument()
            .response()
            .extract()
            .body()
            .asString();
    LOGGER.info("Read BEFORE Update for id: {}: response: {}", id, prettyJson(responseBefore));

    LOGGER.info("Running update for id: {}", id);
    var matcher =
        assertTableCommand(keyspaceName, TABLE_NAME).templated().updateOne(filter, update);
    if (errorClass == null) {
      matcher.wasSuccessful();
    } else {
      matcher.hasSingleApiError(errorCode, errorClass, errorMatch);
    }

    LOGGER.info("Running read AFTER update with doc match for id: {}", id);
    var responseAfter =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .findOne("id", id, projection)
            .wasSuccessful()
            .hasSingleDocument(matchDoc)
            .response()
            .extract()
            .body()
            .asString();
    LOGGER.info("Read AFTER  Update for id: {}: response: {}", id, prettyJson(responseAfter));
  }

  private <T extends APIException> void assertUpdateFail(
      String id,
      String update,
      ErrorCode<T> errorCode,
      Class<T> errorClass,
      String... messageSnippet) {
    assertUpdateFail(id, update, true, errorCode, errorClass, messageSnippet);
  }

  private <T extends APIException> void assertUpdateFail(
      String id,
      String update,
      boolean insertBefore,
      ErrorCode<T> errorCode,
      Class<T> errorClass,
      String... messageSnippet) {

    LOGGER.info("Asserting Update Fails for id: {}: update: {}", id, update);

    var filter =
            """
    {
      "id": "%s"
    }"""
            .formatted(id);
    if (insertBefore) {
      LOGGER.info("Running insert before Update Fails for id: {}:", id);
      assertThat(executeCqlStatement(INSERT_CQL.formatted(keyspaceName, TABLE_NAME, id))).isTrue();
    }

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .updateOne(filter, update)
        .hasSingleApiError(errorCode, errorClass, messageSnippet);
  }

  /**
   * ============================================================================================================
   * Read CQL written data
   * ============================================================================================================
   */
  @Nested
  class ReadCqlData {

    /** Supported to read from an existing non-frozen scalar UDT */
    @Test
    public void readCqlScalarAddress() {
      var matchDoc =
          """
         {
          "id": "cql_inserted",
          "scalar_address": {
               "street": "scalar udt",
               "city": "Original City"
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
               "city": "Original City"
           }
        }""";
      assertRead("cql_inserted", List.of("id", "frozen_address"), matchDoc);
    }

    /** Supported to read from an existing frozen set of UDTs */
    @Test
    public void readCqlSetFrozenAddress() {
      var matchDoc =
          """
         {
          "id": "cql_inserted",
          "set_frozen_address": [
            {
              "street": "set frozen udt",
              "city": "Original City"
            }
          ]
        }""";
      assertRead("cql_inserted", List.of("id", "set_frozen_address"), matchDoc);
    }

    /** Supported to read from an existing frozen list of UDTs */
    @Test
    public void readCqlListFrozenAddress() {
      var matchDoc =
          """
         {
          "id": "cql_inserted",
          "list_frozen_address": [
            {
              "street": "list frozen udt",
              "city": "Original City"
            }
          ]
        }""";
      assertRead("cql_inserted", List.of("id", "list_frozen_address"), matchDoc);
    }

    /** Supported to read from an existing frozen map of UDTs */
    @Test
    public void readCqlMapFrozenAddress() {
      var matchDoc =
          """
         {
          "id": "cql_inserted",
          "map_frozen_address": {
            "key1": {
              "street": "map frozen udt",
              "city": "Original City"
            }
          }
        }""";
      assertRead("cql_inserted", List.of("id", "map_frozen_address"), matchDoc);
    }

    /** Supported to read from an existing all_frozen_set_address column */
    @Test
    public void readAllFrozenSetAddress() {
      var matchDoc =
          """
           {
            "id": "cql_inserted",
            "all_frozen_set_address": [
              {
                "street": "all frozen set udt",
                "city": "Original City"
              }
            ]
          }""";
      assertRead("cql_inserted", List.of("id", "all_frozen_set_address"), matchDoc);
    }

    /** Supported to read from an existing all_frozen_map_address column */
    @Test
    public void readAllFrozenMapAddress() {
      var matchDoc =
          """
           {
            "id": "cql_inserted",
            "all_frozen_map_address": {
              "key1": {
                "street": "all frozen map udt",
                "city": "Original City"
              }
            }
          }""";
      assertRead("cql_inserted", List.of("id", "all_frozen_map_address"), matchDoc);
    }

    /** Supported to read from an existing all_frozen_list_address column */
    @Test
    public void readAllFrozenListAddress() {
      var matchDoc =
          """
           {
            "id": "cql_inserted",
            "all_frozen_list_address": [
              {
                "street": "all frozen list udt",
                "city": "Original City"
              }
            ]
          }""";
      assertRead("cql_inserted", List.of("id", "all_frozen_list_address"), matchDoc);
    }
  }

  /**
   * ============================================================================================================
   * Insert into CQL created UDT columns
   * ============================================================================================================
   */
  @Nested
  class InsertCqlColumns {
    /** Support inserting to a scalar UDT column */
    @Test
    public void insertScalarAddress() {
      var doc =
          """
              {
                "id": "insertScalarAddress",
                "scalar_address": {
                     "street": "scalar udt",
                     "city": "Original City"
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
                     "city": "Original City"
                 }
              }""";
      assertWrite(doc);
    }

    /** Support inserting to a set_frozen_address column */
    @Test
    public void insertSetFrozenAddress() {
      var doc =
          """
              {
                "id": "insertSetFrozenAddress",
                "set_frozen_address": [
                  {
                    "street": "set frozen udt",
                    "city": "Original City"
                  }
                ]
              }""";
      assertWrite(doc);
    }

    /** Support inserting to a list_frozen_address column */
    @Test
    public void insertListFrozenAddress() {
      var doc =
          """
              {
                "id": "insertListFrozenAddress",
                "list_frozen_address": [
                  {
                    "street": "list frozen udt",
                    "city": "Original City"
                  }
                ]
              }""";
      assertWrite(doc);
    }

    /** Support inserting to a map_frozen_address column */
    @Test
    public void insertMapFrozenAddress() {
      var doc =
          """
              {
                "id": "insertMapFrozenAddress",
                "map_frozen_address": {
                  "key1": {
                    "street": "map frozen udt",
                    "city": "Original City"
                  }
                }
              }""";
      assertWrite(doc);
    }

    /** Support inserting to an all_frozen_set_address column */
    @Test
    public void insertAllFrozenSetAddress() {
      var doc =
          """
              {
                "id": "insertAllFrozenSetAddress",
                "all_frozen_set_address": [
                  {
                    "street": "all frozen set udt",
                    "city": "Original City"
                  }
                ]
              }""";
      assertWrite(doc);
    }

    /** Support inserting to an all_frozen_map_address column */
    @Test
    public void insertAllFrozenMapAddress() {
      var doc =
          """
              {
                "id": "insertAllFrozenMapAddress",
                "all_frozen_map_address": {
                  "key1": {
                    "street": "all frozen map udt",
                    "city": "Original City"
                  }
                }
              }""";
      assertWrite(doc);
    }

    /** Support inserting to an all_frozen_list_address column */
    @Test
    public void insertAllFrozenListAddress() {
      var doc =
          """
              {
                "id": "insertAllFrozenListAddress",
                "all_frozen_list_address": [
                  {
                    "street": "all frozen list udt",
                    "city": "Original City"
                  }
                ]
              }""";
      assertWrite(doc);
    }
  }

  /**
   * ============================================================================================================
   * Update CQL create data
   * ============================================================================================================
   */
  @Nested
  class UpdateCqlDataSetOperation {

    /** Support updating a single field in a scalar UDT column */
    @Test
    public void updateSetOpScalarAddressPartial() {
      var update =
          """
              {
                "$set": {
                  "scalar_address.city": "updateSetOpScalarAddressPartial"
                }
              }""";

      var matchDoc =
          """
              {
                "id": "updateSetOpScalarAddressPartial",
                "scalar_address": {
                     "street": "scalar udt",
                     "city": "Original City"
                 }
              }""";

      // TODO: This is failing because we do not yet support partial updates of a UDT, it should be
      // possible to do
      assertUpdate(
          "updateSetOpScalarAddressPartial",
          update,
          List.of("id", "scalar_address"),
          matchDoc,
          true,
          UpdateException.Code.UNKNOWN_TABLE_COLUMNS,
          UpdateException.class,
          "The update included the following unknown columns: \"scalar_address.city\".");
    }

    /** Support updating a full column for a scalar UDT column */
    @Test
    public void updateSetOpScalarAddressFull() {
      var update =
          """
              {
                "$set": {
                  "scalar_address" : {
                    "street": "updateSetOpScalarAddressFull",
                    "city": "updateSetOpScalarAddressFull"
                  }
                }
              }""";

      var matchDoc =
          """
              {
                "id": "updateSetOpScalarAddressFull",
                "scalar_address": {
                     "street": "updateSetOpScalarAddressFull",
                     "city": "updateSetOpScalarAddressFull"
                 }
              }""";

      assertUpdate(
          "updateSetOpScalarAddressFull", update, List.of("id", "scalar_address"), matchDoc);
    }

    /** Fail updating a single field in a frozen scalar UDT column */
    @Test
    public void updateSetOpFrozenScalarAddressPartial() {
      var update =
          """
              {
                "$set": {
                  "frozen_address.city": "updateSetOpFrozenScalarAddressPartial"
                }
              }""";

      // TODO: This fails with unknown because we not yet support partial updates of a UDT, whe we
      // have that it
      // should fail with something better because we cannot update a any field in a frozen UDT
      assertUpdateFail(
          "updateSetOpFrozenScalarAddressPartial",
          update,
          UpdateException.Code.UNKNOWN_TABLE_COLUMNS,
          UpdateException.class,
          "The update included the following unknown columns: \"frozen_address.city\".");
    }

    /** Support updating a frozen UDT column fully */
    @Test
    public void updateSetOpFrozenScalarAddressFull() {
      var update =
          """
              {
                "$set": {
                  "frozen_address" : {
                    "street": "updateSetOpFrozenScalarAddressFull",
                    "city": "updateSetOpFrozenScalarAddressFull"
                  }
                }
              }""";

      var matchDoc =
          """
              {
                "id": "updateSetOpFrozenScalarAddressFull",
                "frozen_address": {
                     "street": "updateSetOpFrozenScalarAddressFull",
                     "city": "updateSetOpFrozenScalarAddressFull"
                 }
              }""";

      assertUpdate(
          "updateSetOpFrozenScalarAddressFull", update, List.of("id", "frozen_address"), matchDoc);
    }

    /** Support updating a set_frozen_address column fully */
    @Test
    public void updateSetOpSetFrozenAddressFull() {
      var update =
          """
              {
                "$set": {
                  "set_frozen_address" : [
                    {
                      "street": "updateSetOpSetFrozenAddressFull",
                      "city": "updateSetOpSetFrozenAddressFull"
                    }
                  ]
                }
              }""";

      var matchDoc =
          """
              {
                "id": "updateSetOpSetFrozenAddressFull",
                "set_frozen_address": [
                  {
                    "street": "updateSetOpSetFrozenAddressFull",
                    "city": "updateSetOpSetFrozenAddressFull"
                  }
                ]
              }""";

      assertUpdate(
          "updateSetOpSetFrozenAddressFull", update, List.of("id", "set_frozen_address"), matchDoc);
    }

    /** Support updating a list_frozen_address column fully */
    @Test
    public void updateSetOpListFrozenAddressFull() {
      var update =
          """
              {
                "$set": {
                  "list_frozen_address" : [
                    {
                      "street": "updateSetOpListFrozenAddressFull",
                      "city": "updateSetOpListFrozenAddressFull"
                    }
                  ]
                }
              }""";

      var matchDoc =
          """
              {
                "id": "updateSetOpListFrozenAddressFull",
                "list_frozen_address": [
                  {
                    "street": "updateSetOpListFrozenAddressFull",
                    "city": "updateSetOpListFrozenAddressFull"
                  }
                ]
              }""";

      assertUpdate(
          "updateSetOpListFrozenAddressFull",
          update,
          List.of("id", "list_frozen_address"),
          matchDoc);
    }

    /** Support updating a map_frozen_address column fully */
    @Test
    public void updateSetOpMapFrozenAddressFull() {
      var update =
          """
              {
                "$set": {
                  "map_frozen_address" : {
                    "key1": {
                      "street": "updateSetOpMapFrozenAddressFull",
                      "city": "updateSetOpMapFrozenAddressFull"
                    }
                  }
                }
              }""";

      var matchDoc =
          """
              {
                "id": "updateSetOpMapFrozenAddressFull",
                "map_frozen_address": {
                  "key1": {
                    "street": "updateSetOpMapFrozenAddressFull",
                    "city": "updateSetOpMapFrozenAddressFull"
                  }
                }
              }""";

      assertUpdate(
          "updateSetOpMapFrozenAddressFull", update, List.of("id", "map_frozen_address"), matchDoc);
    }

    /** Support updating all_frozen_set_address column fully */
    @Test
    public void updateSetOpAllFrozenSetAddressFull() {
      var update =
          """
              {
                "$set": {
                  "all_frozen_set_address" : [
                    {
                      "street": "updateSetOpAllFrozenSetAddressFull",
                      "city": "updateSetOpAllFrozenSetAddressFull"
                    }
                  ]
                }
              }""";

      var matchDoc =
          """
              {
                "id": "updateSetOpAllFrozenSetAddressFull",
                "all_frozen_set_address": [
                  {
                    "street": "updateSetOpAllFrozenSetAddressFull",
                    "city": "updateSetOpAllFrozenSetAddressFull"
                  }
                ]
              }""";

      assertUpdate(
          "updateSetOpAllFrozenSetAddressFull",
          update,
          List.of("id", "all_frozen_set_address"),
          matchDoc);
    }

    /** Support updating all_frozen_map_address column fully */
    @Test
    public void updateSetOpAllFrozenMapAddressFull() {
      var update =
          """
              {
                "$set": {
                  "all_frozen_map_address" : {
                    "key1": {
                      "street": "updateSetOpAllFrozenMapAddressFull",
                      "city": "updateSetOpAllFrozenMapAddressFull"
                    }
                  }
                }
              }""";

      var matchDoc =
          """
              {
                "id": "updateSetOpAllFrozenMapAddressFull",
                "all_frozen_map_address": {
                  "key1": {
                    "street": "updateSetOpAllFrozenMapAddressFull",
                    "city": "updateSetOpAllFrozenMapAddressFull"
                  }
                }
              }""";

      assertUpdate(
          "updateSetOpAllFrozenMapAddressFull",
          update,
          List.of("id", "all_frozen_map_address"),
          matchDoc);
    }

    /** Support updating all_frozen_list_address column fully */
    @Test
    public void updateSetOpAllFrozenListAddressFull() {
      var update =
          """
              {
                "$set": {
                  "all_frozen_list_address" : [
                    {
                      "street": "updateSetOpAllFrozenListAddressFull",
                      "city": "updateSetOpAllFrozenListAddressFull"
                    }
                  ]
                }
              }""";

      var matchDoc =
          """
              {
                "id": "updateSetOpAllFrozenListAddressFull",
                "all_frozen_list_address": [
                  {
                    "street": "updateSetOpAllFrozenListAddressFull",
                    "city": "updateSetOpAllFrozenListAddressFull"
                  }
                ]
              }""";

      assertUpdate(
          "updateSetOpAllFrozenListAddressFull",
          update,
          List.of("id", "all_frozen_list_address"),
          matchDoc);
    }
  }

  @Nested
  class UpdateCqlDataPushOperation {
    /** Support pushing into set_frozen_address */
    @Test
    public void updatePushOpSetFrozenAddress() {
      var update =
          """
          {
            "$push": {
              "set_frozen_address": {
                "street": "updatePushOpSetFrozenAddress",
                "city": "updatePushOpSetFrozenAddress"
              }
            }
          }""";

      var matchDoc =
          """
          {
            "id": "updatePushOpSetFrozenAddress",
            "set_frozen_address": [
              {
                "street": "set frozen udt",
                "city": "Original City"
              },
              {
                "street": "updatePushOpSetFrozenAddress",
                "city": "updatePushOpSetFrozenAddress"
              }
            ]
          }""";

      assertUpdate(
          "updatePushOpSetFrozenAddress", update, List.of("id", "set_frozen_address"), matchDoc);
    }

    /** Support pushing into list_frozen_address */
    @Test
    public void updatePushOpListFrozenAddress() {
      var update =
          """
          {
            "$push": {
              "list_frozen_address": {
                "street": "updatePushOpListFrozenAddress",
                "city": "updatePushOpListFrozenAddress"
              }
            }
          }""";

      var matchDoc =
          """
          {
            "id": "updatePushOpListFrozenAddress",
            "list_frozen_address": [
              {
                "street": "list frozen udt",
                "city": "Original City"
              },
              {
                "street": "updatePushOpListFrozenAddress",
                "city": "updatePushOpListFrozenAddress"
              }
            ]
          }""";

      assertUpdate(
          "updatePushOpListFrozenAddress", update, List.of("id", "list_frozen_address"), matchDoc);
    }

    /** Support pushing into map_frozen_address */
    @Test
    public void updatePushOpMapFrozenAddress() {
      var update =
          """
          {
            "$push": {
              "map_frozen_address": {
                "key2": {
                  "street": "updatePushOpMapFrozenAddress",
                  "city": "updatePushOpMapFrozenAddress"
                }
              }
            }
          }""";

      var matchDoc =
          """
          {
            "id": "updatePushOpMapFrozenAddress",
            "map_frozen_address": {
              "key1": {
                "street": "map frozen udt",
                "city": "Original City"
              },
              "key2": {
                "street": "updatePushOpMapFrozenAddress",
                "city": "updatePushOpMapFrozenAddress"
              }
            }
          }""";

      assertUpdate(
          "updatePushOpMapFrozenAddress", update, List.of("id", "map_frozen_address"), matchDoc);
    }

    /** Support pushing into all_frozen_set_address */
    @Test
    public void updatePushOpAllFrozenSetAddress() {
      var update =
          """
          {
            "$push": {
              "all_frozen_set_address": {
                "street": "updatePushOpAllFrozenSetAddress",
                "city": "updatePushOpAllFrozenSetAddress"
              }
            }
          }""";

      var matchDoc =
          """
          {
            "id": "updatePushOpAllFrozenSetAddress",
            "all_frozen_set_address": [
              {
                "street": "all frozen set udt",
                "city": "Original City"
              },
              {
                "street": "updatePushOpAllFrozenSetAddress",
                "city": "updatePushOpAllFrozenSetAddress"
              }
            ]
          }""";

      assertUpdateFail(
          "updatePushOpAllFrozenSetAddress",
          update,
          UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR,
          UpdateException.class,
          "The command used the update operator: $push.",
          "The operation was not supported by the columns: all_frozen_set_address(set).");
    }

    /** Support pushing into all_frozen_list_address */
    @Test
    public void updatePushOpAllFrozenListAddress() {
      var update =
          """
          {
            "$push": {
              "all_frozen_list_address": {
                "street": "updatePushOpAllFrozenListAddress",
                "city": "updatePushOpAllFrozenListAddress"
              }
            }
          }""";

      var matchDoc =
          """
          {
            "id": "updatePushOpAllFrozenListAddress",
            "all_frozen_list_address": [
              {
                "street": "all frozen list udt",
                "city": "Original City"
              },
              {
                "street": "updatePushOpAllFrozenListAddress",
                "city": "updatePushOpAllFrozenListAddress"
              }
            ]
          }""";

      assertUpdateFail(
          "updatePushOpAllFrozenListAddress",
          update,
          UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR,
          UpdateException.class,
          "The command used the update operator: $push.",
          "The operation was not supported by the columns: all_frozen_list_address(list).");
    }

    /** Support pushing into all_frozen_map_address */
    @Test
    public void updatePushOpAllFrozenMapAddress() {
      var update =
          """
          {
            "$push": {
              "all_frozen_map_address": {
                "key2": {
                  "street": "updatePushOpAllFrozenMapAddress",
                  "city": "updatePushOpAllFrozenMapAddress"
                }
              }
            }
          }""";

      var matchDoc =
          """
          {
            "id": "updatePushOpAllFrozenMapAddress",
            "all_frozen_map_address": {
              "key1": {
                "street": "all frozen map udt",
                "city": "Original City"
              },
              "key2": {
                "street": "updatePushOpAllFrozenMapAddress",
                "city": "updatePushOpAllFrozenMapAddress"
              }
            }
          }""";

      assertUpdateFail(
          "updatePushOpAllFrozenMapAddress",
          update,
          UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR,
          UpdateException.class,
          "The command used the update operator: $push.",
          "The operation was not supported by the columns: all_frozen_map_address(map).");
    }
  }

  @Nested
  class UpdateCqlDataPushEachOperation {
    /** Support updating a list_frozen_address column fully */
    @Test
    public void updatePushEachOpListFrozenAddress() {
      var update =
          """
          {
            "$push": {
              "list_frozen_address": {
                "$each": [
                  {
                    "street": "updatePushEachOpListFrozenAddress",
                    "city": "updatePushEachOpListFrozenAddress"
                  }
                ]
              }
            }
          }
          """;

      var matchDoc =
          """
              {
                "id": "updatePushEachOpListFrozenAddress",
                 "list_frozen_address": [
                    {
                      "street": "list frozen udt",
                      "city": "Original City"
                    },
                    {
                      "street": "updatePushEachOpListFrozenAddress",
                      "city": "updatePushEachOpListFrozenAddress"
                    }
                  ]
                }
              }""";

      assertUpdate(
          "updatePushEachOpListFrozenAddress",
          update,
          List.of("id", "list_frozen_address"),
          matchDoc);
    }

    /** Support pushing with $each into set_frozen_address */
    @Test
    public void updatePushEachOpSetFrozenAddress() {
      var update =
          """
          {
            "$push": {
              "set_frozen_address": {
                "$each": [
                  {
                    "street": "updatePushEachOpSetFrozenAddress",
                    "city": "updatePushEachOpSetFrozenAddress"
                  }
                ]
              }
            }
          }
          """;

      var matchDoc =
          """
              {
                "id": "updatePushEachOpSetFrozenAddress",
                "set_frozen_address": [
                  {
                    "street": "set frozen udt",
                    "city": "Original City"
                  },
                  {
                    "street": "updatePushEachOpSetFrozenAddress",
                    "city": "updatePushEachOpSetFrozenAddress"
                  }
                ]
              }""";

      assertUpdate(
          "updatePushEachOpSetFrozenAddress",
          update,
          List.of("id", "set_frozen_address"),
          matchDoc);
    }

    /** Support pushing with $each into map_frozen_address */
    @Test
    public void updatePushEachOpMapFrozenAddress() {
      var update =
          """
          {
            "$push": {
              "map_frozen_address": {
                "$each": [
                  {
                    "key2": {
                      "street": "updatePushEachOpMapFrozenAddress",
                      "city": "updatePushEachOpMapFrozenAddress"
                    }
                  }
                ]
              }
            }
          }
          """;

      var matchDoc =
          """
              {
                "id": "updatePushEachOpMapFrozenAddress",
                "map_frozen_address": {
                  "key1": {
                    "street": "map frozen udt",
                    "city": "Original City"
                  },
                  "key2": {
                    "street": "updatePushEachOpMapFrozenAddress",
                    "city": "updatePushEachOpMapFrozenAddress"
                  }
                }
              }""";

      assertUpdate(
          "updatePushEachOpMapFrozenAddress",
          update,
          List.of("id", "map_frozen_address"),
          matchDoc);
    }

    /** Support pushing with $each into all_frozen_set_address */
    @Test
    public void updatePushEachOpAllFrozenSetAddress() {
      var update =
          """
          {
            "$push": {
              "all_frozen_set_address": {
                "$each": [
                  {
                    "street": "updatePushEachOpAllFrozenSetAddress",
                    "city": "updatePushEachOpAllFrozenSetAddress"
                  }
                ]
              }
            }
          }
          """;

      var matchDoc =
          """
              {
                "id": "updatePushEachOpAllFrozenSetAddress",
                "all_frozen_set_address": [
                  {
                    "street": "all frozen set udt",
                    "city": "Original City"
                  },
                  {
                    "street": "updatePushEachOpAllFrozenSetAddress",
                    "city": "updatePushEachOpAllFrozenSetAddress"
                  }
                ]
              }""";

      assertUpdateFail(
          "updatePushEachOpAllFrozenSetAddress",
          update,
          UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR,
          UpdateException.class,
          "The command used the update operator: $push.",
          "The operation was not supported by the columns: all_frozen_set_address(set).");
    }

    /** Support pushing with $each into all_frozen_map_address */
    @Test
    public void updatePushEachOpAllFrozenMapAddress() {
      var update =
          """
          {
            "$push": {
              "all_frozen_map_address": {
                "$each": [
                  {
                    "key2": {
                      "street": "updatePushEachOpAllFrozenMapAddress",
                      "city": "updatePushEachOpAllFrozenMapAddress"
                    }
                  }
                ]
              }
            }
          }
          """;

      var matchDoc =
          """
              {
                "id": "updatePushEachOpAllFrozenMapAddress",
                "all_frozen_map_address": {
                  "key1": {
                    "street": "all frozen map udt",
                    "city": "Original City"
                  },
                  "key2": {
                    "street": "updatePushEachOpAllFrozenMapAddress",
                    "city": "updatePushEachOpAllFrozenMapAddress"
                  }
                }
              }""";

      assertUpdateFail(
          "updatePushEachOpAllFrozenMapAddress",
          update,
          UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR,
          UpdateException.class,
          "The command used the update operator: $push.",
          "The operation was not supported by the columns: all_frozen_map_address(map).");
    }

    /** Support pushing with $each into all_frozen_list_address */
    @Test
    public void updatePushEachOpAllFrozenListAddress() {
      var update =
          """
          {
            "$push": {
              "all_frozen_list_address": {
                "$each": [
                  {
                    "street": "updatePushEachOpAllFrozenListAddress",
                    "city": "updatePushEachOpAllFrozenListAddress"
                  }
                ]
              }
            }
          }
          """;

      var matchDoc =
          """
              {
                "id": "updatePushEachOpAllFrozenListAddress",
                "all_frozen_list_address": [
                  {
                    "street": "all frozen list udt",
                    "city": "Original City"
                  },
                  {
                    "street": "updatePushEachOpAllFrozenListAddress",
                    "city": "updatePushEachOpAllFrozenListAddress"
                  }
                ]
              }""";

      assertUpdateFail(
          "updatePushEachOpAllFrozenListAddress",
          update,
          UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR,
          UpdateException.class,
          "The command used the update operator: $push.",
          "The operation was not supported by the columns: all_frozen_list_address(list).");
    }
  }

  @Nested
  class UpdateCqlDataUnsetOperation {

    @Test
    public void updateUnsetOpScalarAddressPartial() {
      var update =
          """
              {
                "$unset": {
                  "scalar_address.city": 1
                }
              }
              """;

      var matchDoc =
          """
              {
                "id": "updateUnsetOpScalarAddressPartial",
                "scalar_address": {
                     "street": "scalar udt"
                }
              }""";

      // TODO: This is failing because we do not yet support partial updates of a UDT, it should be
      // possible to do
      assertUpdateFail(
          "updateUnsetOpScalarAddressPartial",
          update,
          UpdateException.Code.UNKNOWN_TABLE_COLUMNS,
          UpdateException.class,
          "The update included the following unknown columns: \"scalar_address.city\".");
    }

    @Test
    public void updateUnsetOpFrozenScalarAddressPartial() {
      var update =
          """
              {
                "$unset": {
                  "frozen_address.city": 1
                }
              }
              """;

      var matchDoc =
          """
              {
                "id": "updateUnsetOpScalarAddressPartial",
                "frozen_address": {
                     "street": "scalar udt"
                }
              }""";

      // TODO: this fails with unknown because we not yet support partial updates of a UDT, when we
      // have that
      // it should fail with a different error because we cannot update a any field in a frozen UDT
      assertUpdateFail(
          "updateUnsetOpFrozenScalarAddressPartial",
          update,
          UpdateException.Code.UNKNOWN_TABLE_COLUMNS,
          UpdateException.class,
          "The update included the following unknown columns: \"frozen_address.city\".");
    }

    @Test
    public void updateUnsetOpScalarAddressFull() {
      var update =
          """
              {
                "$unset": {
                  "scalar_address": 1
                }
              }
              """;

      var matchDoc =
          """
              {
                "id": "updateUnsetOpScalarAddressFull"
              }""";

      assertUpdate(
          "updateUnsetOpScalarAddressFull", update, List.of("id", "scalar_address"), matchDoc);
    }

    @Test
    public void updateUnsetOpFrozenScalarAddressFull() {
      var update =
          """
          {
            "$unset": {
              "frozen_address": 1
            }
          }
          """;

      var matchDoc =
          """
          {
            "id": "updateUnsetOpFrozenScalarAddressFull"
          }""";

      assertUpdate(
          "updateUnsetOpFrozenScalarAddressFull",
          update,
          List.of("id", "frozen_address"),
          matchDoc);
    }

    @Test
    public void updateUnsetOpSetFrozenAddressFull() {
      var update =
          """
          {
            "$unset": {
              "set_frozen_address": 1
            }
          }
          """;

      var matchDoc =
          """
          {
            "id": "updateUnsetOpSetFrozenAddressFull"
          }""";

      assertUpdate(
          "updateUnsetOpSetFrozenAddressFull",
          update,
          List.of("id", "set_frozen_address"),
          matchDoc);
    }

    @Test
    public void updateUnsetOpListFrozenAddressFull() {
      var update =
          """
          {
            "$unset": {
              "list_frozen_address": 1
            }
          }
          """;

      var matchDoc =
          """
          {
            "id": "updateUnsetOpListFrozenAddressFull"
          }""";

      assertUpdate(
          "updateUnsetOpListFrozenAddressFull",
          update,
          List.of("id", "list_frozen_address"),
          matchDoc);
    }

    @Test
    public void updateUnsetOpMapFrozenAddressFull() {
      var update =
          """
          {
            "$unset": {
              "map_frozen_address": 1
            }
          }
          """;

      var matchDoc =
          """
          {
            "id": "updateUnsetOpMapFrozenAddressFull"
          }""";

      assertUpdate(
          "updateUnsetOpMapFrozenAddressFull",
          update,
          List.of("id", "map_frozen_address"),
          matchDoc);
    }

    @Test
    public void updateUnsetOpAllFrozenSetAddressFull() {
      var update =
          """
          {
            "$unset": {
              "all_frozen_set_address": 1
            }
          }
          """;

      var matchDoc =
          """
          {
            "id": "updateUnsetOpAllFrozenSetAddressFull"
          }""";

      assertUpdate(
          "updateUnsetOpAllFrozenSetAddressFull",
          update,
          List.of("id", "all_frozen_set_address"),
          matchDoc);
    }

    @Test
    public void updateUnsetOpAllFrozenMapAddressFull() {
      var update =
          """
          {
            "$unset": {
              "all_frozen_map_address": 1
            }
          }
          """;

      var matchDoc =
          """
          {
            "id": "updateUnsetOpAllFrozenMapAddressFull"
          }""";

      assertUpdate(
          "updateUnsetOpAllFrozenMapAddressFull",
          update,
          List.of("id", "all_frozen_map_address"),
          matchDoc);
    }

    @Test
    public void updateUnsetOpAllFrozenListAddressFull() {
      var update =
          """
          {
            "$unset": {
              "all_frozen_list_address": 1
            }
          }
          """;

      var matchDoc =
          """
          {
            "id": "updateUnsetOpAllFrozenListAddressFull"
          }""";

      assertUpdate(
          "updateUnsetOpAllFrozenListAddressFull",
          update,
          List.of("id", "all_frozen_list_address"),
          matchDoc);
    }
  }

  @Nested
  class UpdateCqlDataPullAllOperation {

    @Test
    public void updatePullAllOpListFrozenAddress() {
      var doc =
          """
              {
                "id": "updatePullAllOpListFrozenAddress",
                "list_frozen_address": [
                  {
                    "street": "added 1",
                    "city": "added 1"
                  },
                  {
                    "street": "added 2",
                    "city": "added 2"
                  }
                ]
              }""";
      assertWrite(doc);

      var update =
          """
              {
                "$pullAll": {
                     "list_frozen_address": [
                          {
                          "street": "added 1",
                          "city": "added 1"
                        }
                     ]
                }
              }
              """;

      var matchDoc =
          """
              {
                "id": "updatePullAllOpListFrozenAddress",
                "list_frozen_address": [
                  {
                      "street": "added 2",
                      "city": "added 2"
                    }
                ]
              }""";

      assertUpdate(
          "updatePullAllOpListFrozenAddress",
          update,
          List.of("id", "list_frozen_address"),
          matchDoc,
          false);
    }

    @Test
    public void updatePullAllOpSetFrozenAddress() {
      var doc =
          """
              {
                "id": "updatePullAllOpSetFrozenAddress",
                "set_frozen_address": [
                  {
                    "street": "added 1",
                    "city": "added 1"
                  },
                  {
                    "street": "added 2",
                    "city": "added 2"
                  }
                ]
              }""";
      assertWrite(doc);

      var update =
          """
              {
                "$pullAll": {
                     "set_frozen_address": [
                          {
                          "street": "added 1",
                          "city": "added 1"
                        }
                     ]
                }
              }
              """;

      var matchDoc =
          """
              {
                "id": "updatePullAllOpSetFrozenAddress",
                "set_frozen_address": [
                  {
                      "street": "added 2",
                      "city": "added 2"
                    }
                ]
              }""";

      assertUpdate(
          "updatePullAllOpSetFrozenAddress",
          update,
          List.of("id", "set_frozen_address"),
          matchDoc,
          false);
    }

    @Test
    public void updatePullAllOpMapFrozenAddress() {
      var doc =
          """
              {
                "id": "updatePullAllOpMapFrozenAddress",
                "map_frozen_address": {
                  "key1": {
                    "street": "added 1",
                    "city": "added 1"
                  },
                  "key2": {
                    "street": "added 2",
                    "city": "added 2"
                  }
                }
              }""";
      assertWrite(doc);

      var update =
          """
              {
                "$pullAll": {
                     "map_frozen_address": [
                          "key1"
                     ]
                }
              }
              """;

      var matchDoc =
          """
              {
                "id": "updatePullAllOpMapFrozenAddress",
                "map_frozen_address": {
                  "key2": {
                    "street": "added 2",
                    "city": "added 2"
                  }
                }
              }""";

      assertUpdate(
          "updatePullAllOpMapFrozenAddress",
          update,
          List.of("id", "map_frozen_address"),
          matchDoc,
          false);
    }

    @Test
    public void updatePullAllOpAllFrozenSetAddress() {
      var doc =
          """
              {
                "id": "updatePullAllOpAllFrozenSetAddress",
                "all_frozen_set_address": [
                  {
                    "street": "added 1",
                    "city": "added 1"
                  },
                  {
                    "street": "added 2",
                    "city": "added 2"
                  }
                ]
              }""";
      assertWrite(doc);

      var update =
          """
              {
                "$pullAll": {
                     "all_frozen_set_address": [
                          {
                          "street": "added 1",
                          "city": "added 1"
                        }
                     ]
                }
              }
              """;

      var matchDoc =
          """
              {
                "id": "updatePullAllOpAllFrozenSetAddress",
                "all_frozen_set_address": [
                  {
                      "street": "added 2",
                      "city": "added 2"
                    }
                ]
              }""";

      assertUpdateFail(
          "updatePullAllOpAllFrozenSetAddress",
          update,
          false,
          UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR,
          UpdateException.class,
          "The command used the update operator: $pullAll.",
          "The operation was not supported by the columns: all_frozen_set_address(set).");
    }

    @Test
    public void updatePullAllOpAllFrozenMapAddress() {
      var doc =
          """
              {
                "id": "updatePullAllOpAllFrozenMapAddress",
                "all_frozen_map_address": {
                  "key1": {
                    "street": "added 1",
                    "city": "added 1"
                  },
                  "key2": {
                    "street": "added 2",
                    "city": "added 2"
                  }
                }
              }""";
      assertWrite(doc);

      var update =
          """
              {
                "$pullAll": {
                     "all_frozen_map_address": [
                         "key1"
                     ]
                }
              }
              """;

      var matchDoc =
          """
              {
                "id": "updatePullAllOpAllFrozenMapAddress",
                "all_frozen_map_address": {
                  "key2": {
                    "street": "added 2",
                    "city": "added 2"
                  }
                }
              }""";

      assertUpdateFail(
          "updatePullAllOpAllFrozenMapAddress",
          update,
          false,
          UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR,
          UpdateException.class,
          "The command used the update operator: $pullAll.",
          "The operation was not supported by the columns: all_frozen_map_address(map).");
    }

    @Test
    public void updatePullAllOpAllFrozenListAddress() {
      var doc =
          """
              {
                "id": "updatePullAllOpAllFrozenListAddress",
                "all_frozen_list_address": [
                  {
                    "street": "added 1",
                    "city": "added 1"
                  },
                  {
                    "street": "added 2",
                    "city": "added 2"
                  }
                ]
              }""";
      assertWrite(doc);

      var update =
          """
              {
                "$pullAll": {
                     "all_frozen_list_address": [
                          {
                          "street": "added 1",
                          "city": "added 1"
                        }
                     ]
                }
              }
              """;

      var matchDoc =
          """
              {
                "id": "updatePullAllOpAllFrozenListAddress",
                "all_frozen_list_address": [
                  {
                      "street": "added 2",
                      "city": "added 2"
                    }
                ]
              }""";

      assertUpdateFail(
          "updatePullAllOpAllFrozenListAddress",
          update,
          false,
          UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR,
          UpdateException.class,
          "The command used the update operator: $pullAll.",
          "The operation was not supported by the columns: all_frozen_list_address(list).");
    }
  }
}
