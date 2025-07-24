package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@QuarkusTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class MapSetListFilterTableIntegrationTest extends AbstractTableIntegrationTestBase {

  static final String INDEX_TABLE = "indexed_table";
  static final String NO_INDEX_TABLE = "no_indexed_table";

  static final Map<String, Object> TABLE_SCHEMA =
      Map.ofEntries(
          Map.entry("id", Map.of("type", "text")),
          Map.entry("name", Map.of("type", "int")),
          Map.entry("list_type", Map.of("type", "list", "valueType", "text")),
          Map.entry("set_type", Map.of("type", "set", "valueType", "text")),
          Map.entry("map_type", Map.of("type", "map", "keyType", "text", "valueType", "text")));

  static final String ROW_1 =
      """
                      {
                          "id": "1",
                          "list_type": [
                              "apple","orange"
                          ],
                          "set_type": [
                              "monkey", "tiger"
                          ],
                          "map_type": [
                              ["UK","London"],
                              ["CHINA","Beijing"]
                          ]
                      }
                      """;

  static final String ROW_2 =
      """
                      {
                          "id": "2",
                          "list_type": [
                              "banana", "grape"
                          ],
                          "set_type": [
                              "lion", "elephant"
                          ],
                          "map_type": [
                              ["USA", "Washington"],
                              ["CANADA", "Ottawa"]
                          ]
                      }
                      """;

  static final String ROW_3 =
      """
                      {
                          "id": "3",
                          "list_type": [
                              "kiwi", "melon", "mongo"
                          ],
                          "set_type": [
                              "zebra", "giraffe","pig"
                          ],
                          "map_type": [
                              ["FRANCE", "Paris"],
                              ["GERMANY", "Berlin"]
                          ]
                      }
                      """;

  static final String[] ROWS = new String[] {ROW_1, ROW_2, ROW_3};

  @BeforeAll
  public final void createSimpleTable() {
    // table with map/set/list fully indexed
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(INDEX_TABLE, TABLE_SCHEMA, "id")
        .wasSuccessful();
    // create list index
    assertTableCommand(keyspaceName, INDEX_TABLE)
        .templated()
        .createIndex("list_index", "list_type");
    // create set index
    assertTableCommand(keyspaceName, INDEX_TABLE).templated().createIndex("set_index", "set_type");
    // create map entry index
    assertTableCommand(keyspaceName, INDEX_TABLE)
        .templated()
        .createIndex("map_entry_index", "map_type");
    // create map key index
    assertTableCommand(keyspaceName, INDEX_TABLE)
        .templated()
        .createIndexOnMapKeys("map_key_index", "map_type");
    // create map value index
    assertTableCommand(keyspaceName, INDEX_TABLE)
        .templated()
        .createIndexOnMapValues("map_value_index", "map_type");

    // table with no index for map/set/list
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(NO_INDEX_TABLE, TABLE_SCHEMA, "id")
        .wasSuccessful();

    // insert 3 rows for above two tables
    for (int i = 0; i < 3; i++) {
      assertTableCommand(keyspaceName, INDEX_TABLE).templated().insertOne(ROWS[i]).wasSuccessful();
    }
    for (int i = 0; i < 3; i++) {
      assertTableCommand(keyspaceName, NO_INDEX_TABLE)
          .templated()
          .insertOne(ROWS[i])
          .wasSuccessful();
    }
  }

  private static Stream<Object> listFilters() {
    var filter1 =
        """
                {"list_type": {"$in": ["apple","orange"]}}
            """;
    var filter2 =
        """
                {"list_type": {"$nin": ["apple"]}}
            """;
    var filter3 =
        """
                {"list_type": {"$all": ["kiwi", "melon", "mongo"]}}
            """;
    var filter4 =
        """
                {"$not": {"list_type": {"$all": ["apple","orange"]}}}
            """;
    return Stream.of(
        Arguments.of(filter1, List.of("1")),
        Arguments.of(filter2, List.of("2", "3")),
        Arguments.of(filter3, List.of("3")),
        Arguments.of(filter4, List.of("2", "3")));
  }

  @ParameterizedTest
  @MethodSource("listFilters")
  public void listFilters(String filter, List<Object> expectedIds) {
    assertTableCommand(keyspaceName, INDEX_TABLE)
        .postFindWithFilter(filter)
        .wasSuccessful()
        .hasDocumentsMatchedByIds(expectedIds);
  }

  private static Stream<Object> setFilters() {
    var filter1 =
        """
                    {"set_type": {"$in": ["monkey","tiger"]}}
                """;
    var filter2 =
        """
                    {"set_type": {"$nin": ["monkey"]}}
                """;
    var filter3 =
        """
                    {"set_type": {"$all": ["zebra", "giraffe","pig"]}}
                """;
    var filter4 =
        """
                    {"$not": {"set_type": {"$all": ["monkey","tiger"]}}}
                """;
    return Stream.of(
        Arguments.of(filter1, List.of("1")),
        Arguments.of(filter2, List.of("2", "3")),
        Arguments.of(filter3, List.of("3")),
        Arguments.of(filter4, List.of("2", "3")));
  }

  @ParameterizedTest
  @MethodSource("setFilters")
  public void setFilters(String filter, List<Object> expectedIds) {
    assertTableCommand(keyspaceName, INDEX_TABLE)
        .postFindWithFilter(filter)
        .wasSuccessful()
        .hasDocumentsMatchedByIds(expectedIds);
  }

  private static Stream<Object> mapFilters() {
    // filter on map entry
    var filter1 =
        """
                    {"map_type": {"$in": [["USA","Washington"],["CHINA","Beijing"]]}}
                """;
    var filter2 =
        """
                    {"map_type": {"$nin": [["USA","Washington"],["CHINA","Beijing"]]}}
                """;
    var filter3 =
        """
                  {"map_type": {"$all": [["USA","Washington"],["CANADA","Ottawa"]]}}
                """;
    var filter4 =
        """
                    {"$not": {"map_type": {"$all": [["UK","London"],["CHINA","Beijing"]]}}}
                """;
    // filter on map key
    var filter5 =
        """
                {"map_type": {"$keys": {"$in": ["USA","FRANCE"]}}}
            """;
    var filter6 =
        """
                {"map_type": {"$keys": {"$nin": ["USA","FRANCE"]}}}
            """;
    var filter7 =
        """
                {"map_type": {"$keys": {"$all": ["GERMANY","FRANCE"]}}}
            """;
    var filter8 =
        """
                {"$not": {"map_type": {"$keys": {"$all": ["UK","CANADA"]}}}}
            """;
    // filter on map value
    var filter9 =
        """
                {"map_type": {"$values": {"$in": ["Washington","London"]}}}
            """;
    var filter10 =
        """
                {"map_type": {"$values": {"$nin": ["Washington","London"]}}}
            """;
    var filter11 =
        """
                {"map_type": {"$values": {"$all": ["Washington","London"]}}}
            """;
    var filter12 =
        """
                {"$not": {"map_type": {"$values": {"$all": ["Washington","Ottawa"]}}}}
            """;
    return Stream.of(
        Arguments.of(filter1, List.of("1", "2")),
        Arguments.of(filter2, List.of("3")),
        Arguments.of(filter3, List.of("2")),
        Arguments.of(filter4, List.of("2", "3")),
        Arguments.of(filter5, List.of("2", "3")),
        Arguments.of(filter6, List.of("1")),
        Arguments.of(filter7, List.of("3")),
        Arguments.of(filter8, List.of("1", "2", "3")),
        Arguments.of(filter9, List.of("1", "2")),
        Arguments.of(filter10, List.of("3")),
        Arguments.of(filter11, List.of()),
        Arguments.of(filter12, List.of("1", "3")));
  }

  @ParameterizedTest
  @MethodSource("mapFilters")
  public void mapFilters(String filter, List<Object> expectedIds) {
    assertTableCommand(keyspaceName, INDEX_TABLE)
        .postFindWithFilter(filter)
        .wasSuccessful()
        .hasDocumentsMatchedByIds(expectedIds);
  }
}
