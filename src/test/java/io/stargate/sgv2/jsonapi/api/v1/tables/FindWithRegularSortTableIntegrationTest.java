package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.util.JsonNodeComparator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Integration tests for the find command with regular (non-ANN, non-Lexical) sorting on a table.
 *
 * <p>This test class creates a table with various fields and performs find operations with sorting
 * on different fields, including text and numeric fields, both ascending and descending.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
public class FindWithRegularSortTableIntegrationTest extends AbstractTableIntegrationTestBase {
  static final String TABLE_WITH_STRING_ID_AGE_NAME = "sort_testing";
  private static final List<Object> testDatas = getDocuments(25);

  @BeforeAll
  public final void createDefaultTables() {
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_STRING_ID_AGE_NAME,
            Map.of(
                "id",
                Map.of("type", "text"),
                "age",
                Map.of("type", "int"),
                "name",
                Map.of("type", "text"),
                "city",
                Map.of("type", "text"),
                "active_user",
                Map.of("type", "boolean")),
            "id")
        .wasSuccessful();
    Collections.shuffle(testDatas);
    insert(TABLE_WITH_STRING_ID_AGE_NAME, testDatas);
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindSortSuccess {

    @Test
    @Order(1)
    public void sortByTextAndNullValue() throws Exception {
      sortByName(testDatas, true);
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      for (int i = 0; i < 20; i++) {
        arrayNode.add(
            objectMapper.readTree(
                objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(testDatas.get(i))));
      }

      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .templated()
          .find(Map.of(), List.of(), Map.of("name", 1), Map.of())
          .wasSuccessful()
          .body("data.documents", hasSize(20))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(2)
    public void sortWithSkipLimit() throws Exception {
      sortByName(testDatas, true);
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(10);
      for (int i = 0; i < 20; i++) {
        if (i >= 10) {
          arrayNode.add(
              objectMapper.readTree(
                  objectMapper
                      .writerWithDefaultPrettyPrinter()
                      .writeValueAsString(testDatas.get(i))));
        }
      }
      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .templated()
          .find(Map.of(), List.of(), Map.of("name", 1), Map.of("skip", 10, "limit", 10))
          .wasSuccessful()
          .body("data.documents", hasSize(10))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(3)
    public void sortDescendingTextValue() throws Exception {
      sortByName(testDatas, false);
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      for (int i = 0; i < 20; i++) {
        arrayNode.add(
            objectMapper.readTree(
                objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(testDatas.get(i))));
      }

      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .templated()
          .find(Map.of(), List.of(), Map.of("name", -1), Map.of())
          .wasSuccessful()
          .body("data.documents", hasSize(20))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(4)
    public void sortNumericField() throws Exception {
      sortByAge(testDatas, true);

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      for (int i = 0; i < 20; i++) {
        arrayNode.add(
            objectMapper.readTree(
                objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(testDatas.get(i))));
      }

      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .templated()
          .find(Map.of(), List.of(), Map.of("age", 1), Map.of())
          .wasSuccessful()
          .body("data.documents", hasSize(20))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(5)
    public void sortNumericFieldDescending() throws Exception {
      sortByAge(testDatas, false);
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      for (int i = 0; i < 20; i++) {
        arrayNode.add(
            objectMapper.readTree(
                objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(testDatas.get(i))));
      }
      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .templated()
          .find(Map.of(), List.of(), Map.of("age", -1), Map.of())
          .wasSuccessful()
          .body("data.documents", hasSize(20))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(6)
    public void sortNumericFieldAndFilter() throws Exception {
      List<Object> datas =
          testDatas.stream()
              .filter(obj -> (obj instanceof TestData o) && o.active_user())
              .collect(Collectors.toList());
      sortByAge(datas, true);
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(datas.size());
      for (Object data : datas) {
        arrayNode.add(
            objectMapper.readTree(
                objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(data)));
      }

      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .templated()
          .find(Map.of("active_user", true), List.of(), Map.of("age", 1), Map.of())
          .wasSuccessful()
          .body("data.documents", hasSize(datas.size()))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(7)
    public void sortMultiColumns() throws Exception {
      sortByUserNameUserId(testDatas, true, true);
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      for (int i = 0; i < 20; i++) {
        arrayNode.add(
            objectMapper.readTree(
                objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(testDatas.get(i))));
      }
      final LinkedHashMap ordering = new LinkedHashMap<>();
      ordering.put("name", 1);
      ordering.put("age", 1);
      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .templated()
          .find(Map.of(), List.of(), ordering, Map.of())
          .wasSuccessful()
          .body("data.documents", hasSize(20))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(8)
    public void sortMultiColumnsMixedOrder() throws Exception {
      List<Object> datas =
          testDatas.stream()
              .filter(obj -> (obj instanceof TestData o) && o.active_user())
              .collect(Collectors.toList());
      sortByUserNameUserId(datas, true, false);
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(datas.size());
      for (Object data : datas) {
        arrayNode.add(
            objectMapper.readTree(
                objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(data)));
      }
      final LinkedHashMap ordering = new LinkedHashMap<>();
      ordering.put("name", 1);
      ordering.put("age", -1);
      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .templated()
          .find(Map.of("active_user", true), List.of(), ordering, Map.of())
          .wasSuccessful()
          .body("data.documents", hasSize(datas.size()))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    private void sortByUserNameUserId(
        List<Object> testDatas, boolean ascUserName, boolean ascUserId) {
      testDatas.sort(
          (o1, o2) -> {
            JsonNode o1j1 = getUserNameAsJsonNode(o1);
            JsonNode o2j1 = getUserNameAsJsonNode(o2);
            JsonNode o1j2 = getUserIdAsJsonNode(o1);
            JsonNode o2j2 = getUserIdAsJsonNode(o2);

            int compareField1 = compareNode(o1j1, o2j1, ascUserName);
            if (compareField1 != 0) return compareField1;
            else {
              int compareField2 = compareNode(o1j2, o2j2, ascUserId);
              if (compareField2 != 0) return compareField2;
              else return compareNode(getIDJsonNode(o1), getIDJsonNode(o2), true);
            }
          });
    }

    private int compareNode(JsonNode field1, JsonNode field2, boolean asc) {
      if (asc) return JsonNodeComparator.ascending().compare(field1, field2);
      else return JsonNodeComparator.descending().compare(field1, field2);
    }

    private void sortByName(List<Object> testDatas, boolean asc) {
      testDatas.sort(
          (o1, o2) -> {
            JsonNode o1j = getUserNameAsJsonNode(o1);
            JsonNode o2j = getUserNameAsJsonNode(o2);
            int compareVal = compareNode(o1j, o2j, asc);
            if (compareVal != 0) {
              return compareVal;
            } else {
              return compareNode(getIDJsonNode(o1), getIDJsonNode(o2), true);
            }
          });
    }

    private void sortByAge(List<Object> testDatas, boolean asc) {
      testDatas.sort(
          (o1, o2) -> {
            JsonNode o1j = getUserIdAsJsonNode(o1);
            JsonNode o2j = getUserIdAsJsonNode(o2);
            int compareVal = compareNode(o1j, o2j, asc);
            if (compareVal != 0) {
              return compareVal;
            } else {
              return compareNode(getIDJsonNode(o1), getIDJsonNode(o2), true);
            }
          });
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindSortFail {
    static final String biggerTableName = "col_fail_" + RandomStringUtils.insecure().nextAlphanumeric(16);

    // Test limit is max 100, create couple more
    private static final List<Object> bigTestDatas = getDocuments(110);

    @Test
    @Order(1)
    public void setUp() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
              biggerTableName,
              Map.of(
                  "id",
                  Map.of("type", "text"),
                  "age",
                  Map.of("type", "int"),
                  "name",
                  Map.of("type", "text"),
                  "city",
                  Map.of("type", "text"),
                  "active_user",
                  Map.of("type", "boolean")),
              "id")
          .wasSuccessful();
      Collections.shuffle(bigTestDatas);
      insert(biggerTableName, bigTestDatas);
    }

    @Test
    public void sortFailDueToTooMany() {
      assertTableCommand(keyspaceName, biggerTableName)
          .templated()
          .find(Map.of(), List.of(), Map.of("name", 1), Map.of())
          .body("errors[0].errorCode", is(SortException.Code.OVERLOADED_SORT_ROW_LIMIT.name()));
    }
  }

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private JsonNode getUserNameAsJsonNode(Object data) {
    if (data instanceof TestData td) {
      if (td.name() == null) return objectMapper.getNodeFactory().nullNode();
      return objectMapper.getNodeFactory().textNode(td.name());
    }

    return objectMapper.getNodeFactory().missingNode();
  }

  private JsonNode getIDJsonNode(Object data) {
    if (data instanceof TestData td) {
      return objectMapper.getNodeFactory().textNode(td.id());
    }
    return objectMapper.getNodeFactory().missingNode();
  }

  private JsonNode getUserIdAsJsonNode(Object data) {
    if (data instanceof TestData td) {
      return objectMapper.getNodeFactory().numberNode(td.age());
    }

    return objectMapper.getNodeFactory().missingNode();
  }

  private static List<Object> getDocuments(int countOfDocuments) {
    List<Object> data = new ArrayList<>(countOfDocuments);

    for (int docId = 1; docId <= countOfDocuments - 3; docId++) {
      data.add(
          new TestData(
              "doc" + docId,
              "user" + docId,
              countOfDocuments - docId,
              "city" + docId,
              docId % 2 == 0));
    }
    return data;
  }

  private void insert(String tableName, List<Object> testDatas) {
    testDatas.forEach(
        testData -> {
          String json;
          try {
            json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(testData);
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
          assertTableCommand(keyspaceName, tableName).templated().insertOne(json).wasSuccessful();
        });
  }

  record TestData(String id, String name, int age, String city, boolean active_user) {}
}
