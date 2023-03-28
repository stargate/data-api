package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.util.JsonNodeComparator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class FindOperationWithSortIntegrationTest extends CollectionResourceBaseIntegrationTest {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindOperationWithSort {
    private ObjectMapper objectMapper = new ObjectMapper();
    private List<Object> testDatas = getDocuments(25);

    @Test
    @Order(1)
    public void setUp() {
      Collections.shuffle(testDatas);
      insert(testDatas);
    }

    @Test
    @Order(2)
    public void sortByTextAndNullValue() {
      sortByUserName(testDatas, true);
      String json =
          """
          {
            "find": {
              "sort" : ["username"]
            }
          }
        """;
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      try {
        for (int i = 0; i < 20; i++)
          arrayNode.add(
              objectMapper.readTree(
                  objectMapper
                      .writerWithDefaultPrettyPrinter()
                      .writeValueAsString(testDatas.get(i))));
      } catch (Exception e) {
        // ignore the object node creation error should never happen
      }
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(20))
          .body("data.docs", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(3)
    public void sortWithSkipLimit() {
      sortByUserName(testDatas, true);
      String json =
          """
          {
            "find": {
              "sort" : ["username"],
              "options" : {"skip": 10, "limit" : 10}
            }
          }
          """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(10);
      try {
        for (int i = 0; i < 20; i++) {
          if (i >= 10) {
            arrayNode.add(
                objectMapper.readTree(
                    objectMapper
                        .writerWithDefaultPrettyPrinter()
                        .writeValueAsString(testDatas.get(i))));
          }
        }
      } catch (Exception e) {
        // ignore the object node creation error should never happen
      }

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(10))
          .body("data.docs", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(4)
    public void sortDescendingTextValue() {
      sortByUserName(testDatas, false);
      String json =
          """
          {
            "find": {
              "sort" : ["-username"]
            }
          }
          """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      try {
        for (int i = 0; i < 20; i++)
          arrayNode.add(
              objectMapper.readTree(
                  objectMapper
                      .writerWithDefaultPrettyPrinter()
                      .writeValueAsString(testDatas.get(i))));
      } catch (Exception e) {
        // ignore the object node creation error should never happen
      }

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(20))
          .body("data.docs", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(5)
    public void sortBooleanValueAndMissing() {
      sortByActiveUser(testDatas, true);
      String json =
          """
              {
                "find": {
                  "sort" : ["activeUser"]
                }
              }
              """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      try {
        for (int i = 0; i < 20; i++)
          arrayNode.add(
              objectMapper.readTree(
                  objectMapper
                      .writerWithDefaultPrettyPrinter()
                      .writeValueAsString(testDatas.get(i))));
      } catch (Exception e) {
        // ignore the object node creation error should never happen
      }

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(20))
          .body("data.docs", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(6)
    public void sortBooleanValueAndMissingDescending() {
      sortByActiveUser(testDatas, false);
      String json =
          """
              {
                "find": {
                  "sort" : ["-activeUser"]
                }
              }
              """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      try {
        for (int i = 0; i < 20; i++)
          arrayNode.add(
              objectMapper.readTree(
                  objectMapper
                      .writerWithDefaultPrettyPrinter()
                      .writeValueAsString(testDatas.get(i))));
      } catch (Exception e) {
        // ignore the object node creation error should never happen
      }

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(20))
          .body("data.docs", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(7)
    public void sortNumericField() {
      sortByUserId(testDatas, true);
      String json =
          """
              {
                "find": {
                  "sort" : ["userId"]
                }
              }
              """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      try {
        for (int i = 0; i < 20; i++)
          arrayNode.add(
              objectMapper.readTree(
                  objectMapper
                      .writerWithDefaultPrettyPrinter()
                      .writeValueAsString(testDatas.get(i))));
      } catch (Exception e) {
        // ignore the object node creation error should never happen
      }

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(20))
          .body("data.docs", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(8)
    public void sortNumericFieldDescending() {
      sortByUserId(testDatas, false);
      String json =
          """
              {
                "find": {
                  "sort" : ["-userId"]
                }
              }
              """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      try {
        for (int i = 0; i < 20; i++)
          arrayNode.add(
              objectMapper.readTree(
                  objectMapper
                      .writerWithDefaultPrettyPrinter()
                      .writeValueAsString(testDatas.get(i))));
      } catch (Exception e) {
        // ignore the object node creation error should never happen
      }

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(20))
          .body("data.docs", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(9)
    public void sortNumericFieldAndFilter() {
      List<Object> datas =
          testDatas.stream()
              .filter(obj -> (obj instanceof TestData o) && o.activeUser())
              .collect(Collectors.toList());
      sortByUserId(datas, true);
      String json =
          """
              {
                "find": {
                  "filter" : {"activeUser" : true},
                  "sort" : ["userId"]
                }
              }
              """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(datas.size());
      try {
        for (int i = 0; i < datas.size(); i++)
          arrayNode.add(
              objectMapper.readTree(
                  objectMapper
                      .writerWithDefaultPrettyPrinter()
                      .writeValueAsString(testDatas.get(i))));
      } catch (Exception e) {
        // ignore the object node creation error should never happen
      }

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(datas.size()))
          .body("data.docs", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(10)
    public void sortMultiColumns() {
      sortByUserNameUserId(testDatas, true, true);
      String json =
          """
              {
                "find": {
                  "sort" : ["username", "userId"]
                }
              }
              """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      try {
        for (int i = 0; i < 20; i++)
          arrayNode.add(
              objectMapper.readTree(
                  objectMapper
                      .writerWithDefaultPrettyPrinter()
                      .writeValueAsString(testDatas.get(i))));
      } catch (Exception e) {
        // ignore the object node creation error should never happen
      }

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(20))
          .body("data.docs", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(11)
    public void sortMultiColumnsMixedOrder() {
      List<Object> datas =
          testDatas.stream()
              .filter(obj -> (obj instanceof TestData o) && o.activeUser())
              .collect(Collectors.toList());
      sortByUserNameUserId(testDatas, true, false);
      String json =
          """
              {
                "find": {
                  "filter" : {"activeUser" : true},
                  "sort" : ["username", "-userId"]
                }
              }
              """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(datas.size());
      try {
        for (int i = 0; i < datas.size(); i++)
          arrayNode.add(
              objectMapper.readTree(
                  objectMapper
                      .writerWithDefaultPrettyPrinter()
                      .writeValueAsString(testDatas.get(i))));
      } catch (Exception e) {
        // ignore the object node creation error should never happen
      }

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(datas.size()))
          .body("data.docs", jsonEquals(arrayNode.toString()));
    }

    private void sortByUserNameUserId(
        List<Object> testDatas, boolean ascUserName, boolean ascUserId) {
      Collections.sort(
          testDatas,
          new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
              JsonNode o1j1 = getUserNameAsJsonNode(o1);
              JsonNode o2j1 = getUserNameAsJsonNode(o2);
              JsonNode o1j2 = getUserIdAsJsonNode(o1);
              JsonNode o2j2 = getUserIdAsJsonNode(o2);

              int compareField1 = compare(o1j1, o2j1);
              if (compareField1 != 0) return compareField1;
              else {
                return compare(o1j2, o2j2);
              }
            }
          });
    }

    private int compare(JsonNode field1, JsonNode field2, boolean asc) {
      if (asc) return JsonNodeComparator.ascending().compare(field1, field2);
      else return JsonNodeComparator.descending().compare(field1, field2);
    }

    private void sortByUserName(List<Object> testDatas, boolean asc) {
      Collections.sort(
          testDatas,
          new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
              JsonNode o1j = getUserNameAsJsonNode(o1);
              JsonNode o2j = getUserNameAsJsonNode(o2);
              return compare(o1j, o2j);
            }
          });
    }

    private void sortByUserId(List<Object> testDatas, boolean asc) {
      Collections.sort(
          testDatas,
          new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
              JsonNode o1j = getUserIdAsJsonNode(o1);
              JsonNode o2j = getUserIdAsJsonNode(o2);
              return compare(o1j, o2j);
            }
          });
    }

    private void sortByActiveUser(List<Object> testDatas, boolean asc) {
      Collections.sort(
          testDatas,
          new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
              JsonNode o1j = getActiveUserAsJsonNode(o1);
              JsonNode o2j = getActiveUserAsJsonNode(o2);
              return compare(o1j, o2j);
            }
          });
    }

    private JsonNode getUserNameAsJsonNode(Object data) {
      if (data instanceof TestData td) {
        if (td.username() == null) return objectMapper.getNodeFactory().nullNode();
        return objectMapper.getNodeFactory().textNode(td.username());
      }
      if (data instanceof TestDataMissingBoolean td) {
        return objectMapper.getNodeFactory().textNode(td.username());
      }

      if (data instanceof TestDataUserIdAsText td) {
        return objectMapper.getNodeFactory().textNode(td.username());
      }

      return objectMapper.getNodeFactory().missingNode();
    }

    private JsonNode getUserIdAsJsonNode(Object data) {
      if (data instanceof TestData td) {
        return objectMapper.getNodeFactory().numberNode(td.userId());
      }
      if (data instanceof TestDataMissingBoolean td) {
        return objectMapper.getNodeFactory().numberNode(td.userId());
      }

      if (data instanceof TestDataUserIdAsText td) {
        return objectMapper.getNodeFactory().textNode(td.userId());
      }

      return objectMapper.getNodeFactory().missingNode();
    }

    private JsonNode getActiveUserAsJsonNode(Object data) {
      if (data instanceof TestData td) {
        return objectMapper.getNodeFactory().booleanNode(td.activeUser());
      }
      return objectMapper.getNodeFactory().missingNode();
    }

    private List<Object> getDocuments(int countOfDocuments) {
      List<Object> data = new ArrayList<>(countOfDocuments);
      for (int docId = 1; docId <= countOfDocuments - 3; docId++) {
        data.add(new TestData("doc" + docId, "user" + docId, docId, docId % 2 == 0));
      }
      data.add(
          new TestData(
              "doc" + (countOfDocuments - 2),
              null,
              (countOfDocuments - 2),
              (countOfDocuments - 2) % 2 == 0));
      data.add(
          new TestDataMissingBoolean(
              "doc" + (countOfDocuments - 1),
              "user" + (countOfDocuments - 1),
              (countOfDocuments - 1)));
      data.add(
          new TestDataUserIdAsText(
              "doc" + (countOfDocuments), "user" + (countOfDocuments), "" + 1));
      return data;
    }

    private void insert(List<Object> testDatas) {
      testDatas.forEach(
          testData -> {
            String json = null;
            try {
              json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(testData);
            } catch (JsonProcessingException e) {
              e.printStackTrace();
            }
            insertDoc(json);
          });
    }

    record TestData(String id, String username, int userId, boolean activeUser) {}

    record TestDataMissingBoolean(String id, String username, int userId) {}

    record TestDataUserIdAsText(String id, String username, String userId) {}
  }
}
