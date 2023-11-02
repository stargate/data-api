package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
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
public class FindOperationWithSortIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindOperationWithSort {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<Object> testDatas = getDocuments(25);

    @Test
    @Order(1)
    public void setUp() {
      Collections.shuffle(testDatas);
      insert(testDatas);
    }

    @Test
    public void sortByTextAndNullValue() throws Exception {
      sortByUserName(testDatas, true);
      String json =
          """
          {
            "find": {
              "sort" : {"username" : 1}
            }
          }
          """;
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      for (int i = 0; i < 20; i++)
        arrayNode.add(
            objectMapper.readTree(
                objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(testDatas.get(i))));
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(20))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortWithSkipLimit() throws Exception {
      sortByUserName(testDatas, true);
      String json =
          """
          {
            "find": {
              "sort" : {"username" : 1},
              "options" : {"skip": 10, "limit" : 10}
            }
          }
          """;

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

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(10))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortDescendingTextValue() throws Exception {
      sortByUserName(testDatas, false);
      String json =
          """
          {
            "find": {
              "sort" : {"username" : -1}
            }
          }
          """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      for (int i = 0; i < 20; i++)
        arrayNode.add(
            objectMapper.readTree(
                objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(testDatas.get(i))));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(20))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortBooleanValueAndMissing() throws Exception {
      sortByActiveUser(testDatas, true);
      String json =
          """
          {
            "find": {
              "sort" : {"activeUser" : 1}
            }
          }
          """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      for (int i = 0; i < 20; i++)
        arrayNode.add(
            objectMapper.readTree(
                objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(testDatas.get(i))));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(20))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortBooleanValueAndMissingDescending() throws Exception {
      sortByActiveUser(testDatas, false);
      String json =
          """
          {
            "find": {
              "sort" : {"activeUser" : -1}
            }
          }
          """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      for (int i = 0; i < 20; i++)
        arrayNode.add(
            objectMapper.readTree(
                objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(testDatas.get(i))));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(20))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortNumericField() throws Exception {
      sortByUserId(testDatas, true);
      String json =
          """
          {
            "find": {
              "sort" : {"userId" : 1}
            }
          }
          """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      for (int i = 0; i < 20; i++)
        arrayNode.add(
            objectMapper.readTree(
                objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(testDatas.get(i))));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(20))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortNumericFieldDescending() throws Exception {
      sortByUserId(testDatas, false);
      String json =
          """
          {
            "find": {
              "sort" : {"userId" : -1}
            }
          }
          """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      for (int i = 0; i < 20; i++)
        arrayNode.add(
            objectMapper.readTree(
                objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(testDatas.get(i))));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(20))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortNumericFieldAndFilter() throws Exception {
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
              "sort" : {"userId" : 1}
            }
          }
          """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(datas.size());
      for (int i = 0; i < datas.size(); i++)
        arrayNode.add(
            objectMapper.readTree(
                objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(datas.get(i))));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(Math.min(20, datas.size())))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortMultiColumns() throws Exception {
      sortByUserNameUserId(testDatas, true, true);
      String json =
          """
          {
            "find": {
              "sort" : {"username" : 1, "userId" : 1}
            }
          }
          """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(20);
      for (int i = 0; i < 20; i++)
        arrayNode.add(
            objectMapper.readTree(
                objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(testDatas.get(i))));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(20))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortMultiColumnsMixedOrder() throws Exception {
      List<Object> datas =
          testDatas.stream()
              .filter(obj -> (obj instanceof TestData o) && o.activeUser())
              .collect(Collectors.toList());
      sortByUserNameUserId(datas, true, false);
      String json =
          """
          {
            "find": {
              "filter" : {"activeUser" : true},
              "sort" : {"username" : 1, "userId" : -1}
            }
          }
          """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(datas.size());
      for (int i = 0; i < datas.size(); i++)
        arrayNode.add(
            objectMapper.readTree(
                objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(datas.get(i))));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(Math.min(20, datas.size())))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortByDate() throws Exception {
      List<Object> datas =
          testDatas.stream()
              .filter(obj -> (obj instanceof TestData o) && o.activeUser())
              .collect(Collectors.toList());
      sortByDate(datas, true);
      String json =
          """
          {
            "find": {
              "filter" : {"activeUser" : true},
              "sort" : {"dateValue" : 1}
            }
          }
          """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(datas.size());
      for (int i = 0; i < datas.size(); i++)
        arrayNode.add(
            objectMapper.readTree(
                objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(datas.get(i))));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(Math.min(20, datas.size())))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortByDateDescending() throws Exception {
      List<Object> datas =
          testDatas.stream()
              .filter(obj -> (obj instanceof TestData o) && o.activeUser())
              .collect(Collectors.toList());
      sortByDate(datas, false);
      String json =
          """
          {
            "find": {
              "filter" : {"activeUser" : true},
              "sort" : {"dateValue" : -1}
            }
          }
          """;

      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(datas.size());
      for (int i = 0; i < datas.size(); i++)
        arrayNode.add(
            objectMapper.readTree(
                objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(datas.get(i))));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(Math.min(20, datas.size())))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    private void sortByUserNameUserId(
        List<Object> testDatas, boolean ascUserName, boolean ascUserId) {
      testDatas.sort(
          new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
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
            }
          });
    }

    private int compareNode(JsonNode field1, JsonNode field2, boolean asc) {
      if (asc) return JsonNodeComparator.ascending().compare(field1, field2);
      else return JsonNodeComparator.descending().compare(field1, field2);
    }

    private void sortByUserName(List<Object> testDatas, boolean asc) {
      testDatas.sort(
          new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
              JsonNode o1j = getUserNameAsJsonNode(o1);
              JsonNode o2j = getUserNameAsJsonNode(o2);
              int compareVal = compareNode(o1j, o2j, asc);
              if (compareVal != 0) {
                return compareVal;
              } else {
                return compareNode(getIDJsonNode(o1), getIDJsonNode(o2), true);
              }
            }
          });
    }

    private void sortByUserId(List<Object> testDatas, boolean asc) {
      testDatas.sort(
          new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
              JsonNode o1j = getUserIdAsJsonNode(o1);
              JsonNode o2j = getUserIdAsJsonNode(o2);
              int compareVal = compareNode(o1j, o2j, asc);
              if (compareVal != 0) {
                return compareVal;
              } else {
                return compareNode(getIDJsonNode(o1), getIDJsonNode(o2), true);
              }
            }
          });
    }

    private void sortByActiveUser(List<Object> testDatas, boolean asc) {
      testDatas.sort(
          new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
              JsonNode o1j = getActiveUserAsJsonNode(o1);
              JsonNode o2j = getActiveUserAsJsonNode(o2);
              int compareVal = compareNode(o1j, o2j, asc);
              if (compareVal != 0) {
                return compareVal;
              } else {
                return compareNode(getIDJsonNode(o1), getIDJsonNode(o2), true);
              }
            }
          });
    }

    private void sortByDate(List<Object> testDatas, boolean asc) {
      testDatas.sort(
          new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
              JsonNode o1j = getDateValueAsJsonNode(o1);
              JsonNode o2j = getDateValueAsJsonNode(o2);
              int compareVal = compareNode(o1j, o2j, asc);
              if (compareVal != 0) {
                return compareVal;
              } else {
                return compareNode(getIDJsonNode(o1), getIDJsonNode(o2), true);
              }
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

    private JsonNode getIDJsonNode(Object data) {
      if (data instanceof TestData td) {
        return objectMapper.getNodeFactory().textNode(td._id());
      }
      if (data instanceof TestDataMissingBoolean td) {
        return objectMapper.getNodeFactory().textNode(td._id());
      }

      if (data instanceof TestDataUserIdAsText td) {
        return objectMapper.getNodeFactory().textNode(td._id());
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

    private JsonNode getDateValueAsJsonNode(Object data) {
      if (data instanceof TestData td) {
        return objectMapper.getNodeFactory().numberNode(td.dateValue().$date());
      }
      return objectMapper.getNodeFactory().missingNode();
    }

    private List<Object> getDocuments(int countOfDocuments) {
      List<Object> data = new ArrayList<>(countOfDocuments);
      for (int docId = 1; docId <= countOfDocuments - 3; docId++) {
        data.add(
            new TestData(
                "doc" + docId,
                "user" + docId,
                docId,
                docId % 2 == 0,
                new DateValue(1672531200000L + docId * 1000)));
      }
      data.add(
          new TestData(
              "doc" + (countOfDocuments - 2),
              null,
              (countOfDocuments - 2),
              (countOfDocuments - 2) % 2 == 0,
              new DateValue(1672531200000L + (countOfDocuments - 2) * 1000)));
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

    record TestData(
        String _id, String username, int userId, boolean activeUser, DateValue dateValue) {}

    record TestDataMissingBoolean(String _id, String username, int userId) {}

    record TestDataUserIdAsText(String _id, String username, String userId) {}

    record DateValue(long $date) {}
  }
}
