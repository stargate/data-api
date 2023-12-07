package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class RangeReadIntegrationTest extends AbstractCollectionIntegrationTestBase {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(1)
  class Find {
    private final List<Object> testDatas = getDocuments(25);

    @Test
    @Order(1)
    public void setUp() {
      insert(testDatas);
    }

    @Test
    @Order(2)
    public void gt() throws Exception {
      int[] ids = {24, 25};
      List<Object> testDatas = getDocuments(ids);
      String json =
          """
        {
          "find": {
            "filter" : {"userId" : {"$gt" : 23}},
            "sort" : {"userId" : 1}
          }
        }
        """;
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(testDatas.size());
      for (int i = 0; i < testDatas.size(); i++)
        arrayNode.add(objectMapper.valueToTree(testDatas.get(i)));
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
          .body("data.documents", hasSize(2))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(3)
    public void gte() throws Exception {
      int[] ids = {23, 24, 25};
      List<Object> testDatas = getDocuments(ids);
      String json =
          """
        {
          "find": {
            "filter" : {"userId" : {"$gte" : 23}},
            "sort" : {"userId" : 1}
          }
        }
        """;
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(testDatas.size());
      for (int i = 0; i < testDatas.size(); i++)
        arrayNode.add(objectMapper.valueToTree(testDatas.get(i)));
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
          .body("data.documents", hasSize(3))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(4)
    public void lt() throws Exception {
      int[] ids = {1, 2};
      List<Object> testDatas = getDocuments(ids);
      String json =
          """
        {
          "find": {
            "filter" : {"userId" : {"$lt" : 3}},
            "sort" : {"userId" : 1}
          }
        }
        """;
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(testDatas.size());
      for (int i = 0; i < testDatas.size(); i++)
        arrayNode.add(objectMapper.valueToTree(testDatas.get(i)));
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
          .body("data.documents", hasSize(2))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(5)
    public void lte() throws Exception {
      int[] ids = {1, 2, 3};
      List<Object> testDatas = getDocuments(ids);
      String json =
          """
        {
          "find": {
            "filter" : {"userId" : {"$lte" : 3}},
            "sort" : {"userId" : 1}
          }
        }
        """;
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(testDatas.size());
      for (int i = 0; i < testDatas.size(); i++)
        arrayNode.add(objectMapper.valueToTree(testDatas.get(i)));
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
          .body("data.documents", hasSize(3))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(7)
    public void rangeWithDate() throws Exception {
      int[] ids = {24, 25};
      List<Object> testDatas = getDocuments(ids);
      String json =
          """
        {
          "find": {
            "filter" : {"dateValue" : {"$gt" : {"$date" : 1672531223000}}},
            "sort" : {"userId" : 1}
          }
        }
        """;
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(testDatas.size());
      for (int i = 0; i < testDatas.size(); i++)
        arrayNode.add(objectMapper.valueToTree(testDatas.get(i)));
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
          .body("data.documents", hasSize(2))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(8)
    public void rangeWithText() throws Exception {
      int[] ids = {24, 25};
      List<Object> testDatas = getDocuments(ids);
      String json =
          """
        {
          "find": {
            "filter" : {"activeUser" : {"$gt" : "data"}},
            "sort" : {"userId" : 1}
          }
        }
        """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              is("Invalid filter expression, $gt operator must have `DATE` or `NUMBER` value"))
          .body("errors[0].errorCode", is("INVALID_FILTER_EXPRESSION"));
    }

    @Test
    @Order(9)
    public void gtWithFindOne() throws Exception {
      int[] ids = {24};
      List<Object> testDatas = getDocuments(ids);
      String json =
          """
        {
          "findOne": {
            "filter" : {"userId" : {"$gt" : 23}},
            "sort" : {"userId" : 1}
          }
        }
        """;
      final String expected = objectMapper.writeValueAsString(testDatas.get(0));
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
          .body("data.document", jsonEquals(expected));
    }

    @Test
    @Order(10)
    public void gtWithIDRange() throws Exception {
      int[] ids = {24, 25};
      List<Object> testDatas = getDocuments(ids);
      String json =
          """
            {
              "findOne": {
                "filter" : {"_id" : {"$gt" : 23}},
                "sort" : {"userId" : 1}
              }
            }
            """;
      final String expected = objectMapper.writeValueAsString(testDatas.get(0));
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
          .body("data.document", is(notNullValue()))
          .body("data.document", jsonEquals(expected));
    }

    @Test
    @Order(11)
    public void gtWithDeleteOne() throws Exception {
      String json =
          """
        {
          "deleteOne": {
            "filter" : {"userId" : {"$gt" : 23}},
            "sort" : {"userId" : 1}
          }
        }
        """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("status.deletedCount", is(1));
    }

    @Test
    @Order(12)
    public void gtWithDeleteMany() throws Exception {
      String json =
          """
              {
                "deleteMany": {
                  "filter" : {"userId" : {"$gte" : 23}}                }
              }
              """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("status.deletedCount", is(2));
    }
  }

  private List<Object> getDocuments(int countOfDocuments) {
    List<Object> data = new ArrayList<>(countOfDocuments);
    for (int docId = 1; docId <= countOfDocuments; docId++) {
      data.add(
          new TestData(
              docId,
              "user" + docId,
              docId,
              docId % 2 == 0,
              new DateValue(1672531200000L + docId * 1000)));
    }
    return data;
  }

  private List<Object> getDocuments(int[] docIds) {
    List<Object> data = new ArrayList<>(docIds.length);
    for (int docId : docIds) {
      data.add(
          new TestData(
              docId,
              "user" + docId,
              docId,
              docId % 2 == 0,
              new DateValue(1672531200000L + docId * 1000)));
    }
    return data;
  }

  private void insert(List<Object> testDatas) {
    testDatas.forEach(
        testData -> {
          String json = null;
          try {
            json = objectMapper.writeValueAsString(testData);
          } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
          }
          insertDoc(json);
        });
  }

  record TestData(int _id, String username, int userId, boolean activeUser, DateValue dateValue) {}

  record DateValue(long $date) {}
}
