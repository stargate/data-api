package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
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

    @Test
    @Order(1)
    public void sort() {
      deleteAllDocuments();
      Map<String, String> sorted = getDocuments(25, true);
      insert(sorted);
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
      final Iterator<Map.Entry<String, String>> iterator = sorted.entrySet().iterator();
      try {
        for (int i = 0; i < 20; i++)
          arrayNode.add(objectMapper.readTree(iterator.next().getValue()));
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
    @Order(2)
    public void sortWithSkipLimit() {
      deleteAllDocuments();
      Map<String, String> sorted = getDocuments(25, true);
      insert(sorted);
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
      final Iterator<Map.Entry<String, String>> iterator = sorted.entrySet().iterator();
      try {
        for (int i = 0; i < 20; i++) {
          String value = iterator.next().getValue();
          if (i >= 10) {
            arrayNode.add(objectMapper.readTree(value));
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
    @Order(3)
    public void sortDescending() {
      deleteAllDocuments();
      Map<String, String> sorted = getDocuments(25, false);
      insert(sorted);
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
      final Iterator<Map.Entry<String, String>> iterator = sorted.entrySet().iterator();
      try {
        for (int i = 0; i < 20; i++)
          arrayNode.add(objectMapper.readTree(iterator.next().getValue()));
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

    private void insert(Map<String, String> documents) {
      documents.values().forEach(doc -> insertDoc(doc));
    }

    private Map<String, String> getDocuments(int countOfDocuments, boolean asc) {
      String json = "{\"_id\":\"doc%s\", \"username\":\"user%s\", \"active_user\":true}";
      Map<String, String> data =
          new TreeMap<>(
              new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                  return asc ? o1.compareTo(o2) : o2.compareTo(o1);
                }
              });
      for (int docId = 1; docId <= countOfDocuments; docId++) {
        data.put("user%s".formatted(docId), json.formatted(docId, docId));
      }
      return data;
    }
  }
}
