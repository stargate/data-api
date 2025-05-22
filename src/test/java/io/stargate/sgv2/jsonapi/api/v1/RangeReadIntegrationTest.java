package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
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
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(testDatas.size());
      for (int i = 0; i < testDatas.size(); i++) {
        arrayNode.add(objectMapper.valueToTree(testDatas.get(i)));
      }
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"userId" : {"$gt" : 23}},
            "sort" : {"userId" : 1}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(2))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(3)
    public void gte() throws Exception {
      int[] ids = {23, 24, 25};
      List<Object> testDatas = getDocuments(ids);
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(testDatas.size());
      for (int i = 0; i < testDatas.size(); i++) {
        arrayNode.add(objectMapper.valueToTree(testDatas.get(i)));
      }
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"userId" : {"$gte" : 23}},
            "sort" : {"userId" : 1}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(3))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(4)
    public void lt() throws Exception {
      int[] ids = {1, 2};
      List<Object> testDatas = getDocuments(ids);
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(testDatas.size());
      for (int i = 0; i < testDatas.size(); i++) {
        arrayNode.add(objectMapper.valueToTree(testDatas.get(i)));
      }
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"userId" : {"$lt" : 3}},
            "sort" : {"userId" : 1}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(2))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(5)
    public void lte() throws Exception {
      int[] ids = {1, 2, 3};
      List<Object> testDatas = getDocuments(ids);
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(testDatas.size());
      for (int i = 0; i < testDatas.size(); i++) {
        arrayNode.add(objectMapper.valueToTree(testDatas.get(i)));
      }
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"userId" : {"$lte" : 3}},
            "sort" : {"userId" : 1}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(3))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(7)
    public void rangeWithDate() throws Exception {
      int[] ids = {24, 25};
      List<Object> testDatas = getDocuments(ids);
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(testDatas.size());
      for (int i = 0; i < testDatas.size(); i++) {
        arrayNode.add(objectMapper.valueToTree(testDatas.get(i)));
      }
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"dateValue" : {"$gt" : {"$date" : 1672531223000}}},
            "sort" : {"userId" : 1}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(2))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    @Order(8)
    public void rangeWithText() throws Exception {
      JsonNodeFactory nodefactory = objectMapper.getNodeFactory();
      final ArrayNode arrayNode = nodefactory.arrayNode(testDatas.size());
      for (int i = 0; i < testDatas.size(); i++) {
        arrayNode.add(objectMapper.valueToTree(testDatas.get(i)));
      }
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"username" : {"$gt" : "user23"}},
            "sort" : {"userId" : 1}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", notNullValue());
    }

    @Test
    @Order(8)
    public void rangeWithBoolean() {
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                "filter" : {"activeUser" : {"$gt" : false}},
                "sort" : {"userId" : 1}
              }
            }
            """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", notNullValue());
    }

    @Test
    @Order(9)
    public void gtWithFindOne() throws Exception {
      int[] ids = {24};
      List<Object> testDatas = getDocuments(ids);
      final String expected = objectMapper.writeValueAsString(testDatas.get(0));
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "findOne": {
            "filter" : {"userId" : {"$gt" : 23}},
            "sort" : {"userId" : 1}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(expected));
    }

    @Test
    @Order(10)
    public void gtWithIDRange() throws Exception {
      int[] ids = {24, 25};
      List<Object> testDatas = getDocuments(ids);
      final String expected = objectMapper.writeValueAsString(testDatas.get(0));
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "findOne": {
                "filter" : {"_id" : {"$gt" : 23}},
                "sort" : {"userId" : 1}
              }
            }
            """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(notNullValue()))
          .body("data.document", jsonEquals(expected));
    }

    @Test
    @Order(11)
    public void gtWithDeleteOne() {
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "deleteOne": {
            "filter" : {"userId" : {"$gt" : 23}},
            "sort" : {"userId" : 1}
          }
        }
        """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(1));
    }

    @Test
    @Order(12)
    public void gtWithDeleteMany() {
      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "deleteMany": {
                  "filter" : {"userId" : {"$gte" : 23}} }
              }
              """)
          .body("$", responseIsStatusOnly())
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
          String json;
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

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(2)
  class DocumentIdRange {

    // DocumentId with different types, Date/String/Boolean/BigDecimal
    static Stream<Arguments> documentIds() {
      return Stream.of(
          Arguments.of(Map.of("$date", 1672531200000L)),
          Arguments.of("doc1"),
          Arguments.of(true),
          Arguments.of(123));
    }

    @Test
    @Order(0)
    public void cleanUpCollection() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "deleteMany": {
                    "filter": {}
                  }
                }
                """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(-1))
          .body("status.moreData", is(nullValue()));
    }

    @ParameterizedTest()
    @MethodSource("documentIds")
    @Order(1)
    public void inserts(Object id) throws Exception {
      givenHeadersPostJsonThenOkNoErrors(
                  """
                      {
                        "insertOne": {
                          "document": {
                            "_id": %s
                          }
                        }
                      }
                      """
                  .formatted(objectMapper.writeValueAsString(id)))
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is(id));
    }

    @ParameterizedTest()
    @MethodSource("documentIds")
    @Order(2)
    // Take $lte as example, we can use equal to test the filter value against inserted value.
    public void rangeTest(Object id) throws JsonProcessingException {
      givenHeadersPostJsonThenOkNoErrors(
                  """
                      {
                        "find": {
                          "filter" : {"_id" : {"$lte" : %s}}
                        }
                      }
                      """
                  .formatted(objectMapper.writeValueAsString(id)))
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body("data.documents[0]._id", is(id));
    }

    @Order(3)
    @Test
    public void InvalidRangeFilter() {
      String filter =
          """
          {"_id" : {"$lte" : null}}
          """;
      givenHeadersPostJsonThenOk("{ \"findOne\": { \"filter\" : %s}}".formatted(filter))
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_FILTER_EXPRESSION"))
          .body(
              "errors[0].message",
              containsString(
                  "$lte operator must have `DATE` or `NUMBER` or `TEXT` or `BOOLEAN` value"));
    }
  }
}
