package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.uuid.Generators;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.util.JsonNodeComparator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
public class FindCollectionWithSortIntegrationTest extends AbstractCollectionIntegrationTestBase {
  private static final int DEFAULT_PAGE_SIZE = OperationsConfig.DEFAULT_PAGE_SIZE;

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindCollectionOperationWithSort {
    // should be static, since UUID should not be generated multiple times across all test methods
    private static final List<Object> testDatas = getDocuments(DEFAULT_PAGE_SIZE + 5);

    @Test
    @Order(1)
    public void setUp() {
      Collections.shuffle(testDatas);
      insert(collectionName, testDatas);
    }

    @Test
    public void sortByTextAndNullValue() throws Exception {
      sortByUserName(testDatas, true);
      final ArrayNode arrayNode = testDataArray(testDatas, 0, DEFAULT_PAGE_SIZE);
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "sort" : {"username" : 1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(DEFAULT_PAGE_SIZE))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortWithSkipLimit() throws Exception {
      sortByUserName(testDatas, true);
      final ArrayNode arrayNode = testDataArray(testDatas, 10, 10);

      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "sort" : {"username" : 1},
                          "options" : {"skip": 10, "limit" : 10}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(10))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortDescendingTextValue() throws Exception {
      sortByUserName(testDatas, false);
      final ArrayNode arrayNode = testDataArray(testDatas, 0, DEFAULT_PAGE_SIZE);

      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "sort" : {"username" : -1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(DEFAULT_PAGE_SIZE))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortBooleanValueAndMissing() throws Exception {
      sortByActiveUser(testDatas, true);
      final ArrayNode arrayNode = testDataArray(testDatas, 0, DEFAULT_PAGE_SIZE);
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "sort" : {"activeUser" : 1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(DEFAULT_PAGE_SIZE))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortBooleanValueAndMissingDescending() throws Exception {
      sortByActiveUser(testDatas, false);
      final ArrayNode arrayNode = testDataArray(testDatas, 0, DEFAULT_PAGE_SIZE);
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "sort" : {"activeUser" : -1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(DEFAULT_PAGE_SIZE))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortNumericField() throws Exception {
      sortByUserId(testDatas, true);
      final ArrayNode arrayNode = testDataArray(testDatas, 0, DEFAULT_PAGE_SIZE);
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "sort" : {"userId" : 1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(DEFAULT_PAGE_SIZE))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortNumericFieldDescending() throws Exception {
      sortByUserId(testDatas, false);
      final ArrayNode arrayNode = testDataArray(testDatas, 0, DEFAULT_PAGE_SIZE);
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "sort" : {"userId" : -1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(DEFAULT_PAGE_SIZE))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortNumericFieldAndFilter() throws Exception {
      List<Object> datas =
          testDatas.stream()
              .filter(obj -> (obj instanceof TestData o) && o.activeUser())
              .collect(Collectors.toList());
      sortByUserId(datas, true);
      final ArrayNode arrayNode = testDataArray(datas, 0, datas.size());

      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "filter" : {"activeUser" : true},
                          "sort" : {"userId" : 1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(Math.min(DEFAULT_PAGE_SIZE, datas.size())))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortMultiColumns() throws Exception {
      sortByUserNameUserId(testDatas, true, true);
      final ArrayNode arrayNode = testDataArray(testDatas, 0, DEFAULT_PAGE_SIZE);
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "sort" : {"username" : 1, "userId" : 1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(DEFAULT_PAGE_SIZE))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortMultiColumnsMixedOrder() throws Exception {
      List<Object> datas =
          testDatas.stream()
              .filter(obj -> (obj instanceof TestData o) && o.activeUser())
              .collect(Collectors.toList());
      sortByUserNameUserId(datas, true, false);
      final ArrayNode arrayNode = testDataArray(datas, 0, datas.size());
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "filter" : {"activeUser" : true},
                          "sort" : {"username" : 1, "userId" : -1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(Math.min(DEFAULT_PAGE_SIZE, datas.size())))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortByDate() throws Exception {
      List<Object> datas =
          testDatas.stream()
              .filter(obj -> (obj instanceof TestData o) && o.activeUser())
              .collect(Collectors.toList());
      sortByDate(datas, true);
      final ArrayNode arrayNode = testDataArray(datas, 0, datas.size());
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "filter" : {"activeUser" : true},
                          "sort" : {"dateValue" : 1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(Math.min(DEFAULT_PAGE_SIZE, datas.size())))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    @Test
    public void sortByDateDescending() throws Exception {
      List<Object> datas =
          testDatas.stream()
              .filter(obj -> (obj instanceof TestData o) && o.activeUser())
              .collect(Collectors.toList());
      sortByDate(datas, false);
      final ArrayNode arrayNode = testDataArray(datas, 0, datas.size());
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "filter" : {"activeUser" : true},
                          "sort" : {"dateValue" : -1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(Math.min(DEFAULT_PAGE_SIZE, datas.size())))
          .body("data.documents", jsonEquals(arrayNode.toString()));
    }

    // sort by uuid v6, same for v7
    @Test
    public void sortByUUID() throws Exception {
      List<Object> datas =
          testDatas.stream()
              .filter(obj -> (obj instanceof TestData o))
              .collect(Collectors.toList());
      sortByUUID(datas, true);
      // Create a sublist of the first page.
      List<Object> firstPageDatas =
          new ArrayList<>(datas.subList(0, Math.min(DEFAULT_PAGE_SIZE, datas.size())));
      final ArrayNode arrayNode = testDataArray(firstPageDatas, 0, firstPageDatas.size());
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "filter":{
                             "uuid" : {"$exists" : true}
                           },
                           "sort" : {"uuid" : 1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(Math.min(DEFAULT_PAGE_SIZE, firstPageDatas.size())))
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

    private void sortByUUID(List<Object> testDatas, boolean asc) {
      testDatas.sort(
          new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
              JsonNode o1j = getUUIDAsJsonNode(o1);
              JsonNode o2j = getUUIDAsJsonNode(o2);
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
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindCollectionOperationWithFailingSort {
    static final String biggerCollectionName =
        "col_fail_" + RandomStringUtils.insecure().nextAlphanumeric(16);

    // Test limit is max 100, create couple more
    private static final List<Object> testDatas = getDocuments(110);

    @Test
    @Order(1)
    public void setUp() {
      createSimpleCollection(biggerCollectionName);
      Collections.shuffle(testDatas);
      insert(biggerCollectionName, testDatas);
    }

    @Test
    public void sortFailDueToTooMany() {
      givenHeadersPostJsonThenOk(
              keyspaceName,
              biggerCollectionName,
              """
                      {
                        "find": {
                          "sort" : {"username" : 1}
                        }
                      }
                      """)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is(SortException.Code.OVERLOADED_SORT_ROW_LIMIT.name()))
          .body(
              "errors[0].message",
              startsWith("The command used in-memory sorting which has a limit of 100 documents"));
    }
  }

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static ArrayNode testDataArray(List<Object> datas, int offset, int limit)
      throws JsonProcessingException {
    final ArrayNode arrayNode = objectMapper.getNodeFactory().arrayNode(limit);
    for (int i = offset; i < offset + limit; i++) {
      arrayNode.add(
          objectMapper.readTree(
              objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(datas.get(i))));
    }
    return arrayNode;
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

  private JsonNode getUUIDAsJsonNode(Object data) {
    if (data instanceof TestData td) {
      if (td.uuid() == null) return objectMapper.getNodeFactory().nullNode();
      return objectMapper.getNodeFactory().textNode(td.uuid.$uuid());
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

  private static List<Object> getDocuments(int countOfDocuments) {
    List<Object> data = new ArrayList<>(countOfDocuments);
    for (int docId = 1; docId <= countOfDocuments - 3; docId++) {
      data.add(
          new TestData(
              "doc" + docId,
              // generate uuid v6
              new UuidValue(Generators.timeBasedReorderedGenerator().generate().toString()),
              "user" + docId,
              docId,
              docId % 2 == 0,
              new DateValue(1672531200000L + docId * 1000)));
    }
    data.add(
        new TestData(
            "doc" + (countOfDocuments - 2),
            new UuidValue(Generators.timeBasedReorderedGenerator().generate().toString()),
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
        new TestDataUserIdAsText("doc" + (countOfDocuments), "user" + (countOfDocuments), "" + 1));
    return data;
  }

  private void insert(String collectionName, List<Object> testDatas) {
    testDatas.forEach(
        testData -> {
          String json;
          try {
            json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(testData);
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
          insertDoc(collectionName, json);
        });
  }

  record TestData(
      String _id,
      UuidValue uuid,
      String username,
      int userId,
      boolean activeUser,
      DateValue dateValue) {}

  record TestDataMissingBoolean(String _id, String username, int userId) {}

  record TestDataUserIdAsText(String _id, String username, String userId) {}

  record UuidValue(String $uuid) {}

  record DateValue(long $date) {}
}
