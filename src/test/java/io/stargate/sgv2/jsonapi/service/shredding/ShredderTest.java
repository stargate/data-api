package io.stargate.sgv2.jsonapi.service.shredding;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ShredderTest {
  @Inject ObjectMapper objectMapper;

  @Inject Shredder shredder;
  @InjectMock protected DataApiRequestInfo dataApiRequestInfo;

  @Nested
  class OkCases {
    @Test
    public void shredSimpleWithId() throws Exception {
      final String inputJson =
          """
                      { "_id" : "abc",
                        "name" : "Bob",
                        "values" : [ 1, 2 ],
                        "extra_stuff" : true,
                        "nullable" : null,
                        "$vector" : [ 0.11, 0.22, 0.33, 0.44 ]
                      }
                      """;
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      WritableShreddedDocument doc = shredder.shred(inputDoc);
      assertThat(doc.id()).isEqualTo(DocumentId.fromString("abc"));
      List<JsonPath> expPaths =
          Arrays.asList(
              JsonPath.from("_id"),
              JsonPath.from("name"),
              JsonPath.from("values"),
              JsonPath.from("values.0", true),
              JsonPath.from("values.1", true),
              JsonPath.from("extra_stuff"),
              JsonPath.from("nullable"),
              JsonPath.from("$vector"));

      // First verify paths
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

      // Then array info (doc has one array, with 2 elements)
      assertThat(doc.arraySize())
          .hasSize(1)
          .containsEntry(JsonPath.from("values"), Integer.valueOf(2));

      // We have 2 from array, plus 3 main level properties (_id excluded)
      assertThat(doc.arrayContains()).hasSize(7);
      assertThat(doc.arrayContains())
          .containsExactlyInAnyOrder(
              "name SBob",
              "values N1",
              "values N2",
              "extra_stuff B1",
              "nullable Z",
              "values.0 N1",
              "values.1 N2");

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      // Then atomic value containers
      assertThat(doc.queryBoolValues())
          .isEqualTo(Collections.singletonMap(JsonPath.from("extra_stuff"), Boolean.TRUE));
      Map<JsonPath, BigDecimal> expNums = new LinkedHashMap<>();
      expNums.put(JsonPath.from("values.0", true), BigDecimal.valueOf(1));
      expNums.put(JsonPath.from("values.1", true), BigDecimal.valueOf(2));
      assertThat(doc.queryNumberValues()).isEqualTo(expNums);
      assertThat(doc.queryTextValues())
          .isEqualTo(
              Map.of(
                  JsonPath.from("_id"),
                  "abc",
                  JsonPath.from("name"),
                  "Bob",
                  JsonPath.from("values"),
                  new DocValueHasher()
                      .getHash(List.of(new BigDecimal(1), new BigDecimal(2)))
                      .hash()));
      assertThat(doc.queryNullValues()).isEqualTo(Collections.singleton(JsonPath.from("nullable")));
      float[] vector = {0.11f, 0.22f, 0.33f, 0.44f};
      assertThat(doc.queryVectorValues()).containsOnly(0.11f, 0.22f, 0.33f, 0.44f);
    }

    @Test
    public void shredSimpleWithoutId() throws Exception {
      final String inputJson =
          """
                      {
                        "age" : 39,
                        "name" : "Chuck"
                      }
                      """;
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      WritableShreddedDocument doc = shredder.shred(inputDoc);

      assertThat(doc.id()).isInstanceOf(DocumentId.StringId.class);
      // should be auto-generated UUID:
      assertThat(UUID.fromString(doc.id().asDBKey())).isNotNull();
      List<JsonPath> expPaths =
          Arrays.asList(JsonPath.from("_id"), JsonPath.from("age"), JsonPath.from("name"));

      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));
      assertThat(doc.arraySize()).isEmpty();
      // 2 non-doc-id main-level properties with hashes:
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("age N39", "name SChuck");

      // Also, the document should be the same, including _id added:
      ObjectNode jsonFromShredded = (ObjectNode) objectMapper.readTree(doc.docJson());
      JsonNode idNode = jsonFromShredded.get("_id");
      assertThat(idNode).isNotNull();
      String generatedId = idNode.textValue();
      assertThat(generatedId).isEqualTo(doc.id().asDBKey());
      ((ObjectNode) inputDoc).put("_id", generatedId);
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryNumberValues())
          .isEqualTo(Map.of(JsonPath.from("age"), BigDecimal.valueOf(39)));
      assertThat(doc.queryTextValues())
          .isEqualTo(Map.of(JsonPath.from("_id"), generatedId, JsonPath.from("name"), "Chuck"));
    }

    @Test
    public void shredSimpleWithBooleanId() throws Exception {
      final String inputJson =
          """
                      { "_id" : true,
                        "name" : "Bob"
                      }
                      """;
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      WritableShreddedDocument doc = shredder.shred(inputDoc);
      assertThat(doc.id()).isEqualTo(DocumentId.fromBoolean(true));

      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      assertThat(doc.arraySize()).isEmpty();
      // 1 non-doc-id main-level property
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("name SBob");

      assertThat(doc.queryBoolValues()).isEqualTo(Map.of(JsonPath.from("_id"), Boolean.TRUE));
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryNumberValues()).isEmpty();
      assertThat(doc.queryTextValues()).isEqualTo(Map.of(JsonPath.from("name"), "Bob"));
    }

    @Test
    public void shredSimpleWithNumberId() throws Exception {
      final String inputJson =
          """
                      { "_id" : 123,
                        "name" : "Bob"
                      }
                      """;
      final JsonNode inputDoc = fromJson(inputJson);
      WritableShreddedDocument doc = shredder.shred(inputDoc);
      assertThat(doc.id()).isEqualTo(DocumentId.fromNumber(new BigDecimal(123L)));

      JsonNode jsonFromShredded = fromJson(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      assertThat(doc.arraySize()).isEmpty();
      // 1 non-doc-id main-level property
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("name SBob");

      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryNumberValues())
          .isEqualTo(Map.of(JsonPath.from("_id"), new BigDecimal(123L)));
      assertThat(doc.queryTextValues()).isEqualTo(Map.of(JsonPath.from("name"), "Bob"));
    }

    // [json-api#210]: accidental use of Engineering notation with trailing zeroes
    @Test
    public void shredSimpleWithNumberIdWithTrailingZeroes() {
      final String inputJson = "{\"_id\":30}";
      WritableShreddedDocument doc = shredder.shred(fromJson(inputJson));
      assertThat(doc.id()).isEqualTo(DocumentId.fromNumber(new BigDecimal(30L)));
      // Verify that we do NOT have '{"_id":3E+1}':
      assertThat(doc.docJson()).isEqualTo(inputJson);
    }
  }

  @Nested
  class EJSONDateTime {
    @Test
    public void shredDocWithDateTimeColumn() {
      final long testTimestamp = defaultTestDate().getTime();
      final String inputJson =
              """
              {
                "_id" : 123,
                "name" : "Bob",
                "datetime" : {
                  "$date" : %d
                }
              }
              """
              .formatted(testTimestamp);
      final JsonNode inputDoc = fromJson(inputJson);
      WritableShreddedDocument doc = shredder.shred(inputDoc);
      assertThat(doc.id()).isEqualTo(DocumentId.fromNumber(new BigDecimal(123L)));

      JsonNode jsonFromShredded = fromJson(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      assertThat(doc.arraySize()).isEmpty();
      // 2 non-doc-id main-level properties
      assertThat(doc.arrayContains())
          .containsExactlyInAnyOrder("name SBob", "datetime T" + testTimestamp);

      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryNumberValues())
          .isEqualTo(Map.of(JsonPath.from("_id"), new BigDecimal(123L)));
      assertThat(doc.queryTextValues()).isEqualTo(Map.of(JsonPath.from("name"), "Bob"));
      assertThat(doc.queryTimestampValues())
          .isEqualTo(Map.of(JsonPath.from("datetime"), new Date(testTimestamp)));
    }

    @Test
    public void badEJSONDate() {
      Throwable t =
          catchThrowable(
              () -> shredder.shred(objectMapper.readTree("{ \"date\": { \"$date\": false } }")));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_EJSON_VALUE)
          .hasMessage(
              ErrorCode.SHRED_BAD_EJSON_VALUE.getMessage()
                  + ": type '$date' has invalid JSON value of type BOOLEAN");
    }

    @Test
    public void badEmptyVectorData() {
      Throwable t =
          catchThrowable(() -> shredder.shred(objectMapper.readTree("{ \"$vector\": [] }")));

      assertThat(t)
          .isNotNull()
          .hasMessage("$vector value can't be empty")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_VECTOR_SIZE);
    }

    @Test
    public void badInvalidVectorData() {
      Throwable t =
          catchThrowable(
              () -> shredder.shred(objectMapper.readTree("{ \"$vector\": [0.11, \"abc\"] }")));

      assertThat(t)
          .isNotNull()
          .hasMessage("$vector value needs to be array of numbers")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_VECTOR_VALUE);
    }

    @Test
    public void badEJSONUnrecognized() {
      Throwable t =
          catchThrowable(
              () ->
                  shredder.shred(
                      objectMapper.readTree("{ \"value\": { \"$unknownType\": 123 } }")));

      assertThat(t)
          .isNotNull()
          .hasMessageStartingWith(ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION.getMessage())
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION);
    }
  }

  @Nested
  class ErrorCases {

    @Test
    public void docBadJSONType() {
      Throwable t = catchThrowable(() -> shredder.shred(objectMapper.readTree("[ 1, 2 ]")));

      assertThat(t)
          .isNotNull()
          .hasMessage(
              "Bad document type to shred: document to shred must be a JSON Object, instead got ARRAY")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_DOCUMENT_TYPE);
    }

    @Test
    public void docBadDocIdTypeArray() {
      Throwable t =
          catchThrowable(() -> shredder.shred(objectMapper.readTree("{ \"_id\" : [ ] }")));

      assertThat(t)
          .isNotNull()
          .hasMessage(
              "Bad type for '_id' property: Document Id must be a JSON String, Number, Boolean, EJSON-Encoded Date Object or NULL instead got ARRAY")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_DOCID_TYPE);
    }

    @Test
    public void docBadDocIdEmptyString() {
      Throwable t =
          catchThrowable(() -> shredder.shred(objectMapper.readTree("{ \"_id\" : \"\" }")));

      assertThat(t)
          .isNotNull()
          .hasMessage("Bad value for '_id' property: empty String not allowed")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_DOCID_EMPTY_STRING);
    }
  }

  @Nested
  class NoIndexCases {
    @Test
    public void shredWithIndexAllowSome() throws Exception {
      final String inputJson =
          """
                          { "_id" : 123,
                            "name" : "Bob",
                            "values" : [ 1, 2 ],
                            "metadata": {
                               "x": 28,
                               "y" :12
                            },
                            "nullable" : null,
                            "$vector" : [ 0.11, 0.22, 0.33, 0.44 ],
                            "$vectorize" : "some data"
                          }
                          """;
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      IndexingProjector indexProjector =
          IndexingProjector.createForIndexing(
              new HashSet<>(Arrays.asList("name", "metadata")), null);
      WritableShreddedDocument doc =
          shredder.shred(
              inputDoc, null, indexProjector, "testCommand", CollectionSchemaObject.EMPTY, null);
      assertThat(doc.id()).isEqualTo(DocumentId.fromNumber(BigDecimal.valueOf(123)));
      List<JsonPath> expPaths =
          Arrays.asList(
              // NOTE: "$vector" is implicitly added to non-empty "allow" List
              JsonPath.from("$vector"),
              JsonPath.from("$vectorize"),
              JsonPath.from("name"),
              JsonPath.from("metadata"),
              JsonPath.from("metadata.x"),
              JsonPath.from("metadata.y"));

      // First verify paths
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

      // Then array info: nothing, since "values" not included
      assertThat(doc.arraySize()).isEmpty();

      // We have 2 from sub-doc, plus 1 other main level property
      assertThat(doc.arrayContains()).hasSize(3);
      assertThat(doc.arrayContains())
          .containsExactlyInAnyOrder("metadata.x N28", "metadata.y N12", "name SBob");

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      Map<JsonPath, BigDecimal> expNums = new LinkedHashMap<>();
      expNums.put(JsonPath.from("metadata.x"), BigDecimal.valueOf(28));
      expNums.put(JsonPath.from("metadata.y"), BigDecimal.valueOf(12));
      assertThat(doc.queryNumberValues()).isEqualTo(expNums);
      assertThat(doc.queryTextValues())
          .hasSize(2)
          .isEqualTo(
              Map.of(
                  JsonPath.from("name"), "Bob", JsonPath.from("metadata"), "O2\nx\nN28\ny\nN12"));
      assertThat(doc.queryNullValues()).isEmpty();
      float[] vector = {0.11f, 0.22f, 0.33f, 0.44f};
      assertThat(doc.queryVectorValues()).containsOnly(vector);
    }

    @Test
    public void shredVectorize9K() throws Exception {
      char[] arr = new char[9000];
      Arrays.fill(arr, 'A');
      String str = new String(arr);
      final String inputJson =
              """
                              { "_id" : 123,
                                "name" : "Bob",
                                "values" : [ 1, 2 ],
                                "metadata": {
                                   "x": 28,
                                   "y" :12
                                },
                                "nullable" : null,
                                "$vector" : [ 0.11, 0.22, 0.33, 0.44 ],
                                "$vectorize" : "%s"
                              }
                              """
              .formatted(str);
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      IndexingProjector indexProjector =
          IndexingProjector.createForIndexing(
              new HashSet<>(Arrays.asList("name", "metadata")), null);
      WritableShreddedDocument doc =
          shredder.shred(
              inputDoc, null, indexProjector, "testCommand", CollectionSchemaObject.EMPTY, null);
      assertThat(doc.id()).isEqualTo(DocumentId.fromNumber(BigDecimal.valueOf(123)));
      List<JsonPath> expPaths =
          Arrays.asList(
              // NOTE: "$vector" is implicitly added to non-empty "allow" List
              JsonPath.from("$vector"),
              JsonPath.from("$vectorize"),
              JsonPath.from("name"),
              JsonPath.from("metadata"),
              JsonPath.from("metadata.x"),
              JsonPath.from("metadata.y"));

      // First verify paths
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

      // Then array info: nothing, since "values" not included
      assertThat(doc.arraySize()).isEmpty();

      // We have 2 from sub-doc, plus 1 other main level property
      assertThat(doc.arrayContains()).hasSize(3);
      assertThat(doc.arrayContains())
          .containsExactlyInAnyOrder("metadata.x N28", "metadata.y N12", "name SBob");

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      Map<JsonPath, BigDecimal> expNums = new LinkedHashMap<>();
      expNums.put(JsonPath.from("metadata.x"), BigDecimal.valueOf(28));
      expNums.put(JsonPath.from("metadata.y"), BigDecimal.valueOf(12));
      assertThat(doc.queryNumberValues()).isEqualTo(expNums);
      assertThat(doc.queryTextValues())
          .hasSize(2)
          .isEqualTo(
              Map.of(
                  JsonPath.from("name"), "Bob", JsonPath.from("metadata"), "O2\nx\nN28\ny\nN12"));
      assertThat(doc.queryNullValues()).isEmpty();
      float[] vector = {0.11f, 0.22f, 0.33f, 0.44f};
      assertThat(doc.queryVectorValues()).containsOnly(vector);
    }

    @Test
    public void shredWithIndexAllowAll() throws Exception {
      final String inputJson =
          """
                { "_id" : 123,
                  "name" : "Bob",
                  "values" : [ 1, 2 ],
                  "metadata": {
                     "x": 28
                  },
                  "nullable" : null,
                  "$vector" : [ 0.25, -0.5 ]
                }
                """;
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      IndexingProjector indexProjector =
          IndexingProjector.createForIndexing(
              new HashSet<>(Arrays.asList("name", "metadata")), null);
      WritableShreddedDocument doc =
          shredder.shred(
              inputDoc, null, indexProjector, "testCommand", CollectionSchemaObject.EMPTY, null);
      assertThat(doc.id()).isEqualTo(DocumentId.fromNumber(BigDecimal.valueOf(123)));
      List<JsonPath> expPaths =
          Arrays.asList(
              // NOTE: "$vector" is implicitly added to non-empty "allow" List
              JsonPath.from("$vector"),
              JsonPath.from("name"),
              JsonPath.from("metadata"),
              JsonPath.from("metadata.x"));

      // First verify paths
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

      // Then array info: nothing, since "values" not included
      assertThat(doc.arraySize()).isEmpty();

      // We have 2 from sub-doc, plus 1 other main level property
      assertThat(doc.arrayContains()).hasSize(2);
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("metadata.x N28", "name SBob");

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      Map<JsonPath, BigDecimal> expNums = new LinkedHashMap<>();
      expNums.put(JsonPath.from("metadata.x"), BigDecimal.valueOf(28));
      assertThat(doc.queryNumberValues()).isEqualTo(expNums);
      assertThat(doc.queryTextValues())
          .hasSize(2)
          .isEqualTo(Map.of(JsonPath.from("name"), "Bob", JsonPath.from("metadata"), "O1\nx\nN28"));
      assertThat(doc.queryNullValues()).isEmpty();
      float[] vector = {0.25f, -0.5f};
      assertThat(doc.queryVectorValues()).containsOnly(vector);
    }

    @Test
    public void shredWithIndexDenySome() throws Exception {
      final String inputJson =
          """
              { "_id" : 123,
                "name" : "Bob",
                "values" : [ 1, 2 ],
                "metadata": {
                   "x": 28,
                   "y" :12
                },
                "nullable" : null,
                "$vector" : [ 0.11, 0.22, 0.33, 0.44 ],
                "$vectorize" : "sample data"
              }
              """;
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      IndexingProjector indexProjector =
          IndexingProjector.createForIndexing(null, new HashSet<>(Arrays.asList("name", "values")));
      WritableShreddedDocument doc =
          shredder.shred(
              inputDoc, null, indexProjector, "testCommand", CollectionSchemaObject.EMPTY, null);
      assertThat(doc.id()).isEqualTo(DocumentId.fromNumber(BigDecimal.valueOf(123)));
      List<JsonPath> expPaths =
          Arrays.asList(
              JsonPath.from("_id"),
              JsonPath.from("metadata"),
              JsonPath.from("metadata.x"),
              JsonPath.from("metadata.y"),
              JsonPath.from("nullable"),
              JsonPath.from("$vector"),
              JsonPath.from("$vectorize"));

      // First verify paths
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

      // Then array info: nothing, since "values" not included
      assertThat(doc.arraySize()).isEmpty();

      // We have 2 from sub-doc, plus 1 other main level property
      assertThat(doc.arrayContains()).hasSize(3);
      assertThat(doc.arrayContains())
          .containsExactlyInAnyOrder("metadata.x N28", "metadata.y N12", "nullable Z");

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      Map<JsonPath, BigDecimal> expNums = new LinkedHashMap<>();
      expNums.put(JsonPath.from("_id"), BigDecimal.valueOf(123));
      expNums.put(JsonPath.from("metadata.x"), BigDecimal.valueOf(28));
      expNums.put(JsonPath.from("metadata.y"), BigDecimal.valueOf(12));
      assertThat(doc.queryNumberValues()).isEqualTo(expNums);
      assertThat(doc.queryTextValues())
          .hasSize(1)
          .isEqualTo(Map.of(JsonPath.from("metadata"), "O2\nx\nN28\ny\nN12"));
      assertThat(doc.queryNullValues()).hasSize(1).containsExactly(JsonPath.from("nullable"));
      float[] vector = {0.11f, 0.22f, 0.33f, 0.44f};
      assertThat(doc.queryVectorValues()).containsOnly(vector);
    }

    // Test for "index absolutely nothing" setting
    @Test
    public void shredWithIndexDenyAll() throws Exception {
      final String inputJson =
          """
                  { "_id" : 123,
                    "name" : "Bob",
                    "values" : [ 1, 2 ],
                    "metadata": {
                       "x": 28
                    },
                    "nullable" : null,
                    "$vectorize" : "sample data",
                    "$vector" : [ 0.5, 0.25 ]
                  }
                  """;
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      IndexingProjector indexProjector =
          IndexingProjector.createForIndexing(null, new HashSet<>(Arrays.asList("*")));
      WritableShreddedDocument doc =
          shredder.shred(
              inputDoc, null, indexProjector, "testCommand", CollectionSchemaObject.EMPTY, null);
      assertThat(doc.id()).isEqualTo(DocumentId.fromNumber(BigDecimal.valueOf(123)));

      List<JsonPath> expPaths =
          Arrays.asList(JsonPath.from("$vector"), JsonPath.from("$vectorize"));
      // First verify paths
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

      // Then array info: nothing, since "values" not included
      assertThat(doc.arraySize()).isEmpty();

      // We have 2 from sub-doc, plus 1 other main level property
      assertThat(doc.arrayContains()).isEmpty();

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNumberValues()).isEmpty();
      assertThat(doc.queryTextValues()).isEmpty();
      assertThat(doc.queryNullValues()).isEmpty();
      float[] vector = {0.5f, 0.25f};
      assertThat(doc.queryVectorValues()).containsOnly(vector);
    }

    @Test
    public void shredWithHugeNonIndexedString() throws Exception {
      final String hugeString = "abcd".repeat(240_000); // about 960K, close to max doc of 1M
      final String inputJson =
              """
              { "_id": 1,
                "name": "Mo",
                "blob": "%s"
              }
              """
              .formatted(hugeString);

      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      IndexingProjector indexProjector =
          IndexingProjector.createForIndexing(null, new HashSet<>(Arrays.asList("blob")));
      WritableShreddedDocument doc =
          shredder.shred(
              inputDoc, null, indexProjector, "testCommand", CollectionSchemaObject.EMPTY, null);
      assertThat(doc.id()).isEqualTo(DocumentId.fromNumber(BigDecimal.valueOf(1)));
      List<JsonPath> expPaths = Arrays.asList(JsonPath.from("_id"), JsonPath.from("name"));
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));
      assertThat(doc.arraySize()).isEmpty();

      // We have 2 from sub-doc, plus 1 other main level property
      assertThat(doc.arrayContains()).hasSize(1);
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("name SMo");

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNumberValues())
          .isEqualTo(Map.of(JsonPath.from("_id"), BigDecimal.valueOf(1)));
      assertThat(doc.queryTextValues()).isEqualTo(Map.of(JsonPath.from("name"), "Mo"));
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryVectorValues()).isNull();
    }
  }

  @Nested
  class JsonMetricsReporter {
    @Test
    public void validateJsonBytesWriteMetrics() throws Exception {
      final String inputJson =
          """
                      {
                        "name" : "Bob"
                      }
                      """;
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      shredder.shred(
          inputDoc,
          null,
          IndexingProjector.identityProjector(),
          "jsonBytesWriteCommand",
          CollectionSchemaObject.EMPTY,
          null);

      // verify metrics
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> jsonBytesWrittenMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_bytes_written")
                          && !line.startsWith("json_bytes_written_bucket")
                          && !line.contains("quantile")
                          && line.contains("jsonBytesWriteCommand"))
              .toList();
      assertThat(jsonBytesWrittenMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"jsonBytesWriteCommand\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"unknown\"");
                    });
              });
    }
  }

  protected JsonNode fromJson(String json) {
    try {
      return objectMapper.readTree(json);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected Date defaultTestDate() {
    OffsetDateTime dt = OffsetDateTime.parse("2023-01-01T00:00:00Z");
    return new Date(dt.toInstant().toEpochMilli());
  }
}
