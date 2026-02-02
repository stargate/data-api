package io.stargate.sgv2.jsonapi.service.shredding;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.*;
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
public class DocumentShredderTest {
  @Inject ObjectMapper objectMapper;

  @Inject DocumentShredder documentShredder;
  @InjectMock protected RequestContext dataApiRequestInfo;

  private final TestConstants testConstants = new TestConstants();

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
      WritableShreddedDocument doc = documentShredder.shred(commandContext(), inputDoc, null);
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
      WritableShreddedDocument doc = documentShredder.shred(commandContext(), inputDoc, null);

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
      WritableShreddedDocument doc = documentShredder.shred(commandContext(), inputDoc, null);
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
      WritableShreddedDocument doc = documentShredder.shred(commandContext(), inputDoc, null);
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
      WritableShreddedDocument doc =
          documentShredder.shred(commandContext(), fromJson(inputJson), null);
      assertThat(doc.id()).isEqualTo(DocumentId.fromNumber(new BigDecimal(30L)));
      // Verify that we do NOT have '{"_id":3E+1}':
      assertThat(doc.docJson()).isEqualTo(inputJson);
    }

    @Test
    public void shredOverlappingPaths() throws JsonProcessingException {
      final String inputJson =
          """
              {
                "_id" : "doc1",
                "price": {
                  "usd": 5
                },
                "price.usd": 8.5
              }
              """;
      WritableShreddedDocument doc =
          documentShredder.shred(commandContext(), fromJson(inputJson), null);

      List<JsonPath> expPaths =
          Arrays.asList(
              JsonPath.from("_id"),
              JsonPath.from("price"),
              JsonPath.from("price.usd"),
              JsonPath.from("price&.usd"));

      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));
      assertThat(doc.arraySize()).isEmpty();
      // 2 non-doc-id main-level properties with hashes:
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("price.usd N5", "price&.usd N8.5");

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryNumberValues())
          .isEqualTo(
              Map.of(
                  JsonPath.from("price&.usd"),
                  BigDecimal.valueOf(8.5),
                  JsonPath.from("price.usd"),
                  BigDecimal.valueOf(5)));
      assertThat(doc.queryTextValues())
          .isEqualTo(Map.of(JsonPath.from("_id"), "doc1", JsonPath.from("price"), "O1\nusd\nN5"));

      // the doc_json should not have the escape character
      final JsonNode inputDoc = fromJson(inputJson);
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);
    }
  }

  @Nested
  class LexicalOkCases {
    @Test
    void simpleLexicalString() throws Exception {
      final String inputJson =
          """
                          { "_id" : "lex1",
                            "$lexical": "bag of words",
                            "value": 3
                          }
                          """;
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      WritableShreddedDocument doc = documentShredder.shred(commandContext(), inputDoc, null);
      assertThat(doc.id()).isEqualTo(DocumentId.fromString("lex1"));
      List<JsonPath> expPaths =
          Arrays.asList(JsonPath.from("_id"), JsonPath.from("$lexical"), JsonPath.from("value"));

      // First verify paths
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("value N3");

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      assertThat(doc.queryBoolValues()).isEmpty();
      Map<JsonPath, BigDecimal> expNums =
          Map.of(JsonPath.from("value", false), BigDecimal.valueOf(3));
      assertThat(doc.queryNumberValues()).isEqualTo(expNums);
      assertThat(doc.queryTextValues()).isEqualTo(Map.of(JsonPath.from("_id"), "lex1"));
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryVectorValues()).isNull();
      assertThat(doc.queryLexicalValue()).isEqualTo("bag of words");
    }

    @Test
    void simpleLexicalNull() throws Exception {
      final String inputJson =
          """
                          { "_id" : "lex-null",
                            "$lexical": null
                          }
                          """;
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      WritableShreddedDocument doc = documentShredder.shred(commandContext(), inputDoc, null);
      assertThat(doc.id()).isEqualTo(DocumentId.fromString("lex-null"));

      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(Arrays.asList(JsonPath.from("_id"))));

      assertThat(doc.arrayContains()).isEmpty();

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNumberValues()).isEmpty();
      assertThat(doc.queryTextValues()).isEqualTo(Map.of(JsonPath.from("_id"), "lex-null"));
      // $lexical not like everything else hence:
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryVectorValues()).isNull();
      assertThat(doc.queryLexicalValue()).isNull();
    }
  }

  @Nested
  class LexicalFailCases {
    @Test
    void badLexicalObject() throws Exception {
      final String inputJson =
          """
                      { "_id" : "lex1",
                        "$lexical": { "value": "bag of words" }
                      }
                      """;
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      Throwable t = catchThrowable(() -> documentShredder.shred(commandContext(), inputDoc, null));
      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue(
              "code", DocumentException.Code.SHRED_BAD_DOCUMENT_LEXICAL_TYPE.name())
          .hasMessageContaining(
              "the value for field '$lexical' must be a JSON String, not a JSON Object");
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
      WritableShreddedDocument doc = documentShredder.shred(commandContext(), inputDoc, null);
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
              () ->
                  documentShredder.shred(
                      commandContext(),
                      objectMapper.readTree("{ \"date\": { \"$date\": false } }"),
                      null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("code", DocumentException.Code.SHRED_BAD_EJSON_VALUE.name())
          .hasMessageContaining(
              "Bad JSON Extension value to shred: type '$date' has invalid JSON value of type Boolean");
    }

    @Test
    public void badEmptyVectorData() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(), objectMapper.readTree("{ \"$vector\": [] }"), null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("code", DocumentException.Code.SHRED_BAD_VECTOR_SIZE.name())
          .hasMessageContaining("Bad $vector value: cannot be empty Array");
    }

    @Test
    public void badInvalidVectorData() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(),
                      objectMapper.readTree("{ \"$vector\": [0.11, \"abc\"] }"),
                      null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("code", DocumentException.Code.SHRED_BAD_VECTOR_VALUE.name())
          .hasMessageContaining(
              "Bad $vector value: needs to be an array containing only Numbers but has a String value (\"abc\")");
    }

    @Test
    public void badEJSONUnrecognized() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(),
                      objectMapper.readTree("{ \"value\": { \"$unknownType\": 123 } }"),
                      null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("code", DocumentException.Code.SHRED_BAD_FIELD_NAME.name())
          .hasMessageContaining(
              "Document field name not valid: field name '$unknownType' starts with '$'");
    }
  }

  @Nested
  class ErrorCases {

    @Test
    public void docBadJSONType() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(), objectMapper.readTree("[ 1, 2 ]"), null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue(
              "code", DocumentException.Code.SHRED_BAD_DOCUMENT_TYPE.name())
          .hasMessageContaining(
              "Bad document type to shred: document must be a JSON Object, instead got a JSON Array");
    }

    @Test
    public void docBadDocIdTypeArray() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(), objectMapper.readTree("{ \"_id\" : [ ] }"), null));

      assertThat(t)
          .hasFieldOrPropertyWithValue("code", DocumentException.Code.SHRED_BAD_DOCID_TYPE.name())
          .hasMessageContaining(
              "Bad type for '_id' field: Document Id must be a JSON String, Number, Boolean, EJSON-Encoded Date Object or null instead got Array: [].");
    }

    @Test
    public void docBadDocIdTypeObjectNotEJSON() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(),
                      objectMapper.readTree("{ \"_id\" : { \"foo\": \"bar\" } }"),
                      null));

      assertThat(t)
          .hasFieldOrPropertyWithValue("code", DocumentException.Code.SHRED_BAD_DOCID_TYPE.name())
          .hasMessageContaining(
              "Bad type for '_id' field: Document Id must be a JSON String, Number, Boolean, EJSON-Encoded Date Object or null instead got Object: {\"foo\":\"bar\"}.");
    }

    @Test
    public void docBadDocIdEmptyString() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(), objectMapper.readTree("{ \"_id\" : \"\" }"), null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("code", DocumentException.Code.SHRED_BAD_DOCID_VALUE.name())
          .hasMessageContaining("Bad value for '_id' field: empty String not allowed");
    }

    @Test
    public void docBadFieldNameRootLeadingDollar() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(), objectMapper.readTree("{ \"$id\" : 42 }"), null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("code", DocumentException.Code.SHRED_BAD_FIELD_NAME.name())
          .hasMessageContaining("Document field name not valid: field name '$id' starts with '$'");
    }

    @Test
    public void docBadFieldNameNestedLeadingDollar() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(),
                      objectMapper.readTree("{ \"price\": { \"$usd\" : 42.0 } }"),
                      null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("code", DocumentException.Code.SHRED_BAD_FIELD_NAME.name())
          .hasMessageContaining("Document field name not valid: field name '$usd' starts with '$'");
    }

    @Test
    public void docBadFieldNameEmptyRoot() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(), objectMapper.readTree("{ \"\" : 1972 }"), null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("code", DocumentException.Code.SHRED_BAD_FIELD_NAME.name())
          .hasMessageContaining("Document field name not valid: field name '' is empty");
    }

    @Test
    public void docBadFieldNameEmptyNested() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(),
                      objectMapper.readTree("{ \"price\": { \"\" : false } }"),
                      null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("code", DocumentException.Code.SHRED_BAD_FIELD_NAME.name())
          .hasMessageContaining("Document field name not valid: field name '' is empty");
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
          documentShredder.shred(
              inputDoc, null, indexProjector, "testCommand", CollectionSchemaObject.MISSING, null);
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
          documentShredder.shred(
              inputDoc, null, indexProjector, "testCommand", CollectionSchemaObject.MISSING, null);
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
          documentShredder.shred(
              inputDoc, null, indexProjector, "testCommand", CollectionSchemaObject.MISSING, null);
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
          documentShredder.shred(
              inputDoc, null, indexProjector, "testCommand", CollectionSchemaObject.MISSING, null);
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
          documentShredder.shred(
              inputDoc, null, indexProjector, "testCommand", CollectionSchemaObject.MISSING, null);
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
          documentShredder.shred(
              inputDoc, null, indexProjector, "testCommand", CollectionSchemaObject.MISSING, null);
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
      documentShredder.shred(
          inputDoc,
          null,
          IndexingProjector.identityProjector(),
          "jsonBytesWriteCommand",
          CollectionSchemaObject.MISSING,
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

  private CommandContext<CollectionSchemaObject> commandContext() {
    return testConstants.collectionContext();
  }
}
