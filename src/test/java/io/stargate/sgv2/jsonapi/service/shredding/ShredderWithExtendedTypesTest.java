package io.stargate.sgv2.jsonapi.service.shredding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.JsonExtensionType;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import jakarta.inject.Inject;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Separate tests to check that handling of "Extended JSON" types (see JsonExtensionType} is working
 * as expected.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ShredderWithExtendedTypesTest {
  @Inject ObjectMapper objectMapper;

  @Inject Shredder shredder;
  @InjectMock protected DataApiRequestInfo bogusRequestInfo;

  @Nested
  class OkCasesId {
    @Test
    public void shredSimpleWithUUIDKeyAndValue() throws Exception {
      final String idUUID = defaultTestUUID().toString();
      final String valueUUID = defaultTestUUID2().toString();
      final String inputJson =
          """
                      { "_id" : {"$uuid": "%s"},
                        "name" : "Bob",
                        "extraId" : {"$uuid": "%s"}
                      }
                      """
              .formatted(idUUID, valueUUID);
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      WritableShreddedDocument doc = shredder.shred(inputDoc);

      assertThat(doc.id())
          .isEqualTo(
              DocumentId.fromExtensionType(
                  JsonExtensionType.UUID, objectMapper.getNodeFactory().textNode(idUUID)));
      List<JsonPath> expPaths =
          Arrays.asList(JsonPath.from("_id"), JsonPath.from("name"), JsonPath.from("extraId"));

      // First verify paths
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

      assertThat(doc.arraySize()).isEmpty();

      // We have 2 from array, plus 3 main level properties (_id excluded)
      assertThat(doc.arrayContains())
          .containsExactlyInAnyOrder(
              "name SBob", "extraId " + new DocValueHasher().getHash(valueUUID).hash());

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNumberValues()).isEmpty();
      assertThat(doc.queryTextValues())
          .isEqualTo(
              Map.of(
                  JsonPath.from("_id"),
                  idUUID,
                  JsonPath.from("name"),
                  "Bob",
                  JsonPath.from("extraId"),
                  valueUUID));
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryVectorValues()).isNull();
    }

    @Test
    public void shredSimpleWithObjectIdKeyAndValue() throws Exception {
      final String idObjectId = defaultTestObjectId().toString();
      final String valueObjectId = defaultTestObjectId2().toString();
      final String inputJson =
          """
                          { "_id" : {"$objectId": "%s"},
                            "name" : "Bob",
                            "objectId2" : {"$objectId": "%s"}
                          }
                          """
              .formatted(idObjectId, valueObjectId);
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      WritableShreddedDocument doc = shredder.shred(inputDoc);

      assertThat(doc.id())
          .isEqualTo(
              DocumentId.fromExtensionType(
                  JsonExtensionType.OBJECT_ID, objectMapper.getNodeFactory().textNode(idObjectId)));
      List<JsonPath> expPaths =
          Arrays.asList(JsonPath.from("_id"), JsonPath.from("name"), JsonPath.from("objectId2"));

      // First verify paths
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

      assertThat(doc.arraySize()).isEmpty();

      // We have 2 from array, plus 3 main level properties (_id excluded)
      assertThat(doc.arrayContains())
          .containsExactlyInAnyOrder(
              "name SBob", "objectId2 " + new DocValueHasher().getHash(valueObjectId).hash());

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNumberValues()).isEmpty();
      assertThat(doc.queryTextValues())
          .isEqualTo(
              Map.of(
                  JsonPath.from("_id"),
                  idObjectId,
                  JsonPath.from("name"),
                  "Bob",
                  JsonPath.from("objectId2"),
                  valueObjectId));
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryVectorValues()).isNull();
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
  }

  @Nested
  class ErrorCasesDocId {
    @Test
    public void docInvalidObjectAsDocId() {
      final String inputJson =
          """
                              { "_id" : {"$objectId": "not-an-oid"},
                                "name" : "Bob"
                              }
                              """;
      Throwable t = catchThrowable(() -> shredder.shred(objectMapper.readTree(inputJson)));

      assertThat(t)
          .isNotNull()
          .hasMessage(
              ErrorCode.SHRED_BAD_EJSON_VALUE.getMessage()
                  + ": invalid value (\"not-an-oid\") for extended JSON type '$objectId' (path '_id')")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_EJSON_VALUE);
    }

    @Test
    public void docInvalidUUIDAsDocId() {
      // First, invalid String
      final String inputJson =
          """
                          { "_id" : {"$uuid": "not-a-uuid"},
                            "name" : "Bob"
                          }
                          """;
      Throwable t = catchThrowable(() -> shredder.shred(objectMapper.readTree(inputJson)));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_EJSON_VALUE)
          .hasMessage(
              ErrorCode.SHRED_BAD_EJSON_VALUE.getMessage()
                  + ": invalid value (\"not-a-uuid\") for extended JSON type '$uuid' (path '_id')");

      // second: JSON Object also not valid UUID representation
      t =
          catchThrowable(
              () -> shredder.shred(objectMapper.readTree("{ \"_id\" : {\"$uuid\": { } } }")));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_DOCID_TYPE)
          .hasMessage(
              ErrorCode.SHRED_BAD_DOCID_TYPE.getMessage()
                  + ": Extension type '$uuid' must have JSON String as value: instead got OBJECT");
    }

    @Test
    public void docUnknownEJSonAsId() {
      final String inputJson =
          """
                                  { "_id" : {"$unknown": "value"} }
                                  """;
      Throwable t = catchThrowable(() -> shredder.shred(objectMapper.readTree(inputJson)));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_DOCID_TYPE)
          .hasMessage(
              ErrorCode.SHRED_BAD_DOCID_TYPE.getMessage()
                  + ": unrecognized JSON extension type '$unknown'");
    }
  }

  @Nested
  class ErrorCasesValue {
    @Test
    public void docBadObjectIdAsValue() {
      Throwable t =
          catchThrowable(
              () ->
                  shredder.shred(
                      objectMapper.readTree("{ \"value\": { \"$objectId\": \"abc\" } }")));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_EJSON_VALUE)
          .hasMessageStartingWith(
              ErrorCode.SHRED_BAD_EJSON_VALUE.getMessage()
                  + ": invalid value (\"abc\") for extended JSON type '$objectId' (path 'value')");
    }

    @Test
    public void docBadUUIDAsValue() {
      Throwable t =
          catchThrowable(
              () ->
                  shredder.shred(
                      objectMapper.readTree("{ \"value\": { \"$uuid\": \"foobar\" } }")));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_EJSON_VALUE)
          .hasMessageStartingWith(
              ErrorCode.SHRED_BAD_EJSON_VALUE.getMessage()
                  + ": invalid value (\"foobar\") for extended JSON type '$uuid' (path 'value')");
    }

    @Test
    public void docUnknownEJsonAsValue() {
      Throwable t =
          catchThrowable(
              () ->
                  shredder.shred(
                      objectMapper.readTree("{ \"value\": { \"$unknownType\": 123 } }")));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION)
          .hasMessageStartingWith(
              ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION.getMessage()
                  + ": property name ('$unknownType') contains character(s) not allowed");
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

  protected UUID defaultTestUUID() {
    return UUID.fromString("128a5eb5-008a-4e4e-ac1f-0d4255f00f61");
  }

  protected UUID defaultTestUUID2() {
    return UUID.fromString("8a251cdc-624e-4b10-a290-1aaad0bb57d0");
  }

  protected ObjectId defaultTestObjectId() {
    return new ObjectId("1234567890abcdef12345678");
  }

  protected ObjectId defaultTestObjectId2() {
    return new ObjectId("1234567890abcdef87654321");
  }
}
