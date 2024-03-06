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
    public void shredSimpleWithId() throws Exception {
      final String inputJson =
          """
                      { "_id" : {"$uuid": "%s"},
                        "name" : "Bob"
                      }
                      """
              .formatted(defaultTestUUID());
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      final String idUUID = defaultTestUUID().toString();
      WritableShreddedDocument doc = shredder.shred(inputDoc);

      assertThat(doc.id())
          .isEqualTo(
              DocumentId.fromExtensionType(
                  JsonExtensionType.UUID, objectMapper.getNodeFactory().textNode(idUUID)));
      List<JsonPath> expPaths = Arrays.asList(JsonPath.from("_id"), JsonPath.from("name"));

      // First verify paths
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

      assertThat(doc.arraySize()).isEmpty();

      // We have 2 from array, plus 3 main level properties (_id excluded)
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("name SBob");

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNumberValues()).isEmpty();
      assertThat(doc.queryTextValues())
          .isEqualTo(Map.of(JsonPath.from("_id"), idUUID, JsonPath.from("name"), "Bob"));
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
          .hasMessage(
              "Bad EJSON value: Date ($date) needs to have NUMBER value, has BOOLEAN (path 'date')")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_EJSON_VALUE);
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
    public void badEJSONUnrecognized() {
      Throwable t =
          catchThrowable(
              () ->
                  shredder.shred(
                      objectMapper.readTree("{ \"value\": { \"$unknownType\": 123 } }")));

      assertThat(t)
          .isNotNull()
          .hasMessageStartingWith("Document key name constraints violated")
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
}
