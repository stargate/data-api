package io.stargate.sgv2.jsonapi.service.shredding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ShredderTest {
  @Inject ObjectMapper objectMapper;

  @Inject Shredder shredder;

  @Nested
  class OkCases {
    @Test
    public void shredSimpleWithId() throws Exception {
      final String inputJson =
          """
                      { "_id" : "abc",
                        "name" : "Bob",
                        "values" : [ 1, 2 ],
                        "[extra.stuff]" : true,
                        "nullable" : null
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
              JsonPath.from("[extra.stuff]"),
              JsonPath.from("nullable"));

      // First verify paths
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

      // Then array info (doc has one array, with 2 elements)
      assertThat(doc.arraySize())
          .hasSize(1)
          .containsEntry(JsonPath.from("values"), Integer.valueOf(2));
      assertThat(doc.arrayEquals()).hasSize(1);

      // We have 2 from array, plus 3 main level properties (_id excluded)
      assertThat(doc.arrayContains()).hasSize(7);
      assertThat(doc.arrayContains())
          .containsExactlyInAnyOrder(
              "name SBob",
              "values N1",
              "values N2",
              "[extra.stuff] B1",
              "nullable Z",
              "values.0 N1",
              "values.1 N2");

      // Also, the document should be the same, including _id:
      JsonNode jsonFromShredded = objectMapper.readTree(doc.docJson());
      assertThat(jsonFromShredded).isEqualTo(inputDoc);

      // Sub-documents (Object values): none in this example
      assertThat(doc.subDocEquals()).hasSize(0);

      // Then atomic value containers
      assertThat(doc.queryBoolValues())
          .isEqualTo(Collections.singletonMap(JsonPath.from("[extra.stuff]"), Boolean.TRUE));
      Map<JsonPath, BigDecimal> expNums = new LinkedHashMap<>();
      expNums.put(JsonPath.from("values.0", true), BigDecimal.valueOf(1));
      expNums.put(JsonPath.from("values.1", true), BigDecimal.valueOf(2));
      assertThat(doc.queryNumberValues()).isEqualTo(expNums);
      assertThat(doc.queryTextValues())
          .isEqualTo(Map.of(JsonPath.from("_id"), "abc", JsonPath.from("name"), "Bob"));
      assertThat(doc.queryNullValues()).isEqualTo(Collections.singleton(JsonPath.from("nullable")));
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
      assertThat(UUID.fromString(doc.id().toString())).isNotNull();
      List<JsonPath> expPaths =
          Arrays.asList(JsonPath.from("_id"), JsonPath.from("age"), JsonPath.from("name"));

      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));
      assertThat(doc.arraySize()).isEmpty();
      assertThat(doc.arrayEquals()).isEmpty();
      // 2 non-doc-id main-level properties with hashes:
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("age N39", "name SChuck");
      assertThat(doc.subDocEquals()).hasSize(0);

      // Also, the document should be the same, including _id added:
      ObjectNode jsonFromShredded = (ObjectNode) objectMapper.readTree(doc.docJson());
      JsonNode idNode = jsonFromShredded.get("_id");
      assertThat(idNode).isNotNull();
      String generatedId = idNode.textValue();
      assertThat(generatedId).isEqualTo(doc.id().toString());
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
      assertThat(doc.arrayEquals()).isEmpty();
      // 1 non-doc-id main-level property
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("name SBob");
      assertThat(doc.subDocEquals()).hasSize(0);

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
      assertThat(doc.arrayEquals()).isEmpty();
      // 1 non-doc-id main-level property
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("name SBob");
      assertThat(doc.subDocEquals()).hasSize(0);

      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryNumberValues())
          .isEqualTo(Map.of(JsonPath.from("_id"), new BigDecimal(123L)));
      assertThat(doc.queryTextValues()).isEqualTo(Map.of(JsonPath.from("name"), "Bob"));
    }

    // [json-api#210]: accidental use of Engineering notation with trailing zeroes
    @Test
    public void shredSimpleWithNumberIdWithTrailingZeroes() throws Exception {
      final String inputJson = "{\"_id\":30}";
      final JsonNode inputDoc = fromJson(inputJson);

      WritableShreddedDocument doc = shredder.shred(inputDoc);
      assertThat(doc.id()).isEqualTo(DocumentId.fromNumber(new BigDecimal(30L)));

      JsonNode jsonFromShredded = fromJson(doc.docJson());

      // Important: MUST use ObjectMapper we have configured as it has necessary
      // settings. JsonNode.toString() would use "wrong" mapper
      assertThat(objectMapper.writeValueAsString(jsonFromShredded)).isEqualTo(inputJson);
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
              "Bad document type to shred: Document to shred must be a JSON Object, instead got ARRAY")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_DOCUMENT_TYPE);
    }

    @Test
    public void docBadDocIdTypeArray() {
      Throwable t =
          catchThrowable(() -> shredder.shred(objectMapper.readTree("{ \"_id\" : [ ] }")));

      assertThat(t)
          .isNotNull()
          .hasMessage(
              "Bad type for '_id' property: Document Id must be a JSON String, Number, Boolean or NULL instead got ARRAY")
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
}
