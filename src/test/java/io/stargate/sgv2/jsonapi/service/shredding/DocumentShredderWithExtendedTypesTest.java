package io.stargate.sgv2.jsonapi.service.shredding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.uuid.impl.UUIDUtil;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionIdType;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.*;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.Arrays;
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
public class DocumentShredderWithExtendedTypesTest {
  @Inject ObjectMapper objectMapper;

  @Inject DocumentShredder documentShredder;
  @InjectMock protected RequestContext bogusRequestInfo;

  private final TestConstants testConstants = new TestConstants();

  @Nested
  class OkCasesExplicitId {
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
      WritableShreddedDocument doc = documentShredder.shred(commandContext(), inputDoc, null);

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
      WritableShreddedDocument doc = documentShredder.shred(commandContext(), inputDoc, null);

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
  }

  @Nested
  class OkCasesGeneratedId {
    @Test
    public void shredSimpleWithoutIdGenLegacyUUID() throws Exception {
      final String inputJson = "{\"value\": 42}";
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      WritableShreddedDocument doc =
          documentShredder.shred(
              inputDoc,
              null,
              IndexingProjector.identityProjector(),
              "test",
              CollectionSchemaObject.MISSING.withIdType(CollectionIdType.UNDEFINED),
              null);

      DocumentId docId = doc.id();
      // Legacy UUID generated as "plain" String id
      assertThat(docId).isInstanceOf(DocumentId.StringId.class);

      // should be auto-generated ObjectId: verify by constructing from String representation:
      UUID typedId = UUIDUtil.uuid(((DocumentId.StringId) docId).key());
      assertThat(typedId).isNotNull();
      List<JsonPath> expPaths = Arrays.asList(JsonPath.from("_id"), JsonPath.from("value"));

      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));
      assertThat(doc.arraySize()).isEmpty();
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("value N42");

      // Also, the document should be the same, including _id added:
      ObjectNode jsonFromShredded = (ObjectNode) objectMapper.readTree(doc.docJson());
      JsonNode idNode = jsonFromShredded.get("_id");

      assertThat(idNode.asText()).isEqualTo(typedId.toString());

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryNumberValues())
          .isEqualTo(Map.of(JsonPath.from("value"), BigDecimal.valueOf(42)));
      assertThat(doc.queryTextValues()).isEqualTo(Map.of(JsonPath.from("_id"), typedId.toString()));
    }

    @Test
    public void shredSimpleWithoutIdGenObjectId() throws Exception {
      final String inputJson = "{\"value\": 42}";
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      WritableShreddedDocument doc =
          documentShredder.shred(
              inputDoc,
              null,
              IndexingProjector.identityProjector(),
              "test",
              CollectionSchemaObject.MISSING.withIdType(CollectionIdType.OBJECT_ID),
              null);

      DocumentId docId = doc.id();
      assertThat(docId).isInstanceOf(DocumentId.ExtensionTypeId.class);

      // should be auto-generated ObjectId: verify by constructing from String representation:
      ObjectId typedId = new ObjectId(((DocumentId.ExtensionTypeId) docId).valueAsString());
      assertThat(typedId).isNotNull();
      List<JsonPath> expPaths = Arrays.asList(JsonPath.from("_id"), JsonPath.from("value"));

      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));
      assertThat(doc.arraySize()).isEmpty();
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("value N42");

      // Also, the document should be the same, including _id added:
      ObjectNode jsonFromShredded = (ObjectNode) objectMapper.readTree(doc.docJson());
      JsonNode idNode = jsonFromShredded.get("_id");

      assertThat(idNode).isNotNull().isInstanceOf(ObjectNode.class).hasSize(1);
      assertThat(objectMapper.createObjectNode().put("$objectId", typedId.toString()))
          .isEqualTo(idNode);

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryNumberValues())
          .isEqualTo(Map.of(JsonPath.from("value"), BigDecimal.valueOf(42)));
      assertThat(doc.queryTextValues()).isEqualTo(Map.of(JsonPath.from("_id"), typedId.toString()));
    }

    @Test
    public void shredSimpleWithoutIdGenUUIDv4() throws Exception {
      _testShredUUIDAutoGeneration(CollectionIdType.UUID, 4);
    }

    @Test
    public void shredSimpleWithoutIdGenUUIDv6() throws Exception {
      _testShredUUIDAutoGeneration(CollectionIdType.UUID_V6, 6);
    }

    @Test
    public void shredSimpleWithoutIdGenUUIDv7() throws Exception {
      _testShredUUIDAutoGeneration(CollectionIdType.UUID_V7, 7);
    }

    private void _testShredUUIDAutoGeneration(CollectionIdType idType, int uuidVersion)
        throws Exception {
      final String inputJson = "{\"value\": 42}";
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      WritableShreddedDocument doc =
          documentShredder.shred(
              inputDoc,
              null,
              IndexingProjector.identityProjector(),
              "test",
              CollectionSchemaObject.MISSING.withIdType(idType),
              null);

      DocumentId docId = doc.id();
      assertThat(docId).isInstanceOf(DocumentId.ExtensionTypeId.class);

      // should be auto-generated UUID of version 4: verify by constructing from String
      // representation
      UUID typedId = UUIDUtil.uuid(((DocumentId.ExtensionTypeId) docId).valueAsString());
      assertThat(typedId.version()).isEqualTo(uuidVersion);
      List<JsonPath> expPaths = Arrays.asList(JsonPath.from("_id"), JsonPath.from("value"));

      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));
      assertThat(doc.arraySize()).isEmpty();
      assertThat(doc.arrayContains()).containsExactlyInAnyOrder("value N42");

      // Also, the document should be the same, including _id added:
      ObjectNode jsonFromShredded = (ObjectNode) objectMapper.readTree(doc.docJson());
      JsonNode idNode = jsonFromShredded.get("_id");

      assertThat(idNode).isNotNull().isInstanceOf(ObjectNode.class).hasSize(1);
      assertThat(objectMapper.createObjectNode().put("$uuid", typedId.toString()))
          .isEqualTo(idNode);

      // Then atomic value containers
      assertThat(doc.queryBoolValues()).isEmpty();
      assertThat(doc.queryNullValues()).isEmpty();
      assertThat(doc.queryNumberValues())
          .isEqualTo(Map.of(JsonPath.from("value"), BigDecimal.valueOf(42)));
      assertThat(doc.queryTextValues()).isEqualTo(Map.of(JsonPath.from("_id"), typedId.toString()));
    }
  }

  @Nested
  class OkCasesBinaryVector {
    @Test
    public void shredSimpleBinaryVector() throws Exception {
      final String inputJson =
          """
                  {
                    "age" : 39,
                    "$vector": {"$binary": "PoAAAD6AAAA+gAAAPoAAAD6AAAA="}
                  }
                  """;
      final JsonNode inputDoc = objectMapper.readTree(inputJson);
      WritableShreddedDocument doc = documentShredder.shred(commandContext(), inputDoc, null);

      List<JsonPath> expPaths =
          Arrays.asList(JsonPath.from("_id"), JsonPath.from("age"), JsonPath.from("$vector"));

      // First verify paths
      assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

      float[] vector = {0.25f, 0.25f, 0.25f, 0.25f, 0.25f};
      assertThat(doc.queryVectorValues()).containsOnly(vector);
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
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(commandContext(), objectMapper.readTree(inputJson), null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.SHRED_BAD_DOCID_TYPE)
          .hasMessageContaining(
              "'$objectId' value has to be 24-digit hexadecimal ObjectId, instead got (\"not-an-oid\")");
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
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(commandContext(), objectMapper.readTree(inputJson), null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.SHRED_BAD_DOCID_TYPE)
          .hasMessageContaining(
              "'$uuid' value has to be 36-character UUID String, instead got (\"not-a-uuid\")");

      // second: JSON Object also not valid UUID representation
      t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(),
                      objectMapper.readTree("{ \"_id\" : {\"$uuid\": { } } }"),
                      null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.SHRED_BAD_DOCID_TYPE)
          .hasMessageContaining(
              "'$uuid' value has to be 36-character UUID String, instead got ({})");
    }

    @Test
    public void docUnknownEJSonAsId() {
      final String inputJson =
          """
                                  { "_id" : {"$unknown": "value"} }
                                  """;
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(commandContext(), objectMapper.readTree(inputJson), null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.SHRED_BAD_DOCID_TYPE)
          .hasMessage(
              ErrorCodeV1.SHRED_BAD_DOCID_TYPE.getMessage()
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
                  documentShredder.shred(
                      commandContext(),
                      objectMapper.readTree("{ \"value\": { \"$objectId\": \"abc\" } }"),
                      null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.SHRED_BAD_EJSON_VALUE)
          .hasMessageStartingWith(
              ErrorCodeV1.SHRED_BAD_EJSON_VALUE.getMessage()
                  + ": invalid value (\"abc\") for extended JSON type '$objectId' (path 'value')");
    }

    @Test
    public void docBadUUIDAsValue() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(),
                      objectMapper.readTree("{ \"value\": { \"$uuid\": \"foobar\" } }"),
                      null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.SHRED_BAD_EJSON_VALUE)
          .hasMessageStartingWith(
              ErrorCodeV1.SHRED_BAD_EJSON_VALUE.getMessage()
                  + ": invalid value (\"foobar\") for extended JSON type '$uuid' (path 'value')");
    }

    @Test
    public void docUnknownEJsonAsValue() {
      Throwable t =
          catchThrowable(
              () ->
                  documentShredder.shred(
                      commandContext(),
                      objectMapper.readTree("{ \"value\": { \"$unknownType\": 123 } }"),
                      null));

      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.SHRED_DOC_KEY_NAME_VIOLATION)
          .hasMessageStartingWith(ErrorCodeV1.SHRED_DOC_KEY_NAME_VIOLATION.getMessage());
    }
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

  private CommandContext<CollectionSchemaObject> commandContext() {
    return testConstants.collectionContext();
  }
}
