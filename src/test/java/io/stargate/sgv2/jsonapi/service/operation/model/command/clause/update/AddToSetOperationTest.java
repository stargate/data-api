package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.AddToSetOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class AddToSetOperationTest extends UpdateOperationTestBase {
  @Nested
  class AddToSetBasicHappyPath {
    @Test
    public void addToRootArray() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(objectFromJson("{ \"array\" : 32 }"));
      assertThat(oper).isInstanceOf(AddToSetOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ true ] }");
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                              { "a" : 1, "array" : [ true, 32 ] }
                              """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void tryAddExistingValueRoot() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(objectFromJson("{ \"array\" : 19 }"));
      ObjectNode doc = objectFromJson("{ \"array\" : [ true, \"foo\", 19 ] }");
      ObjectNode expected = doc.deepCopy();
      // Won't add since we already had same value
      assertThat(oper.updateDocument(doc)).isFalse();
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void addToNestedArray() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(objectFromJson("{ \"subdoc.array\" : 32 }"));
      ObjectNode doc = objectFromJson("{ \"subdoc\" :  { \"array\" : [ true ] } }");
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
              { "subdoc" : { "array" : [ true, 32 ] } }
              """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void tryAddExistingValueNested() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson("{ \"subdoc.array\" : \"b\" }"));
      ObjectNode doc = objectFromJson("{ \"subdoc\" :  { \"array\" : [ \"a\", \"b\", \"c\" ] } }");
      ObjectNode expected = doc.deepCopy();
      // Already had "b", no change
      assertThat(oper.updateDocument(doc)).isFalse();
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void addToNewArrayRoot() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson("{ \"newArray\" : \"value\" }"));
      ObjectNode doc = objectFromJson("{ \"a\": 1, \"array\" : [ true ] }");
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
              { "a": 1, "array": [ true ], "newArray": [ "value" ] }
              """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void addToNewArrayNested() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson("{ \"subdoc.newArray\" : \"value\" }"));
      ObjectNode doc = objectFromJson("{ \"array\" : [ true ] }");
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
              { "array": [ true ], "subdoc" : { "newArray": [ "value" ] } }
              """);
      assertThat(doc).isEqualTo(expected);
    }
  }

  // Since equality semantics of sub-docs differ, have separate sets for them
  @Nested
  class AddToSetWithSubDocs {
    @Test
    public void addSubDocIfOrderDifferent() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson("{ \"doc.array\" : { \"y\":2, \"x\": 1}}"));
      ObjectNode doc =
          objectFromJson(
              """
                      { "doc" :
                        {
                          "array" : [{ "x": 1, "y": 2 }]
                        }
                      }
                      """);
      // Should add, change
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                      { "doc" :
                        {
                          "array" : [{ "x": 1, "y": 2 }, {"y":2, "x":1 }]
                        }
                      }
                      """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void dontAddSubDocIfSameIncludingOrdering() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson("{ \"doc.array\" : { \"x\":1, \"y\": 2}}"));
      ObjectNode doc =
          objectFromJson(
              """
                      { "doc" :
                        {
                          "array" : [{ "x": 1, "y": 2 }]
                        }
                      }
                      """);
      // No add, no change
      assertThat(oper.updateDocument(doc)).isFalse();
      ObjectNode expected =
          objectFromJson(
              """
                      { "doc" :
                        {
                          "array" : [{ "x": 1, "y": 2 }]
                        }
                      }
                      """);
      assertThat(doc).isEqualTo(expected);
    }
  }

  @Nested
  class AddToSetBasicInvalidCases {
    @Test
    public void onNonArrayProperty() {
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ true ] }");
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(objectFromJson("{ \"a\" : 57 }"));
      Exception e =
          catchException(
              () -> {
                oper.updateDocument(doc);
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET.getMessage()
                  + ": $addToSet requires target to be ARRAY");
    }

    @Test
    public void withUnknownModifier() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.ADD_TO_SET.resolveOperation(
                    objectFromJson(
                        """
                                                { "a" : { "$sort" : { "field": 1 } } }
                                                """));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER.getMessage()
                  + ": $addToSet only supports $each modifier; trying to use '$sort'");
    }

    // Modifiers, if any, must be "under" field name, not at main level (and properties
    // cannot have names starting with "$")
    @Test
    public void withMisplacedModifier() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.ADD_TO_SET.resolveOperation(
                    objectFromJson(
                        """
                                                { "$each" : [ 1, 2 ] }
                                                """));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                  + ": $addToSet requires field names at main level, found modifier: $each");
    }

    @Test
    public void tryWithDocId() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.ADD_TO_SET.resolveOperation(objectFromJson("{ \"_id\" : 123 }"));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID)
          .hasMessage(ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": $addToSet");
    }
  }

  @Nested
  class AddToSetWithEachHappyPath {
    @Test
    public void withEachToExistingRoot() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson(
                  """
                  { "array" : { "$each" : [ 17, false ] } }
                  """));
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ true ] }");
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
              { "a" : 1, "array" : [ true, 17, false ] }
              """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void withEachToExistingNested() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson(
                  """
                  { "nested.array" : { "$each" : [ 17, false ] } }
                  """));
      ObjectNode doc = objectFromJson("{ \"nested\": { \"array\" : [ true ] } }");
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
              { "nested": { "array" : [ true, 17, false ] } }
              """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void withEachToNonExistingRoot() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson(
                  """
                                      { "newArray" : { "$each" : [ -50, "abc" ] } }
                                        """));
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ true ] }");
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
              { "a" : 1, "array" : [ true ], "newArray" : [ -50, "abc" ] }
              """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void withEachToNonExistingNested() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson(
                  """
                  { "nested.newArray" : { "$each" : [ -50, "abc" ] } }
                    """));
      ObjectNode doc = objectFromJson("{ \"nested\": { \"array\" : [ true ] } }");
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                { "nested" : { "array" : [ true ], "newArray" : [ -50, "abc" ] } }
                """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void withEachNestedArray() {
      // Let's check that [ 3 ] is NOT added as it already exists in doc:
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ null, [ 3 ] ] }");
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson(
                  """
                  { "array" : { "$each" : [ [ 1, 2], [ 3 ] ] } }
                  """));
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                { "a" : 1, "array" : [ null, [3], [ 1, 2 ] ] }
                """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void withEachNestedArrayNonExisting() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson(
                  """
                  { "array" : { "$each" : [ [ 1, 2], [ 3 ] ] } }
                  """));
      ObjectNode doc = objectFromJson("{ \"x\" : 1 }");
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
              { "x" : 1, "array" : [ [ 1, 2 ], [ 3 ] ] }
              """);
      assertThat(doc).isEqualTo(expected);
    }
  }

  @Nested
  class AddToSetWithEachInvalidCases {
    // Argument for "$each" must be an Array
    @Test
    public void nonArrayParamForEach() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.ADD_TO_SET.resolveOperation(
                    objectFromJson("{ \"array\" : { \"$each\" : 365 } }"));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                  + ": $addToSet modifier $each requires ARRAY argument, found: NUMBER");
    }

    // If there is one modifier for given field, all Object properties must be (supported)
    // modifiers:
    @Test
    public void nonModifierParamForEach() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.ADD_TO_SET.resolveOperation(
                    objectFromJson(
                        """
                        { "array" : { "$each" : [ 1, 2, 3 ], "value" : 3 } }
                        """));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER.getMessage()
                  + ": $addToSet only supports $each modifier; trying to use 'value'");
    }
  }
}
