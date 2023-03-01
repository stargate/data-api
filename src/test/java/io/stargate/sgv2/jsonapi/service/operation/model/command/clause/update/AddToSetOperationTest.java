package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.AddToSetOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class AddToSetOperationTest extends UpdateOperationTestBase {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class BasicAddToSetHappyPath {
    @Test
    public void addToExistingRoot() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(objectFromJson("{ \"array\" : 32 }"));
      assertThat(oper).isInstanceOf(AddToSetOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ true ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                              { "a" : 1, "array" : [ true, 32 ] }
                              """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void addToExistingNested() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(objectFromJson("{ \"subdoc.array\" : 32 }"));
      assertThat(oper).isInstanceOf(AddToSetOperation.class);
      ObjectNode doc = objectFromJson("{ \"subdoc\" :  { \"array\" : [ true ] } }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                                      { "subdoc" : { "array" : [ true, 32 ] } }
                                      """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void addToNonExistingRoot() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson("{ \"newArray\" : \"value\" }"));
      ObjectNode doc = objectFromJson("{ \"a\": 1, \"array\" : [ true ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                              { "a": 1, "array": [ true ], "newArray": [ "value" ] }
                              """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void addToNonExistingNested() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson("{ \"subdoc.newArray\" : \"value\" }"));
      ObjectNode doc = objectFromJson("{ \"array\" : [ true ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                                      { "array": [ true ], "subdoc" : { "newArray": [ "value" ] } }
                                      """);
      assertThat(doc).isEqualTo(expected);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class BasicAddToSetInvalidCases {
    @Test
    public void onNonArrayProperty() {
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ true ] }");
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson(
                  """
                                      { "a" : 57 }
                                      """));
      Exception e =
          catchException(
              () -> {
                oper.updateDocument(doc, targetLocator);
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
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class AddToSetWithEachHappyCases {
    @Test
    public void withEachToExistingRoot() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson(
                  """
                                      { "array" : { "$each" : [ 17, false ] } }
                                      """));
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ true ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
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
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
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
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
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
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                                      { "nested" : { "array" : [ true ], "newArray" : [ -50, "abc" ] } }
                                      """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void withEachNestedArray() {
      UpdateOperation oper =
          UpdateOperator.ADD_TO_SET.resolveOperation(
              objectFromJson(
                  """
                                      { "array" : { "$each" : [ [ 1, 2], [ 3 ] ] } }
                                      """));
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ null ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                              { "a" : 1, "array" : [ null, [ 1, 2 ], [ 3 ] ] }
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
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                              { "x" : 1, "array" : [ [ 1, 2 ], [ 3 ] ] }
                              """);
      assertThat(doc).isEqualTo(expected);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class AddToSetWithEachInvalidCases {
    // Argument for "$each" must be an Array
    @Test
    public void nonArrayParamForEach() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.ADD_TO_SET.resolveOperation(
                    objectFromJson(
                        """
                                                        { "array" : { "$each" : 365 } }
                                                """));
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
