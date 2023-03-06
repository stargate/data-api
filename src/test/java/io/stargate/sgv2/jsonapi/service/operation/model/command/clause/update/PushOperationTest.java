package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.PushOperation;
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
public class PushOperationTest extends UpdateOperationTestBase {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class BasicPushHappyPath {
    @Test
    public void testPushToExistingRoot() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(objectFromJson("{ \"array\" : 32 }"));
      assertThat(oper).isInstanceOf(PushOperation.class);
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
    public void testPushToExistingNested() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(objectFromJson("{ \"subdoc.array\" : 32 }"));
      assertThat(oper).isInstanceOf(PushOperation.class);
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
    public void testPushToNonExistingRoot() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(objectFromJson("{ \"newArray\" : \"value\" }"));
      assertThat(oper).isInstanceOf(PushOperation.class);
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
    public void testPushToNonExistingNested() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson("{ \"subdoc.newArray\" : \"value\" }"));
      assertThat(oper).isInstanceOf(PushOperation.class);
      ObjectNode doc = objectFromJson("{ \"array\" : [ true ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                                      { "array": [ true ], "subdoc" : { "newArray": [ "value" ] } }
                                      """);
      assertThat(doc).isEqualTo(expected);
    }

    // Test to ensure $push operations are done in alphabetic order by field
    @Test
    public void testPushToNonExistingOrdered() {
      // Put targets in "wrong" order (different from expected execution order)
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson("{ \"subdoc.newArray\" : \"value\", \"array\": 3 }"));
      assertThat(oper).isInstanceOf(PushOperation.class);
      ObjectNode doc = objectFromJson("{ }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                      { "array": [ 3 ], "subdoc" : { "newArray": [ "value" ] } }
                      """);
      // Important: compare serializations as they indicate order of Object fields;
      // ObjectNode.equals() uses order-insensitive comparison
      assertThat(doc.toPrettyString()).isEqualTo(expected.toPrettyString());
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class BasicPushInvalidCases {
    @Test
    public void testPushOnNonArrayProperty() {
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ true ] }");
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
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
                  + ": $push requires target to be ARRAY");
    }

    // Test to make sure we know to look for "$"-qualifiers even if not yet supporting them?
    @Test
    public void testPushWithUnknownModifier() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.PUSH.resolveOperation(
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
                  + ": $push only supports $each and $position currently; trying to use '$sort'");
    }

    // Modifiers, if any, must be "under" field name, not at main level (and properties
    // cannot have names starting with "$")
    @Test
    public void testPushWithMisplacedModifier() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.PUSH.resolveOperation(
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
                  + ": $push requires field names at main level, found modifier: $each");
    }

    @Test
    public void tryPushWithDocId() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.PUSH.resolveOperation(objectFromJson("{ \"_id\" : 123 }"));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID)
          .hasMessage(ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": $push");
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class PushWithEachHappyCases {
    @Test
    public void withEachToExistingRoot() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson(
                  """
                                      { "array" : { "$each" : [ 17, false ] } }
                                      """));
      assertThat(oper).isInstanceOf(PushOperation.class);
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
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson(
                  """
                                                  { "nested.array" : { "$each" : [ 17, false ] } }
                                                  """));
      assertThat(oper).isInstanceOf(PushOperation.class);
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
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson(
                  """
                                      { "newArray" : { "$each" : [ -50, "abc" ] } }
                                        """));
      assertThat(oper).isInstanceOf(PushOperation.class);
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
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson(
                  """
                                                  { "nested.newArray" : { "$each" : [ -50, "abc" ] } }
                                                    """));
      assertThat(oper).isInstanceOf(PushOperation.class);
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
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson(
                  """
                                      { "array" : { "$each" : [ [ 1, 2], [ 3 ] ] } }
                                      """));
      assertThat(oper).isInstanceOf(PushOperation.class);
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
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson(
                  """
                                      { "array" : { "$each" : [ [ 1, 2], [ 3 ] ] } }
                                      """));
      assertThat(oper).isInstanceOf(PushOperation.class);
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
  class PushWithEachInvalidCases {
    // Argument for "$each" must be an Array
    @Test
    public void nonArrayParamForEach() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.PUSH.resolveOperation(
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
                  + ": $push modifier $each requires ARRAY argument, found: NUMBER");
    }

    // If there is one modifier for given field, all Object properties must be (supported)
    // modifiers:
    @Test
    public void nonModifierParamForEach() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.PUSH.resolveOperation(
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
                  + ": $push only supports $each and $position currently; trying to use 'value'");
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class PushWithPositionHappyCases {
    @Test
    public void withEachToExistingPositiveRoot() {
      // First case: insert between 1st and 2nd elements
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson("{ \"array\": { \"$each\" : [true, false], \"$position\" : 1 } }"));
      assertThat(oper).isInstanceOf(PushOperation.class);
      ObjectNode doc = objectFromJson("{ \"array\": [ 1, 2, 3, 4 ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected = objectFromJson("{ \"array\": [ 1, true, false, 2, 3, 4 ] }");
      assertThat(doc).isEqualTo(expected);

      // And then try to append way past end; legal, just appends normally
      oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson("{ \"array\": { \"$each\" : [true, false], \"$position\" : 999 } }"));
      assertThat(oper).isInstanceOf(PushOperation.class);
      doc = objectFromJson("{ \"array\": [ 1, 2, 3, 4 ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      expected = objectFromJson("{ \"array\": [ 1, 2, 3, 4, true, false ] }");
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void withEachToExistingPositiveNested() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson(
                  "{ \"nested.array\": { \"$each\" : [true, false], \"$position\" : 1 } }"));
      assertThat(oper).isInstanceOf(PushOperation.class);
      ObjectNode doc = objectFromJson("{ \"nested\": { \"array\": [ 1, 2, 3, 4 ] } }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson("{ \"nested\" : { \"array\": [ 1, true, false, 2, 3, 4 ] } }");
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void withEachToExistingNegative() {
      // First with "insert before the last entry"
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson("{ \"array\": { \"$each\" : [true, false], \"$position\" : -1 } }"));
      assertThat(oper).isInstanceOf(PushOperation.class);
      ObjectNode doc = objectFromJson("{ \"array\": [ 1, 2, 3, 4 ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected = objectFromJson("{ \"array\": [ 1, 2, 3, true, false, 4 ] }");
      assertThat(doc).isEqualTo(expected);

      // And then way before beginning (valid, just gets truncated to index #0)
      oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson("{ \"array\": { \"$each\" : [true, false], \"$position\" : -999 } }"));
      assertThat(oper).isInstanceOf(PushOperation.class);
      doc = objectFromJson("{ \"array\": [ 1, 2, 3, 4 ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      expected = objectFromJson("{ \"array\": [ true, false, 1, 2, 3, 4 ] }");
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void withEachToNonExistingRoot() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson("{ \"newArray\": { \"$each\" : [true, false], \"$position\": 1 } }"));
      assertThat(oper).isInstanceOf(PushOperation.class);
      ObjectNode doc = objectFromJson("{ \"array\": [ 1, 2, 3 ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson("{ \"array\": [ 1, 2, 3 ], \"newArray\": [true, false] }");
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void withEachToNonExistingNested() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson(
                  "{ \"nested.array\": { \"$each\" : [true, false], \"$position\": 1 } }"));
      assertThat(oper).isInstanceOf(PushOperation.class);
      ObjectNode doc = objectFromJson("{ }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected = objectFromJson("{ \"nested\": { \"array\": [true, false] } }");
      assertThat(doc).isEqualTo(expected);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class PushWithPositionInvalidCases {
    // Argument for "$each" must be an Array
    @Test
    public void nonNumberParamForPosition() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.PUSH.resolveOperation(
                    objectFromJson("{\"array\" : { \"$each\": [1], \"$position\" : true } }"));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                  + ": $push modifier $position requires (integral) NUMBER argument, found: BOOLEAN");
    }

    @Test
    public void nonIntegerParamForPosition() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.PUSH.resolveOperation(
                    objectFromJson("{\"array\" : { \"$each\": [1], \"$position\" : 1.5 } }"));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                  + ": $push modifier $position requires Integer NUMBER argument, instead got: 1.5");
    }

    @Test
    public void missingEachParamForPosition() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.PUSH.resolveOperation(
                    objectFromJson("{\"array\" : { \"$position\" : 2 } }"));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                  + ": $push modifiers can only be used with $each modifier; none included");
    }
  }
}
