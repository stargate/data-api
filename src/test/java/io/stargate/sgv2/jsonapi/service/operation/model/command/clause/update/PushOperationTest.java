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
    public void testSimplePushToExisting() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson(
                  """
                              { "array" : 32 }
                              """));
      assertThat(oper).isInstanceOf(PushOperation.class);
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
    public void testSimplePushToNonExisting() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson(
                  """
                                      { "newArray" : "value" }
                                      """));
      assertThat(oper).isInstanceOf(PushOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ true ] }");
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                              { "a" : 1, "array" : [ true ], "newArray" : [ "value" ] }
                              """);
      assertThat(doc).isEqualTo(expected);
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
                oper.updateDocument(doc);
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET.getMessage()
                  + ": $push requires target to be Array");
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
                  + ": $push only supports $each currently; trying to use '$sort'");
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
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class PushWithEachHappyCases {
    @Test
    public void withEachToExisting() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson(
                  """
                                      { "array" : { "$each" : [ 17, false ] } }
                                      """));
      assertThat(oper).isInstanceOf(PushOperation.class);
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
    public void withEachToNonExisting() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson(
                  """
                                      { "newArray" : { "$each" : [ -50, "abc" ] } }
                                        """));
      assertThat(oper).isInstanceOf(PushOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ true ] }");
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                              { "a" : 1, "array" : [ true ], "newArray" : [ -50, "abc" ] }
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
                  + ": $push only supports $each currently; trying to use 'value'");
    }
  }
}
