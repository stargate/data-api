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
  class happyPath {
    @Test
    public void testSimplePushToExisting() {
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson("""
                    { "array" : 32 }
                    """));
      assertThat(oper).isInstanceOf(PushOperation.class);
      ObjectNode doc = defaultArrayTestDoc();
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson("""
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
      ObjectNode doc = defaultArrayTestDoc();
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
  class invalidCases {
    @Test
    public void testPushOnNonArrayProperty() {
      ObjectNode doc = defaultArrayTestDoc();
      UpdateOperation oper =
          UpdateOperator.PUSH.resolveOperation(
              objectFromJson("""
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
    /*
    @Test
    public void testPushWithUnknownModifier() {
      Exception e = catchException(() -> {});
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID)
          .hasMessage(ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": $push");
    }
     */
  }

  ObjectNode defaultArrayTestDoc() {
    return objectFromJson(
        """
                    { "a" : 1, "array" : [ true ] }
                    """);
  }
}
