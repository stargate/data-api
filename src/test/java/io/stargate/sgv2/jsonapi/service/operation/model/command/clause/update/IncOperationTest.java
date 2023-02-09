package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.IncOperation;
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
public class IncOperationTest extends UpdateOperationTestBase {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class HappyPath {
    @Test
    public void testSimpleIncOfExisting() {
      UpdateOperation oper =
          UpdateOperator.INC.resolveOperation(
              objectFromJson(
                  """
                    {
                      "fp" : -0.5,
                      "integer" : 5
                    }
                    """));
      assertThat(oper).isInstanceOf(IncOperation.class);
      ObjectNode doc =
          objectFromJson(
              """
                    { "integer" : 1, "fp" : 0.25, "text" : "value"  }
                    """);
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
              { "integer" : 6, "fp" : -0.25, "text" : "value"   }
              """);
      // NOTE: need to use "toPrettyString()" since NumberNode types may differ
      assertThat(doc.toPrettyString()).isEqualTo(expected.toPrettyString());
    }

    @Test
    public void testSimpleIncOfNonExisting() {
      UpdateOperation oper =
          UpdateOperator.INC.resolveOperation(
              objectFromJson(
                  """
                    { "number" : -123456 }
                    """));
      assertThat(oper).isInstanceOf(IncOperation.class);
      ObjectNode doc =
          objectFromJson(
              """
                    { "integer" : 1, "fp" : 0.25, "text" : "value"  }
                    """);
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
              { "integer" : 1, "fp" : 0.25, "text" : "value", "number" : -123456 }
              """);
      // NOTE: need to use "toPrettyString()" since NumberNode types may differ
      assertThat(doc.toPrettyString()).isEqualTo(expected.toPrettyString());
    }

    @Test
    public void testSimpleIncWithNoChange() {
      UpdateOperation oper =
          UpdateOperator.INC.resolveOperation(
              objectFromJson(
                  """
                                { "integer" : 0, "fp" : 0 }
                                """));
      assertThat(oper).isInstanceOf(IncOperation.class);
      ObjectNode doc =
          objectFromJson(
              """
                    { "integer" : 1, "fp" : 0.25, "text" : "value"  }
                    """);
      assertThat(oper.updateDocument(doc)).isFalse();
      ObjectNode expected =
          objectFromJson(
              """
                      { "integer" : 1, "fp" : 0.25, "text" : "value" }
                      """);
      // NOTE: need to use "toPrettyString()" since NumberNode types may differ
      assertThat(doc.toPrettyString()).isEqualTo(expected.toPrettyString());
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class InvalidCases {
    @Test
    public void testIncWithNonNumberParam() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.INC.resolveOperation(
                    objectFromJson(
                        """
                                  { "a" : "text" }
                                  """));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                  + ": $inc requires numeric parameter, got: STRING");
    }

    @Test
    public void testIncOnNonNumberProperty() {
      ObjectNode doc =
          objectFromJson(
              """
                    { "integer" : 1, "fp" : 0.25, "text" : "value"  }
                    """);
      UpdateOperation oper =
          UpdateOperator.INC.resolveOperation(
              objectFromJson("""
                    { "text" : 57 }
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
                  + ": $inc requires target to be Number");
    }

    // Test to make sure we know to look for "$"-modifier to help with user errors
    @Test
    public void testIncWithModifier() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.INC.resolveOperation(
                    objectFromJson(
                        """
                    { "$each" : 5 }
                    """));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER.getMessage()
                  + ": $inc does not support modifiers");
    }
  }
}
