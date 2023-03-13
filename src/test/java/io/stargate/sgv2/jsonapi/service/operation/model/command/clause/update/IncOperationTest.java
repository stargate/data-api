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
  class HappyPathRoot {
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
  class HappyPathNested {
    @Test
    public void testIncOfExisting() {
      UpdateOperation oper =
          UpdateOperator.INC.resolveOperation(
              objectFromJson(
                  """
                                {
                                  "fpArray.1" : -0.5,
                                  "ints.count" : 1
                                }
                                """));
      assertThat(oper).isInstanceOf(IncOperation.class);
      ObjectNode doc =
          objectFromJson(
              """
                            { "ints" : { "count" : 37 },
                              "fpArray" : [ 0, 0.25 ]
                            }
                            """);
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                            { "ints" : { "count" : 38 },
                              "fpArray" : [ 0, -0.25 ]
                            }
                            """);
      // NOTE: need to use "toPrettyString()" since NumberNode types may differ
      assertThat(doc.toPrettyString()).isEqualTo(expected.toPrettyString());
    }

    @Test
    public void testIncOfNonExisting() {
      UpdateOperation oper =
          UpdateOperator.INC.resolveOperation(
              objectFromJson(
                  """
                                { "ints.value" : -123456 }
                                """));
      assertThat(oper).isInstanceOf(IncOperation.class);
      ObjectNode doc =
          objectFromJson(
              """
                            {
                              "numbers" : { },
                              "text" : "value"
                            }"
                            """);
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                            {
                              "numbers" : { },
                              "text" : "value",
                              "ints" : {
                                 "value" : -123456
                              }
                            }"
                      """);
      // NOTE: need to use "toPrettyString()" since NumberNode types may differ
      assertThat(doc.toPrettyString()).isEqualTo(expected.toPrettyString());
    }

    @Test
    public void testIncWithNoChange() {
      UpdateOperation oper =
          UpdateOperator.INC.resolveOperation(
              objectFromJson(
                  """
                              { "numbers.integer" : 0, "numbers.fp" : 0 }
                              """));
      assertThat(oper).isInstanceOf(IncOperation.class);
      ObjectNode doc =
          objectFromJson(
              """
                            {
                              "numbers" : {
                                "integer": 15,
                                "fp" : 2.5
                              },
                              "text" : "value"
                            }"
                            """);
      ObjectNode expected = doc.deepCopy();
      assertThat(oper.updateDocument(doc)).isFalse();
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
    // Not legal to try to modify doc id (immutable):
    @Test
    public void testIncOnDocumentId() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.INC.resolveOperation(objectFromJson("{\"_id\": 1}"));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID)
          .hasMessageStartingWith(ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": $inc");
    }

    @Test
    public void testIncOnRootStringProperty() {
      ObjectNode doc =
          objectFromJson(
              """
                    { "integer" : 1, "fp" : 0.25, "prop" : "some text"  }
                    """);
      UpdateOperation oper =
          UpdateOperator.INC.resolveOperation(
              objectFromJson("""
                    { "prop" : 57 }
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
                  + ": $inc requires target to be Number; value at 'prop' of type STRING");
    }

    @Test
    public void testIncOnNestedStringProperty() {
      ObjectNode doc =
          objectFromJson(
              """
                            { "subdoc" : { "array" : [ 1, "some text"] } }
                            """);
      UpdateOperation oper =
          UpdateOperator.INC.resolveOperation(
              objectFromJson(
                  """
                    { "subdoc.array.1" : -1 }
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
                  + ": $inc requires target to be Number; value at 'subdoc.array.1' of type STRING");
    }

    // One odd case: explicit "null" is invalid target
    @Test
    public void testIncOnExplicitNullProperty() {
      ObjectNode doc =
          objectFromJson(
              """
                    { "subdoc" : {
                       "prop" : null
                       }
                    }
                    """);
      UpdateOperation oper =
          UpdateOperator.INC.resolveOperation(
              objectFromJson(
                  """
                    { "subdoc.prop" : 3 }
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
                  + ": $inc requires target to be Number; value at 'subdoc.prop' of type NULL");
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
