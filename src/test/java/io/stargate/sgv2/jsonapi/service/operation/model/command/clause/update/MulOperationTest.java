package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.MulOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class MulOperationTest extends UpdateOperationTestBase {
  @Nested
  class HappyPathRoot {
    @Test
    public void testSimpleMulOfExisting() {
      UpdateOperation oper =
          UpdateOperator.MUL.resolveOperation(
              objectFromJson(
                  """
                    {
                      "fp" : -0.5,
                      "integer" : 5
                    }
                    """));
      assertThat(oper).isInstanceOf(MulOperation.class);
      ObjectNode doc =
          objectFromJson(
              """
                    { "integer" : 2, "fp" : 0.5, "text" : "value"  }
                    """);
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                    { "integer" : 10, "fp" : -0.25, "text" : "value"   }
                    """);
      // NOTE: need to use "toPrettyString()" since NumberNode types may differ
      assertThat(asPrettyJson(doc)).isEqualTo(asPrettyJson(expected));
    }

    @Test
    public void testSimpleMulOfNonExisting() {
      UpdateOperation oper =
          UpdateOperator.MUL.resolveOperation(
              objectFromJson(
                  """
                    { "number" : -123456 }
                    """));
      ObjectNode doc =
          objectFromJson(
              """
                    { "integer" : 1, "fp" : 0.25, "text" : "value"  }
                    """);
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
              { "integer" : 1, "fp" : 0.25, "text" : "value", "number" : 0 }
              """);
      // NOTE: need to use "toPrettyString()" since NumberNode types may differ
      assertThat(asPrettyJson(doc)).isEqualTo(asPrettyJson(expected));
    }

    @Test
    public void testSimpleMulWithNoChange() {
      UpdateOperation oper =
          UpdateOperator.MUL.resolveOperation(
              objectFromJson(
                  """
                                { "integer" : 1, "fp" : 1.0 }
                                """));
      assertThat(oper).isInstanceOf(MulOperation.class);
      ObjectNode doc =
          objectFromJson(
              """
                    { "integer" : 5, "fp" : 0.25, "text" : "value"  }
                    """);
      ObjectNode expected = doc.deepCopy();
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      // NOTE: need to use "toPrettyString()" since NumberNode types may differ
      assertThat(asPrettyJson(doc)).isEqualTo(asPrettyJson(expected));
    }
  }

  @Nested
  class HappyPathNested {
    @Test
    public void testMulOfExisting() {
      UpdateOperation oper =
          UpdateOperator.MUL.resolveOperation(
              objectFromJson(
                  """
                                {
                                  "fpArray.1" : -0.5,
                                  "ints.count" : 2
                                }
                                """));
      ObjectNode doc =
          objectFromJson(
              """
                            { "ints" : { "count" : 7 },
                              "fpArray" : [ 0, 0.25 ]
                            }
                            """);
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                            { "ints" : { "count" : 14 },
                              "fpArray" : [ 0, -0.125 ]
                            }
                            """);
      assertThat(doc.toPrettyString()).isEqualTo(expected.toPrettyString());
    }

    @Test
    public void testMulOfNonExisting() {
      UpdateOperation oper =
          UpdateOperator.MUL.resolveOperation(
              objectFromJson(
                  """
                  { "ints.value" : -123456 }
                  """));
      ObjectNode doc =
          objectFromJson(
              """
                            {
                              "numbers" : { },
                              "text" : "value"
                            }"
                            """);
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                            {
                              "numbers" : { },
                              "text" : "value",
                              "ints" : {
                                 "value" : 0
                              }
                            }"
                      """);
      assertThat(doc.toPrettyString()).isEqualTo(expected.toPrettyString());
    }

    @Test
    public void testMulWithNoChange() {
      UpdateOperation oper =
          UpdateOperator.MUL.resolveOperation(
              objectFromJson(
                  """
                              { "numbers.integer" : 1, "numbers.fp" : 1 }
                              """));
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
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      assertThat(doc.toPrettyString()).isEqualTo(expected.toPrettyString());
    }
  }

  @Nested
  class InvalidCases {
    @Test
    public void testMulWithNonNumberParam() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.MUL.resolveOperation(
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
                  + ": $mul requires numeric parameter, got: STRING");
    }
    // Not legal to try to modify doc id (immutable):
    @Test
    public void testMulOnDocumentId() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.MUL.resolveOperation(objectFromJson("{\"_id\": 1}"));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID)
          .hasMessageStartingWith(ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": $mul");
    }

    @Test
    public void testMulOnRootStringProperty() {
      ObjectNode doc = objectFromJson("{ \"prop\" : \"some text\"  }");
      UpdateOperation oper =
          UpdateOperator.MUL.resolveOperation(objectFromJson("{ \"prop\" : 57 }"));
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
                  + ": $mul requires target to be Number; value at 'prop' of type STRING");
    }

    // One odd case: explicit "null" is invalid target
    @Test
    public void testMulOnExplicitNullProperty() {
      ObjectNode doc =
          objectFromJson(
              """
                    { "subdoc" : {
                       "prop" : null
                       }
                    }
                    """);
      UpdateOperation oper =
          UpdateOperator.MUL.resolveOperation(
              objectFromJson(
                  """
                    { "subdoc.prop" : 3 }
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
                  + ": $mul requires target to be Number; value at 'subdoc.prop' of type NULL");
    }

    // Test to make sure we know to look for "$"-modifier to help with user errors
    @Test
    public void testIncWithModifier() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.MUL.resolveOperation(
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
                  + ": $mul does not support modifiers");
    }
  }
}
