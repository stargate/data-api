package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.SetOperation;
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
public class SetOperationTest extends UpdateOperationTestBase {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class happyPathRoot {
    @Test
    public void testSimpleSetOfExisting() {
      // Remove 2 of 3 properties:
      UpdateOperation oper =
          UpdateOperator.SET.resolveOperation(
              objectFromJson(
                  """
                    { "a" : "a", "b" : 125 }
                    """));
      assertThat(oper).isInstanceOf(SetOperation.class);
      // Should indicate document being modified
      ObjectNode doc =
          objectFromJson(
              """
                          { "a" : 1, "c" : true, "b" : 1234 }
                          """);
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      assertThat(doc)
          .isEqualTo(
              fromJson(
                  """
                    { "a" : "a", "c" : true, "b" : 125 }
                    """));
    }

    @Test
    public void testSimpleSetOfNonExisting() {
      UpdateOperation oper =
          UpdateOperator.SET.resolveOperation(
              objectFromJson(
                  """
                    { "nosuchvalue" : 99 }
                    """));
      assertThat(oper).isInstanceOf(SetOperation.class);
      ObjectNode doc =
          objectFromJson(
              """
                          { "a" : 1, "c" : true, "b" : 1234 }
                          """);
      // Will append the new property so there is modification
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                          { "a" : 1, "c" : true, "b" : 1234, "nosuchvalue": 99 }
                          """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testSimpleSetWithoutChange() {
      UpdateOperation oper =
          UpdateOperator.SET.resolveOperation(
              objectFromJson("""
                    { "c" : true }
                    """));
      assertThat(oper).isInstanceOf(SetOperation.class);
      ObjectNode orig =
          objectFromJson(
              """
                          { "a" : 1, "c" : true, "b" : 1234 }
                          """);
      ObjectNode doc = orig.deepCopy();
      // No change, c was already set as true
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      // And document should not be changed
      assertThat(doc).isEqualTo(orig);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class happyPathNestedObjects {
    @Test
    public void testSetOfExistingNested() {
      // Replace 2 of 3 properties:
      UpdateOperation oper =
          UpdateOperator.SET.resolveOperation(
              objectFromJson(
                  """
                                {
                                  "a.x" : -27,
                                  "b.array": true
                                }
                                """));
      assertThat(oper).isInstanceOf(SetOperation.class);
      ObjectNode doc =
          objectFromJson(
              """
                {
                  "a" : { "x": 9, "y" : 2 },
                  "b" : { "on": true, "array": [1, 2, 3] }
                }
                """);
      // Should indicate document being modified
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      assertThat(doc)
          .isEqualTo(
              fromJson(
                  """
                {
                  "a" : { "x": -27, "y" : 2 },
                  "b" : { "on": true, "array": true }
                }
                """));
    }

    @Test
    public void testSetOfMissingNested() {
      UpdateOperation oper =
          UpdateOperator.SET.resolveOperation(
              objectFromJson(
                  """
                                            {
                                              "a.x" : true,
                                              "array.0.name": null
                                            }
                                            """));
      ObjectNode doc = objectFromJson("{ }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      assertThat(doc)
          .isEqualTo(
              fromJson(
                  """
                            {
                              "a" : { "x": true },
                              "array" : { "0": { "name": null } }
                            }
                            """));
    }

    @Test
    public void testNoChangeWithNested() {
      UpdateOperation oper =
          UpdateOperator.SET.resolveOperation(objectFromJson("{\"a.x\": \"value\"}"));
      ObjectNode doc = objectFromJson("{\"a\": {\"x\": \"value\" }}");
      ObjectNode exp = doc.deepCopy();

      // No change reported, none observed
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      assertThat(doc).isEqualTo(exp);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class happyPathNestedArrays {
    @Test
    public void testSetOfNestedArrays() {
      UpdateOperation oper =
          UpdateOperator.SET.resolveOperation(
              objectFromJson(
                  """
                                            {
                                              "array.1" : -27,
                                              "subdoc.array.3": true
                                            }
                                            """));
      ObjectNode doc =
          objectFromJson(
              """
                        {
                          "array" : [1, 2],
                          "subdoc" : {
                            "array" : [ ]
                          }
                        }
                        """);
      // Should indicate document being modified
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      assertThat(doc)
          .isEqualTo(
              fromJson(
                  """
                                {
                                  "array" : [1, -27],
                                  "subdoc" : {
                                    "array" : [ null, null, null, true ]
                                  }
                                }
                                """));
    }

    @Test
    public void testNoChangeOfNestedArrays() {
      UpdateOperation oper =
          UpdateOperator.SET.resolveOperation(objectFromJson("{\"subdoc.array.1\": 42 }"));
      ObjectNode doc =
          objectFromJson(
              """
                                {
                                  "subdoc" : {
                                    "array" : [ false, 42]
                                  }
                                }
                                """);
      ObjectNode exp = doc.deepCopy();

      // Should indicate NO change; as well as, well, not change :)
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      assertThat(doc).isEqualTo(exp);
    }

    @Test
    public void testMixedNested() {
      ObjectNode doc =
          objectFromJson(
              """
                      {
                        "array": [
                            137,
                            { "y" : 2, "subarray" : [ ] }
                        ],
                        "subdoc" : {
                            "x" : 5
                        }
                      }
                  """);

      UpdateOperation oper =
          UpdateOperator.SET.resolveOperation(
              objectFromJson(
                  """
                       {
                          "array.0": true,
                          "array.1.subarray.1" : -25,
                          "subdoc.x" : false,
                          "subdoc.y" : 1
                        }
                       """));
      ObjectNode exp =
          objectFromJson(
              """
                {
                  "array": [
                      true,
                      { "y": 2, "subarray": [ null, -25 ] }
                  ],
                  "subdoc" : {
                      "x": false,
                      "y": 1
                  }
                }
            """);

      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      assertThat(doc).isEqualTo(exp);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class invalidCasesRoot {
    @Test
    public void testNoReplacingDocId() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.SET.resolveOperation(
                    objectFromJson(
                        """
                                  { "property" : 1, "_id" : 1 }
                                  """));
              });
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .withFailMessage("Should throw exception on $set of _id")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID)
          .hasMessage(ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": $set");
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class invalidCasesNested {
    @Test
    public void testNoPathThroughAtomics() {
      UpdateOperation oper =
          UpdateOperator.SET.resolveOperation(objectFromJson("{ \"a.x\" : 12 }"));
      ObjectNode doc = objectFromJson("{ \"a\" : null }");
      Exception e = catchException(() -> oper.updateDocument(doc, targetLocator));

      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": cannot create field ('x') in path 'a.x'; only OBJECT nodes have properties (got NULL)");
    }

    @Test
    public void testNoPropertyOnArray() {
      UpdateOperation oper =
          UpdateOperator.SET.resolveOperation(objectFromJson("{ \"array.x.y.z\" : 12 }"));
      ObjectNode doc = objectFromJson("{ \"array\" : [ 1, 2 ] }");
      Exception e = catchException(() -> oper.updateDocument(doc, targetLocator));

      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": cannot create field ('x') in path 'array.x.y.z'; only OBJECT nodes have properties (got ARRAY)");
    }
  }
}
