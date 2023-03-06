package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UnsetOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.Set;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UnsetOperationTest extends UpdateOperationTestBase {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class happyPathRoot {
    @Test
    public void testSimpleUnsetOfExisting() {
      // Remove 2 of 3 properties:
      UpdateOperation oper =
          UpdateOperator.UNSET.resolveOperation(
              objectFromJson("""
                    { "a" : 1, "b" : 1 }
                    """));
      assertThat(oper)
          .isInstanceOf(UnsetOperation.class)
          .hasFieldOrPropertyWithValue("paths", Set.of("a", "b"));
      // Should indicate document being modified
      ObjectNode doc = defaultTestDocABC();
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      // and be left with just one property
      assertThat(doc)
          .isEqualTo(fromJson("""
                    { "c" : true }
                    """));
    }

    @Test
    public void testSimpleUnsetOfNonExisting() {
      // Try to remove 2 properties for which no value exist
      UpdateOperation oper =
          UpdateOperator.UNSET.resolveOperation(
              objectFromJson(
                  """
                    { "missing" : 1, "nosuchvalue" : 1 }
                    """));
      assertThat(oper)
          .isInstanceOf(UnsetOperation.class)
          .hasFieldOrPropertyWithValue("paths", Set.of("missing", "nosuchvalue"));
      ObjectNode doc = defaultTestDocABC();
      // No modifications
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      // and be left with same as original (but get a new copy just to make sure)
      assertThat(doc).isEqualTo(defaultTestDocABC());
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class happyPathNested {
    @Test
    public void testNestedPropertiesExist() {
      ObjectNode doc =
          objectFromJson(
              """
              {
                "subdoc" : {
                   "a": 3,
                   "b": 5,
                   "doc2" : {
                      "a": 1,
                      "c": 3
                   }
                },
                "atomic" : 3,
                "array" : [ 1, 2 ]
              }
              """);

      // Try unset of lots of things; some exists, others not; ones that don't
      // exists do not cause errors but are just ignored
      UpdateOperation oper =
          UpdateOperator.UNSET.resolveOperation(
              objectFromJson(
                  """
                              {
                                 "subdoc.a" : 1,
                                 "subdoc.doc2.c" : 1,
                                 "atomic.x" : 1,
                                 "array.x" : 1
                              }
                              """));
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();

      ObjectNode exp =
          objectFromJson(
              """
              {
                "subdoc" : {
                   "b": 5,
                   "doc2" : {
                      "a": 1
                   }
                },
                "atomic" : 3,
                "array" : [ 1, 2 ]
              }
              """);
      assertThat(doc).isEqualTo(exp);
    }

    @Test
    public void testNestedPropertiesNotExist() {
      final ObjectNode orig =
          objectFromJson(
              """
              {
                "subdoc" : {
                   "a": 3
                 }
               }
              """);
      ObjectNode doc = orig.deepCopy();

      UpdateOperation oper =
          UpdateOperator.UNSET.resolveOperation(objectFromJson("{\"subdoc.b\": 1, \"x.y\": 1 }"));
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      // and no modifications expected
      assertThat(doc).isEqualTo(orig);
    }

    @Test
    public void testNestedArrays() {
      ObjectNode doc =
          objectFromJson(
              """
              {
                "array" : [ 1, 2, {
                  "subdoc" : {
                    "a" : [ true ]
                  }
                } ],
                "atomic" : 3,
                "array2" : [ 7, 13 ]
              }
              """);

      // Try unset multiple things; as earlier, no errors results neither removal
      // for non-existing things (ar even invalid paths wrt Array/Object)
      UpdateOperation oper =
          UpdateOperator.UNSET.resolveOperation(
              objectFromJson(
                  """
                              {
                                 "array.1" : 1,
                                 "array.2.subdoc.a.0" : 1,
                                 "atomic.1.prop" : 1,
                                 "array2" : 1
                              }
                              """));
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();

      // Note: in Array values, placeholder nulls must be added (to retain index positions);
      // but replacing WHOLE array is fine (no null left)
      ObjectNode exp =
          objectFromJson(
              """
              {
                "array" : [ 1, null, {
                  "subdoc" : {
                    "a" : [ null ]
                  }
                } ],
                "atomic" : 3
              }
              """);
      assertThat(doc).isEqualTo(exp);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class invalidCases {
    @Test
    public void testNoUnsettingDocId() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.UNSET.resolveOperation(
                    objectFromJson(
                        """
                                  { "property" : 1, "_id" : 1 }
                                  """));
              });
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .withFailMessage("Should throw exception on $unset of _id")
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID)
          .hasMessage(ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": $unset");
    }
  }
}
