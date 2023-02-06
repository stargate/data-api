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
import java.util.Arrays;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UnsetOperationTest extends UpdateOperationTestBase {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class happyPath {
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
          .hasFieldOrPropertyWithValue("paths", Arrays.asList("a", "b"));
      // Should indicate document being modified
      ObjectNode doc = defaultTestDocABC();
      assertThat(oper.updateDocument(doc)).isTrue();
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
          .hasFieldOrPropertyWithValue("paths", Arrays.asList("missing", "nosuchvalue"));
      ObjectNode doc = defaultTestDocABC();
      // No modifications
      assertThat(oper.updateDocument(doc)).isFalse();
      // and be left with same as original (but get a new copy just to make sure)
      assertThat(doc).isEqualTo(defaultTestDocABC());
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
