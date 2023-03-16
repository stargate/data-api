package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.RenameOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class RenameOperationTest extends UpdateOperationTestBase {
  @Nested
  class HappyPathRoot {
    @Test
    public void testSimpleRenameOfExisting() {
      UpdateOperation oper =
          UpdateOperator.RENAME.resolveOperation(objectFromJson("{\"a\":\"b\"}"));
      assertThat(oper).isInstanceOf(RenameOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\": 1 }");
      // Should indicate document being modified
      assertThat(oper.updateDocument(doc)).isTrue();
      assertThat(doc).isEqualTo(fromJson("{\"b\": 1}"));
    }

    @Test
    public void testSimpleRenameOfNonExisting() {
      UpdateOperation oper =
          UpdateOperator.RENAME.resolveOperation(objectFromJson("{\"a\":\"b\"}"));
      ObjectNode doc = objectFromJson("{ \"b\": 3 }");
      // Nothing to rename, no change
      assertThat(oper.updateDocument(doc)).isFalse();
      assertThat(doc).isEqualTo(fromJson("{\"b\": 3}"));
    }
  }

  @Nested
  class HappyPathNestedObjects {
    @Test
    public void testRenameOfExistingNested() {
      UpdateOperation oper =
          UpdateOperator.RENAME.resolveOperation(objectFromJson("{\"nested.x\":\"renamed.y\"}"));
      ObjectNode doc =
          objectFromJson(
              """
      { "nested" :
         {
           "x": true
         }
      }
      """);
      assertThat(oper.updateDocument(doc)).isTrue();
      // Will leave empty Object after removing the only property:
      assertThat(doc)
          .isEqualTo(
              fromJson(
                  """
      {
        "nested": { },
        "renamed" :
         {
           "y": true
         }
      }
      """));
    }

    @Test
    public void testRenameOfMissingNested() {
      UpdateOperation oper =
          UpdateOperator.RENAME.resolveOperation(objectFromJson("{\"nested.x\":\"renamed.y\"}"));
      ObjectNode doc =
          objectFromJson(
              """
      { "nested" :
         {
           "y": true
         }
      }
      """);
      ObjectNode origDoc = doc.deepCopy();
      // No source property, no change
      assertThat(oper.updateDocument(doc)).isFalse();
      assertThat(doc).isEqualTo(origDoc);
    }
  }

  // No Array as Source or Destination
  @Nested
  class InvalidCasesArrays {
    @Test
    public void invalidRenameSourceArray() {
      UpdateOperation oper =
          UpdateOperator.RENAME.resolveOperation(objectFromJson("{\"array.0\":\"x\"}"));
      ObjectNode doc = objectFromJson("{\"array\" : [1, 2]}");
      Exception e = catchException(() -> oper.updateDocument(doc));

      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": $rename does not allow ARRAY field as source ('array.0')");
    }

    @Test
    public void invalidRenameDestinationArray() {
      UpdateOperation oper =
          UpdateOperator.RENAME.resolveOperation(objectFromJson("{\"x\":\"array.0\"}"));
      ObjectNode doc = objectFromJson("{\"x\":3, \"array\" : [1]}");
      Exception e = catchException(() -> oper.updateDocument(doc));

      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": $rename does not allow ARRAY field as destination ('array.0')");
    }
  }

  @Nested
  class InvalidCasesRoot {
    @Test
    public void testNoRenamingDocId() {
      Exception e =
          catchException(
              () -> UpdateOperator.RENAME.resolveOperation(objectFromJson("{ \"_id\": \"id\" }")));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID)
          .hasMessage(ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": $rename");
    }

    @Test
    public void testNoOverwritingDocId() {
      Exception e =
          catchException(
              () -> UpdateOperator.RENAME.resolveOperation(objectFromJson("{ \"id\": \"_id\" }")));

      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID)
          .hasMessage(ErrorCode.UNSUPPORTED_UPDATE_FOR_DOC_ID.getMessage() + ": $rename");
    }
  }
}
