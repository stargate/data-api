package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CurrentDateOperationTest extends UpdateOperationTestBase {
  @Nested
  class HappyPathCurrentDate {
    @Test
    public void simpleRoot() {
      ObjectNode doc = objectFromJson("{ \"createdAt\": 0}");
      UpdateOperation oper =
          UpdateOperator.CURRENT_DATE.resolveOperation(
              objectFromJson("{ \"createdAt\": true, \"updatedAt\": true}"));
      final long startTime = System.currentTimeMillis();
      assertThat(oper.updateDocument(doc)).isTrue();
      assertThat(doc).hasSize(2);

      verifyApproximateDate(startTime, doc.path("createdAt"));
      verifyApproximateDate(startTime, doc.path("updatedAt"));
    }

    @Test
    public void simpleNested() {
      ObjectNode doc = objectFromJson("{ \"item1\":{\"a\":123}}");
      // 2 updates: 1 for existing, 0 for non-existing
      UpdateOperation oper =
          UpdateOperator.CURRENT_DATE.resolveOperation(
              objectFromJson("{ \"item1.a\": true, \"item2.a\":true}"));
      final long startTime = System.currentTimeMillis();
      assertThat(oper.updateDocument(doc)).isTrue();
      assertThat(doc).hasSize(2);

      verifyApproximateDate(startTime, doc.at("/item1/a"));
      verifyApproximateDate(startTime, doc.at("/item2/a"));
    }

    @Test
    public void currentDateInArray() {
      ObjectNode doc = objectFromJson("{ \"a\":[1, true]}");
      UpdateOperation oper =
          UpdateOperator.CURRENT_DATE.resolveOperation(objectFromJson("{\"a.1\":true }"));
      final long startTime = System.currentTimeMillis();
      assertThat(oper.updateDocument(doc)).isTrue();
      assertThat(doc).hasSize(1);

      verifyApproximateDate(startTime, doc.at("/a/1"));
    }

    private void verifyApproximateDate(final long startTime, JsonNode dateValue) {
      assertThat(dateValue).isInstanceOf(ObjectNode.class);
      assertThat(dateValue).hasSize(1);
      JsonNode datePart = dateValue.path("$date");
      assertThat(datePart.isIntegralNumber()).isTrue();
      final long now = System.currentTimeMillis();
      assertThat(datePart.longValue()).isBetween(startTime, now);
    }
  }

  @Nested
  class FailureCases {
    @Test
    public void testInvalidArgumentString() {
      Exception e =
          catchException(
              () ->
                  UpdateOperator.CURRENT_DATE.resolveOperation(
                      objectFromJson("{ \"x\": \"foobar\"}")));
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                  + ": $currentDate requires argument of either `true` or `{\"$type\":\"date\"}`, got: `\"foobar\"`");
    }

    @Test
    public void testInvalidArgumentFalse() {
      Exception e =
          catchException(
              () ->
                  UpdateOperator.CURRENT_DATE.resolveOperation(objectFromJson("{ \"x\": false}")));
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                  + ": $currentDate requires argument of either `true` or `{\"$type\":\"date\"}`, got: `false`");
    }

    @Test
    public void testInvalidArgumentObject() {
      Exception e =
          catchException(
              () ->
                  UpdateOperator.CURRENT_DATE.resolveOperation(
                      objectFromJson("{ \"x\": {\"value\":1}}")));
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                  + ": $currentDate requires argument of either `true` or `{\"$type\":\"date\"}`, got: `{\"value\":1}`");
    }
  }
}
