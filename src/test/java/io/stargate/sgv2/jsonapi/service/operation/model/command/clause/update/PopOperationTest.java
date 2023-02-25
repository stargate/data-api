package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.PopOperation;
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
public class PopOperationTest extends UpdateOperationTestBase {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class HappyPathRoot {
    @Test
    public void testSimplePopFirstFromExisting() {
      UpdateOperation oper =
          UpdateOperator.POP.resolveOperation(objectFromJson("{ \"array\" : -1 }"));
      assertThat(oper).isInstanceOf(PopOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ 1, 2, 3 ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                                { "a" : 1, "array" : [ 2, 3 ] }
                                """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testSimplePopLastFromExisting() {
      UpdateOperation oper =
          UpdateOperator.POP.resolveOperation(objectFromJson("{ \"array\" : 1 }"));
      assertThat(oper).isInstanceOf(PopOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ 1, 2, 3 ] }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
                                { "a" : 1, "array" : [ 1, 2 ] }
                                """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testSimplePopFirstFromEmpty() {
      UpdateOperation oper =
          UpdateOperator.POP.resolveOperation(objectFromJson("{ \"array\" : -1 }"));
      assertThat(oper).isInstanceOf(PopOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ ] }");
      ObjectNode expected = doc.deepCopy();
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testSimplePopLastFromEmpty() {
      UpdateOperation oper =
          UpdateOperator.POP.resolveOperation(objectFromJson("{ \"array\" : 1 }"));
      assertThat(oper).isInstanceOf(PopOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"array\" : [ ] }");
      ObjectNode expected = doc.deepCopy();
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testSimplePopFirstFromNonExisting() {
      UpdateOperation oper =
          UpdateOperator.POP.resolveOperation(objectFromJson("{ \"newArray\" : -1 }"));
      assertThat(oper).isInstanceOf(PopOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\" : 1}");
      ObjectNode expected = doc.deepCopy();
      // No changes
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testSimplePopLastFromNonExisting() {
      UpdateOperation oper =
          UpdateOperator.POP.resolveOperation(objectFromJson("{ \"newArray\" : 1 }"));
      assertThat(oper).isInstanceOf(PopOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\" : 1}");
      ObjectNode expected = doc.deepCopy();
      // No changes
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      assertThat(doc).isEqualTo(expected);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class HappyPathNested {
    @Test
    public void testNestedPopFromExisting() {
      UpdateOperation oper =
          UpdateOperator.POP.resolveOperation(objectFromJson("{ \"subdoc.array\" : -1 }"));
      assertThat(oper).isInstanceOf(PopOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"subdoc\" : { \"array\" : [ 1, 2, 3 ] } }");
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected =
          objectFromJson(
              """
              { "a" : 1, "subdoc": { "array" : [ 2, 3 ] } }
              """);
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testNestedPopFromEmpty() {
      UpdateOperation oper =
          UpdateOperator.POP.resolveOperation(objectFromJson("{ \"subdoc.array\" : 1 }"));
      assertThat(oper).isInstanceOf(PopOperation.class);
      ObjectNode doc = objectFromJson("{ \"subdoc\" : { \"array\" : [ ] } }");
      ObjectNode expected = doc.deepCopy();
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testNestedPopFromNonExisting() {
      UpdateOperation oper =
          UpdateOperator.POP.resolveOperation(objectFromJson("{ \"subdoc.array\" : -1 }"));
      assertThat(oper).isInstanceOf(PopOperation.class);
      ObjectNode doc = objectFromJson("{ \"a\" : 1, \"doc\" : { } }");
      ObjectNode expected = doc.deepCopy();
      // No changes
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      assertThat(doc).isEqualTo(expected);

      // But let's verify longer nesting too
      oper = UpdateOperator.POP.resolveOperation(objectFromJson("{ \"a.b.c.d\" : -1 }"));
      doc = objectFromJson("{ }");
      // No changes here either
      assertThat(oper.updateDocument(doc, targetLocator)).isFalse();
      assertThat(doc).isEqualTo(objectFromJson("{ }"));
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FailingCases {
    @Test
    public void nonNumberParamForPop() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.POP.resolveOperation(objectFromJson("{\"array\" : \"text\"}"));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                  + ": $pop requires NUMBER argument (-1 or 1), instead got: STRING");
    }

    @Test
    public void wrongNumberParamForPop() {
      Exception e =
          catchException(
              () -> {
                UpdateOperator.POP.resolveOperation(objectFromJson("{\"array\" : 0}"));
              });
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM)
          .hasMessageStartingWith(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                  + ": $pop requires argument of -1 or 1, instead got: 0");
    }

    @Test
    public void testPopFromNonArrayRootProperty() {
      ObjectNode doc = objectFromJson("{ \"a\": 175 }");
      UpdateOperation oper = UpdateOperator.POP.resolveOperation(objectFromJson("{ \"a\": 1 }"));
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
                  + ": $pop requires target to be ARRAY; value at 'a' of type NUMBER");
    }

    @Test
    public void testPopFromNonArrayNestedProperty() {
      ObjectNode doc = objectFromJson("{ \"subdoc\" : [ true, false ] }");
      UpdateOperation oper =
          UpdateOperator.POP.resolveOperation(objectFromJson("{ \"subdoc.1\": 1 }"));
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
                  + ": $pop requires target to be ARRAY; value at 'subdoc.1' of type BOOLEAN");
    }
  }
}
