package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateTarget;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateTargetLocator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UpdateTargetLocatorTest extends UpdateOperationTestBase {
  protected UpdateTargetLocator targetLocator = new UpdateTargetLocator();

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class happyPathFindIfExists {
    @Test
    public void findRootPropertyPath() {
      ObjectNode doc = objectFromJson("{\"a\" : 1 }");
      UpdateTarget target = targetLocator.findIfExists(doc, "a");
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectMapper.getNodeFactory().numberNode(1));

      // But cannot proceed via atomic node
      target = targetLocator.findIfExists(doc, "a.x");
      assertThat(target.contextNode()).isNull();
      assertThat(target.valueNode()).isNull();

      // Can refer to missing property as well
      target = targetLocator.findIfExists(doc, "unknown");
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isEqualTo("unknown");
    }

    @Test
    public void findRootIndexPath() {
      // Although main-level is always an Object, locator is not limited
      // and can refer to array indexes too
      JsonNode doc = fromJson("[ 3, 7 ]");
      UpdateTarget target = targetLocator.findIfExists(doc, "1");
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectMapper.getNodeFactory().numberNode(7));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(1);

      // May try to reference past end, no match
      target = targetLocator.findIfExists(doc, "9");
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(9);
    }

    @Test
    public void findNestedPropertyPath() {
      ObjectNode doc =
          objectFromJson(
              """
                    {
                      "b" : {
                         "c" : true
                      }
                    }
                    """);

      // First: simple nested property:
      UpdateTarget target = targetLocator.findIfExists(doc, "b.c");
      assertThat(target.contextNode()).isSameAs(doc.get("b"));
      assertThat(target.valueNode()).isEqualTo(fromJson("true"));
      assertThat(target.lastProperty()).isEqualTo("c");

      // But can also refer to its parent
      target = targetLocator.findIfExists(doc, "b");
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectFromJson("{\"c\":true}"));
      assertThat(target.lastProperty()).isEqualTo("b");

      // Or to missing property within existing Object:
      target = targetLocator.findIfExists(doc, "b.unknown");
      assertThat(target.contextNode()).isSameAs(doc.get("b"));
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isEqualTo("unknown");

      // But with deeper missing path, no more context
      target = targetLocator.findIfExists(doc, "b.unknown.bogus");
      assertThat(target.contextNode()).isNull();
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isNull();
    }

    @Test
    public void findNestedArrayPath() {
      ObjectNode doc =
          objectFromJson(
              """
                              {
                                "array" : [ 1, 2,
                                   { "a": 3,
                                      "subArray" : [ true, false ]
                                    }
                                 ]
                              }
                              """);

      // First, existing path
      UpdateTarget target = targetLocator.findIfExists(doc, "array.0");
      assertThat(target.contextNode()).isSameAs(doc.get("array"));
      assertThat(target.valueNode()).isEqualTo(doc.numberNode(1));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(0);

      // Then non-existing index, has context (could add)
      target = targetLocator.findIfExists(doc, "array.5");
      assertThat(target.contextNode()).isSameAs(doc.get("array"));
      assertThat(target.valueNode()).isNull();

      // and then non-existing property (no properties in Array); no context (not legal to set)
      target = targetLocator.findIfExists(doc, "array.prop");
      assertThat(target.contextNode()).isNull();
      assertThat(target.valueNode()).isNull();

      // But we can traverse through multiple nesting levels:
      target = targetLocator.findIfExists(doc, "array.2.subArray.1");
      assertThat(target.contextNode()).isSameAs(doc.at("/array/2/subArray"));
      assertThat(target.valueNode()).isEqualTo(doc.booleanNode(false));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(1);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class failForFindIfExists {
    @Test
    public void invalidEmptySegment() {
      Exception e = catchException(() -> targetLocator.findIfExists(objectFromJson("{}"), "a..x"));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": empty segment ('') in path 'a..x'");
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class happyPathFindOrCreate {
    @Test
    public void findRootPropertyPath() {
      ObjectNode doc = objectFromJson("{\"a\" : 1 }");
      UpdateTarget target = targetLocator.findOrCreate(doc, "a");
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectMapper.getNodeFactory().numberNode(1));

      // Can refer to missing property as well
      target = targetLocator.findOrCreate(doc, "unknown");
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isEqualTo("unknown");
    }

    @Test
    public void findRootIndexPath() {
      // Although main-level is always an Object, locator is not limited
      // and can refer to array indexes too
      JsonNode doc = fromJson("[ 3, 7 ]");
      UpdateTarget target = targetLocator.findOrCreate(doc, "1");
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectMapper.getNodeFactory().numberNode(7));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(1);

      // May try to reference past end, no match
      target = targetLocator.findOrCreate(doc, "9");
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(9);
    }

    @Test
    public void findNestedPropertyPath() {
      ObjectNode doc =
          objectFromJson(
              """
                              {
                                "b" : {
                                   "c" : true
                                }
                              }
                              """);

      // First: simple nested property:
      UpdateTarget target = targetLocator.findOrCreate(doc, "b.c");
      assertThat(target.contextNode()).isSameAs(doc.get("b"));
      assertThat(target.valueNode()).isEqualTo(fromJson("true"));
      assertThat(target.lastProperty()).isEqualTo("c");

      // But can also go for not eisting
      target = targetLocator.findOrCreate(doc, "b.x.y");
      // will now have created path
      assertThat(target.contextNode()).isSameAs(doc.at("/b/x"));
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isEqualTo("y");
    }

    @Test
    public void findNestedArrayPath() {
      ObjectNode doc =
          objectFromJson(
              """
                              {
                                "array" : [ 1, 2,
                                   { "a": 3,
                                      "subArray" : [ true, false ]
                                    }
                                 ]
                              }
                              """);

      // First, existing path
      UpdateTarget target = targetLocator.findOrCreate(doc, "array.0");
      assertThat(target.contextNode()).isSameAs(doc.get("array"));
      assertThat(target.valueNode()).isEqualTo(doc.numberNode(1));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(0);

      // But then something past inner array's end
      target = targetLocator.findOrCreate(doc, "array.2.subArray.3");
      assertThat(target.contextNode()).isSameAs(doc.at("/array/2/subArray"));
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(3);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class failForFindOrCreate {
    @Test
    public void invalidPathViaAtomic() {
      Exception e =
          catchException(() -> targetLocator.findOrCreate(objectFromJson("{\"a\": 3}"), "a.x"));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": cannot create field ('x') in path 'a.x'; only OBJECT nodes have properties (got NUMBER)");

      e =
          catchException(
              () ->
                  targetLocator.findOrCreate(objectFromJson("{\"a\": {\"b\": null}}"), "a.b.c.d"));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": cannot create field ('c') in path 'a.b.c.d'; only OBJECT nodes have properties (got NULL)");
    }

    @Test
    public void invalidPropViaArray() {
      Exception e =
          catchException(
              () -> targetLocator.findOrCreate(objectFromJson("{\"array\": [1] }"), "array.prop"));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": cannot create field ('prop') in path 'array.prop'; only OBJECT nodes have properties (got ARRAY)");

      e =
          catchException(
              () ->
                  targetLocator.findOrCreate(
                      objectFromJson("{\"ob\":{\"array\":[{\"a2\":[true]}]}} }"),
                      "ob.array.0.a2.x"));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": cannot create field ('x') in path 'ob.array.0.a2.x'; only OBJECT nodes have properties (got ARRAY)");
    }
  }
}
