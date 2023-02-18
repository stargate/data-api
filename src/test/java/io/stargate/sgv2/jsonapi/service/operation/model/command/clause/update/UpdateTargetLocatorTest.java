package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateTarget;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateTargetLocator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import javax.inject.Inject;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UpdateTargetLocatorTest extends UpdateOperationTestBase {
  @Inject protected UpdateTargetLocator targetLocator;

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
}
