package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class MinMaxOperationTest extends UpdateOperationTestBase {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class HappyPathMin {
    @Test
    public void testSimpleMinRoot() {
      ObjectNode doc = objectFromJson("{ \"x\": 1, \"y\":2}");
      // 3 updates: 2 for existing property, one for not
      UpdateOperation oper =
          UpdateOperator.MIN.resolveOperation(objectFromJson("{ \"x\": -1, \"y\":99, \"z\":0}"));
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected = objectFromJson("{ \"x\": -1, \"y\":2, \"z\":0}");
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testSimpleMaxRoot() {
      ObjectNode doc = objectFromJson("{ \"x\": 1, \"y\":2}");
      // 3 updates: 2 for existing property, one for not
      UpdateOperation oper =
          UpdateOperator.MAX.resolveOperation(objectFromJson("{ \"x\": -1, \"y\":99, \"z\":0}"));
      assertThat(oper.updateDocument(doc, targetLocator)).isTrue();
      ObjectNode expected = objectFromJson("{ \"x\": 1, \"y\":99, \"z\":0}");
      assertThat(doc).isEqualTo(expected);
    }
  }
}
