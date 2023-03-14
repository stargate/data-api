package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class MinMaxOperationTest extends UpdateOperationTestBase {
  @Nested
  class HappyPathMin {
    @Test
    public void testSimpleMinRoot() {
      ObjectNode doc = objectFromJson("{ \"x\": 1, \"y\":2}");
      // 3 updates: 2 for existing property, one for not
      UpdateOperation oper =
          UpdateOperator.MIN.resolveOperation(objectFromJson("{ \"x\": -1, \"y\":99, \"z\":0}"));
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected = objectFromJson("{ \"x\": -1, \"y\":2, \"z\":0}");
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testSimpleMinNested() {
      ObjectNode doc = objectFromJson("{ \"subdoc\":{\"x\": \"abc\", \"y\":\"def\"}}");
      // 3 updates: 2 for existing, 1 for non-existing
      UpdateOperation oper =
          UpdateOperator.MIN.resolveOperation(
              objectFromJson(
                  "{ \"subdoc.x\": \"afx\", \"subdoc.y\":\"\", \"subdoc.z\":\"value\"}"));
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson("{\"subdoc\":{\"x\": \"abc\", \"y\":\"\", \"z\":\"value\"}}");
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testMinNoChanges() {
      ObjectNode orig = objectFromJson("{ \"a\":1, \"b\":true}");
      ObjectNode doc = orig.deepCopy();
      UpdateOperation oper =
          UpdateOperator.MIN.resolveOperation(objectFromJson("{\"a\":2, \"b\":true }"));
      assertThat(oper.updateDocument(doc)).isFalse();
      assertThat(doc).isEqualTo(orig);
    }

    @Test
    public void testMinMixedTypes() {
      ObjectNode doc = objectFromJson("{ \"a\":1, \"b\":true}");
      UpdateOperation oper =
          UpdateOperator.MIN.resolveOperation(objectFromJson("{\"a\":\"value\", \"b\":123 }"));
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected = objectFromJson("{ \"a\":1, \"b\":123}");
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testMinWithArray() {
      ObjectNode doc = objectFromJson("{ \"a\":[1, true]}");
      UpdateOperation oper =
          UpdateOperator.MIN.resolveOperation(objectFromJson("{\"a\":[1, false] }"));
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected = objectFromJson("{\"a\":[1, false] }");
      assertThat(doc).isEqualTo(expected);
    }
  }

  @Nested
  class HappyPathMax {
    @Test
    public void testSimpleMaxRoot() {
      ObjectNode doc = objectFromJson("{ \"x\": 1, \"y\":2}");
      // 3 updates: 2 for existing property, one for not
      UpdateOperation oper =
          UpdateOperator.MAX.resolveOperation(objectFromJson("{ \"x\": -1, \"y\":99, \"z\":0}"));
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected = objectFromJson("{ \"x\": 1, \"y\":99, \"z\":0}");
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testSimpleMaxNested() {
      ObjectNode doc = objectFromJson("{ \"subdoc\":{\"x\": \"abc\", \"y\":\"def\"}}");
      // 3 updates: 2 for existing, 1 for non-existing
      UpdateOperation oper =
          UpdateOperator.MAX.resolveOperation(
              objectFromJson(
                  "{ \"subdoc.x\": \"afx\", \"subdoc.y\":\"\", \"subdoc.z\":\"value\"}"));
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected =
          objectFromJson("{\"subdoc\":{\"x\": \"afx\", \"y\":\"def\", \"z\":\"value\"}}");
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testMaxNoChanges() {
      ObjectNode orig = objectFromJson("{ \"a\":1, \"b\":true}");
      ObjectNode doc = orig.deepCopy();
      UpdateOperation oper =
          UpdateOperator.MAX.resolveOperation(objectFromJson("{\"a\":0, \"b\":true }"));
      assertThat(oper.updateDocument(doc)).isFalse();
      assertThat(doc).isEqualTo(orig);
    }

    @Test
    public void testMaxMixedTypes() {
      ObjectNode doc = objectFromJson("{ \"a\":1, \"b\":true}");
      UpdateOperation oper =
          UpdateOperator.MAX.resolveOperation(objectFromJson("{\"a\":\"value\", \"b\":123 }"));
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected = objectFromJson("{ \"a\":\"value\", \"b\":true}");
      assertThat(doc).isEqualTo(expected);
    }

    @Test
    public void testMaxWithArray() {
      ObjectNode doc = objectFromJson("{ \"arr\":[1, 2]}");
      UpdateOperation oper =
          UpdateOperator.MAX.resolveOperation(objectFromJson("{\"arr\":[1, 2, 3] }"));
      assertThat(oper.updateDocument(doc)).isTrue();
      ObjectNode expected = objectFromJson("{\"arr\":[1, 2, 3] }");
      assertThat(doc).isEqualTo(expected);
    }
  }
}
