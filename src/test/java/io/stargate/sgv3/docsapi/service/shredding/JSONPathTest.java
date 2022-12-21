package io.stargate.sgv3.docsapi.service.shredding;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

// Simple unit test with no injection needed:
public class JSONPathTest {
  @Nested
  class EncodePropertyName {
    @Test
    public void encodePlain() {
      assertThat(JSONPath.Builder.encodePropertyName("simple")).isEqualTo("simple");
    }

    @Test
    public void encodeDots() {
      assertThat(JSONPath.Builder.encodePropertyName("with.dot")).isEqualTo("with\\.dot");
      assertThat(JSONPath.Builder.encodePropertyName(".x.y")).isEqualTo("\\.x\\.y");
    }

    @Test
    public void encodeIndex() {
      assertThat(JSONPath.Builder.encodePropertyName("[123]")).isEqualTo("\\[123]");
    }

    @Test
    public void encodeOther() {
      assertThat(JSONPath.Builder.encodePropertyName("a\\b")).isEqualTo("a\\\\b");
    }
  }

  @Nested
  class Builder {

    @Test
    public void rootPropertyPathViaBuilder() {
      JSONPath.Builder b = JSONPath.rootBuilder();

      // Special case first: should not be used for anything but if it is,
      // has to result in "empty" (
      assertThat(b.build().toString()).isEqualTo("");

      // But more importantly can handle Object paths
      b.property("a");
      assertThat(b.build().toString()).isEqualTo("a");
      b.property("b2");
      assertThat(b.build().toString()).isEqualTo("b2");
      b.property("with.dot");
      assertThat(b.build().toString()).isEqualTo("with\\.dot");
      b.property("[[bracketed]]");
      assertThat(b.build().toString()).isEqualTo("\\[\\[bracketed]]");
      b.property("\\x");
      assertThat(b.build().toString()).isEqualTo("\\\\x");
    }

    @Test
    public void rootArrayPathViaBuilder() {
      JSONPath.Builder b = JSONPath.rootBuilder();
      b.index(0);
      assertThat(b.build().toString()).isEqualTo("[0]");
      b.index(1);
      assertThat(b.build().toString()).isEqualTo("[1]");
      b.index(999);
      assertThat(b.build().toString()).isEqualTo("[999]");
    }

    @Test
    public void nestedPropertyPathViaBuilder() {
      JSONPath.Builder b = JSONPath.rootBuilder();
      b.property("props");
      JSONPath.Builder b2 = b.nestedValueBuilder();

      assertThat(b2.property("propX").build().toString()).isEqualTo("props.propX");
      assertThat(b2.index(12).build().toString()).isEqualTo("props.[12]");
      assertThat(b2.property("with.dot").build().toString()).isEqualTo("props.with\\.dot");
    }

    @Test
    public void nestedIndexPathViaBuilder() {
      JSONPath.Builder b = JSONPath.rootBuilder();
      b.property("arrays");
      JSONPath.Builder b2 = b.nestedValueBuilder().index(5).nestedValueBuilder();
      assertThat(b2.build().toString()).isEqualTo("arrays.[5]");
      assertThat(b2.index(9).build().toString()).isEqualTo("arrays.[5].[9]");

      // Builders are stateful so 'b3' has its last configuration that we can use:
      JSONPath.Builder b3 = b2.nestedValueBuilder().property("leaf").nestedValueBuilder();
      assertThat(b3.build().toString()).isEqualTo("arrays.[5].[9].leaf");

      b.property("arr[0]");
      b2 = b.nestedValueBuilder().index(3).nestedValueBuilder();
      assertThat(b2.build().toString()).isEqualTo("arr\\[0].[3]");
    }
  }
}
