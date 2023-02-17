package io.stargate.sgv2.jsonapi.service.shredding;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

// Simple unit test with no injection needed:
public class JsonPathTest {
  @Nested
  class Builder {

    @Test
    public void rootPropertyPathViaBuilder() {
      JsonPath.Builder b = JsonPath.rootBuilder();
      assertThat(b).hasFieldOrPropertyWithValue("inArray", false);

      // Special case first: should not be used for anything but if it is,
      // has to result in "empty" (
      assertThat(b.build().toString()).isEqualTo("");

      // But more importantly can handle Object paths
      b.property("a");
      assertThat(b).hasFieldOrPropertyWithValue("inArray", false);
      assertThat(b.build().toString()).isEqualTo("a");
      b.property("b2");
      assertThat(b.build().toString()).isEqualTo("b2");
      b.property("with.dot");
      assertThat(b.build().toString()).isEqualTo("with.dot");
      b.property("[[bracketed]]");
      assertThat(b.build().toString()).isEqualTo("[[bracketed]]");
    }

    @Test
    public void nestedPropertyPathViaBuilder() {
      JsonPath.Builder b = JsonPath.rootBuilder();
      b.property("props");
      assertThat(b).hasFieldOrPropertyWithValue("inArray", false);
      JsonPath.Builder b2 = b.nestedObjectBuilder();
      assertThat(b2).hasFieldOrPropertyWithValue("inArray", false);
      assertThat(b2.property("propX").build().toString()).isEqualTo("props.propX");

      JsonPath.Builder b3 = b.nestedArrayBuilder().index(12);
      assertThat(b3).hasFieldOrPropertyWithValue("inArray", true);
      assertThat(b3.build().toString()).isEqualTo("props.12");
      assertThat(b2.property("with.dot").build().toString()).isEqualTo("props.with.dot");
    }

    @Test
    public void nestedArrayPathViaBuilder() {
      JsonPath.Builder b = JsonPath.rootBuilder();
      b.property("arrays");
      JsonPath.Builder b2 = b.nestedArrayBuilder().index(5).nestedArrayBuilder();
      assertThat(b2.build().toString()).isEqualTo("arrays.5");
      assertThat(b2.index(9).build().toString()).isEqualTo("arrays.5.9");

      // Builders are stateful so 'b3' has its last configuration that we can use:
      JsonPath.Builder b3 = b2.nestedObjectBuilder().property("leaf");
      assertThat(b3.build().toString()).isEqualTo("arrays.5.9.leaf");

      b.property("arr[0]");
      b2 = b.nestedArrayBuilder().index(3);
      assertThat(b2.build().toString()).isEqualTo("arr[0].3");
    }
  }
}
