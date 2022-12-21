package io.stargate.sgv3.docsapi.service.shredding;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

// Simple unit test with no injection needed:
public class JSONPathTest {
  @Test
  public void rootPathViaBuilder() {
    JSONPath.Builder b = JSONPath.rootBuilder();

    // Special case first: root builder produces placeholder for root:
    assertThat(b.build().toString()).isEqualTo("$");

    // But more importantly can handle Object paths
    b.property("a");
    assertThat(b.build().toString()).isEqualTo("a");
    b.property("b2");
    assertThat(b.build().toString()).isEqualTo("b2");

    // As well as Array paths
    b.index(0);
    assertThat(b.build().toString()).isEqualTo("[0]");
    b.index(1);
    assertThat(b.build().toString()).isEqualTo("[1]");
    b.index(999);
    assertThat(b.build().toString()).isEqualTo("[999]");
  }

  @Test
  public void nestedPathViaBuilder() {
    JSONPath.Builder b = JSONPath.rootBuilder();
    b.property("root3");
    JSONPath.Builder b2 = b.nestedValueBuilder();

    assertThat(b2.property("propX").build().toString()).isEqualTo("root3.propX");
    assertThat(b2.index(12).build().toString()).isEqualTo("root3.[12]");
    assertThat(b2.property("with.dot").build().toString()).isEqualTo("root3.with\\.dot");

    JSONPath.Builder b3 = b.nestedValueBuilder().index(5).nestedValueBuilder();
    assertThat(b3.index(9).build().toString()).isEqualTo("root3.[5].[9]");

    // Builders are stateful so 'b3' has its last configuration that we can use:
    JSONPath.Builder b4 = b3.nestedValueBuilder().property("leaf").nestedValueBuilder();
    assertThat(b4.build().toString()).isEqualTo("root3.[5].[9].leaf");
  }
}
