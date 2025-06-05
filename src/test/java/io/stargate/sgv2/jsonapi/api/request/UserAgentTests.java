package io.stargate.sgv2.jsonapi.api.request;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class UserAgentTests {

  @Test
  public void shouldCompareCaseInsensitive() {
    var ua1 = new UserAgent("LangChain/0.3.49");
    var ua2 = new UserAgent("langchain/0.3.49");
    var ua3 = new UserAgent("LANGCHAIN/0.3.49");

    // equals
    assertThat(ua1).isEqualTo(ua2);
    assertThat(ua2).isEqualTo(ua3);
    assertThat(ua1).isEqualTo(ua3);

    // hashCode
    assertThat(ua1.hashCode()).isEqualTo(ua2.hashCode());
    assertThat(ua2.hashCode()).isEqualTo(ua3.hashCode());
  }

  @ParameterizedTest
  @MethodSource("userAgentExamples")
  public void shouldExtractCorrectProduct(String rawAgent, String expectedProduct) {
    var userAgent = new UserAgent(rawAgent);
    assertThat(userAgent.product()).isEqualTo(expectedProduct);
  }

  static Stream<Arguments> userAgentExamples() {
    return Stream.of(
        Arguments.of("langchain/0.3.49 langchain_vectorstore/0.6.0 astrapy/2.0.1", "langchain"),
        Arguments.of("astrapy/2.0.1", "astrapy"),
        Arguments.of("langflow/1.4.2 langchain/0.3.59 langchain_vectorstore/0.6.0 astrapy/2.0.1", "langflow"),
        Arguments.of("NoVersionString", "NoVersionString"),
        Arguments.of("", ""),
        Arguments.of(null, ""),
        Arguments.of("Datastax-SLA-Checker", "Datastax-SLA-Checker")
    );
  }
}