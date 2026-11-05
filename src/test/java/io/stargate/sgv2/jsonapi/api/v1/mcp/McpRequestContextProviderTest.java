package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.MDC;

class McpRequestContextProviderTest {
  private final McpRequestContextProvider provider = new McpRequestContextProvider();

  @AfterEach
  void clearContext() {
    MDC.remove(McpRequestLogging.REQUEST_ID);
    MDC.remove("unrelated");
  }

  @Test
  void capturesAtSnapshotCreationAndRestoresNestedContexts() {
    MDC.put(McpRequestLogging.REQUEST_ID, "outer");
    var outer = provider.currentContext(Map.of());
    MDC.put(McpRequestLogging.REQUEST_ID, "inner");
    var inner = provider.currentContext(Map.of());
    MDC.put(McpRequestLogging.REQUEST_ID, "previous");

    var outerScope = outer.begin();
    try {
      assertThat(MDC.get(McpRequestLogging.REQUEST_ID)).isEqualTo("outer");
      var innerScope = inner.begin();
      try {
        assertThat(MDC.get(McpRequestLogging.REQUEST_ID)).isEqualTo("inner");
      } finally {
        innerScope.endContext();
      }
      assertThat(MDC.get(McpRequestLogging.REQUEST_ID)).isEqualTo("outer");
    } finally {
      outerScope.endContext();
    }
    assertThat(MDC.get(McpRequestLogging.REQUEST_ID)).isEqualTo("previous");
  }

  @Test
  void anAbsentCapturedContextClearsAndRestoresTheCurrentMarker() {
    MDC.remove(McpRequestLogging.REQUEST_ID);
    var snapshot = provider.currentContext(Map.of());
    MDC.put(McpRequestLogging.REQUEST_ID, "previous");

    var scope = snapshot.begin();
    try {
      assertThat(MDC.get(McpRequestLogging.REQUEST_ID)).isNull();
    } finally {
      scope.endContext();
    }
    assertThat(MDC.get(McpRequestLogging.REQUEST_ID)).isEqualTo("previous");
  }

  @Test
  void clearedContextIgnoresTheCapturedMarker() {
    MDC.put(McpRequestLogging.REQUEST_ID, "captured");
    var snapshot = provider.clearedContext(Map.of());
    MDC.put(McpRequestLogging.REQUEST_ID, "previous");

    var scope = snapshot.begin();
    try {
      assertThat(MDC.get(McpRequestLogging.REQUEST_ID)).isNull();
    } finally {
      scope.endContext();
    }
    assertThat(MDC.get(McpRequestLogging.REQUEST_ID)).isEqualTo("previous");
  }

  @Test
  void restoresAnAbsentMarker() {
    MDC.put(McpRequestLogging.REQUEST_ID, "captured");
    var snapshot = provider.currentContext(Map.of());
    MDC.remove(McpRequestLogging.REQUEST_ID);

    var scope = snapshot.begin();
    try {
      assertThat(MDC.get(McpRequestLogging.REQUEST_ID)).isEqualTo("captured");
    } finally {
      scope.endContext();
    }
    assertThat(MDC.get(McpRequestLogging.REQUEST_ID)).isNull();
  }

  @Test
  void leavesOtherMdcEntriesAlone() {
    MDC.put(McpRequestLogging.REQUEST_ID, "captured");
    MDC.put("unrelated", "source");
    var snapshot = provider.currentContext(Map.of());
    MDC.put("unrelated", "worker");

    var scope = snapshot.begin();
    try {
      assertThat(MDC.get("unrelated")).isEqualTo("worker");
      MDC.put("unrelated", "changed");
    } finally {
      scope.endContext();
    }
    assertThat(MDC.get("unrelated")).isEqualTo("changed");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void reusingAWorkerRestoresItsContextAfterSuccessAndFailure(boolean fail) throws Exception {
    try (var worker = Executors.newSingleThreadExecutor()) {
      worker
          .submit(() -> MDC.put(McpRequestLogging.REQUEST_ID, "worker"))
          .get(10, TimeUnit.SECONDS);
      MDC.put(McpRequestLogging.REQUEST_ID, "first-request");
      var first = provider.currentContext(Map.of());
      var result =
          worker.submit(
              () -> {
                var scope = first.begin();
                try {
                  assertThat(MDC.get(McpRequestLogging.REQUEST_ID)).isEqualTo("first-request");
                  if (fail) {
                    throw new IllegalStateException("expected failure");
                  }
                } finally {
                  scope.endContext();
                }
              });
      if (fail) {
        assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(IllegalStateException.class);
      } else {
        result.get(10, TimeUnit.SECONDS);
      }
      assertThat(
              worker.submit(() -> MDC.get(McpRequestLogging.REQUEST_ID)).get(10, TimeUnit.SECONDS))
          .isEqualTo("worker");

      MDC.put(McpRequestLogging.REQUEST_ID, "second-request");
      var second = provider.currentContext(Map.of());
      worker
          .submit(
              () -> {
                var scope = second.begin();
                try {
                  assertThat(MDC.get(McpRequestLogging.REQUEST_ID)).isEqualTo("second-request");
                } finally {
                  scope.endContext();
                }
              })
          .get(10, TimeUnit.SECONDS);
      assertThat(
              worker.submit(() -> MDC.get(McpRequestLogging.REQUEST_ID)).get(10, TimeUnit.SECONDS))
          .isEqualTo("worker");
    }
  }
}
