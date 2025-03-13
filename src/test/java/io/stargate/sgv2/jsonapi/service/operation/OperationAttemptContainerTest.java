package io.stargate.sgv2.jsonapi.service.operation;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class OperationAttemptContainerTest {

  private static final TestData TEST_DATA = new TestData();

  /**
   * Mock 5 OperationAttempt 'a','b','c','d','e'. OperationAttempt 'c' is in error status. Then
   * 'c','d','e' should fail fast when sequential processing. shouldFailFast(a) -> false
   * shouldFailFast(b) -> false shouldFailFast(c) -> true shouldFailFast(d) -> true
   * shouldFailFast(e) -> true
   */
  @Test
  public void failFastWithErrorInTheMiddle() {
    final OperationAttemptContainer<TableSchemaObject, TestOperationAttempt>
        operationAttemptContainer = new OperationAttemptContainer<>(true);

    // Map value indicates this attempt should failfast or not.
    LinkedHashMap<TestOperationAttempt, Boolean> operationAttempts =
        new LinkedHashMap<>() {
          {
            put(mockOperationAttemptWithError(null), false);
            put(mockOperationAttemptWithError(null), false);
            put(
                mockOperationAttemptWithError(
                    new RuntimeException("Exception for operationAttempt 'c'")),
                true);
            put(mockOperationAttemptWithError(null), true);
            put(mockOperationAttemptWithError(null), true);
          }
        };

    operationAttemptContainer.addAll(operationAttempts.sequencedKeySet());

    for (Map.Entry<TestOperationAttempt, Boolean> entry : operationAttempts.entrySet()) {
      assertThat(operationAttemptContainer.shouldFailFast(entry.getKey()))
          .isEqualTo(entry.getValue());
    }
  }

  /**
   * Mock 5 OperationAttempt 'a','b','c','d','e'. OperationAttempt 'b' and 'd' are in error status.
   * Then 'b', 'c','d','e' should fail fast when sequential processing. Since 'b' ahead of
   * 'c','d','e' in the order, so any attempt behind of 'b' will also fail-fast.
   */
  @Test
  public void failFastWithMultipleError() {
    final OperationAttemptContainer<TableSchemaObject, TestOperationAttempt>
        operationAttemptContainer = new OperationAttemptContainer<>(true);

    // Map value indicates this attempt should failfast or not.
    LinkedHashMap<TestOperationAttempt, Boolean> operationAttempts =
        new LinkedHashMap<>() {
          {
            put(mockOperationAttemptWithError(null), false);
            put(
                mockOperationAttemptWithError(
                    new RuntimeException("Exception for operationAttempt 'b'")),
                true);
            put(mockOperationAttemptWithError(null), true);
            put(
                mockOperationAttemptWithError(
                    new RuntimeException("Exception for operationAttempt 'd'")),
                true);
            put(mockOperationAttemptWithError(null), true);
          }
        };

    operationAttemptContainer.addAll(operationAttempts.sequencedKeySet());

    for (Map.Entry<TestOperationAttempt, Boolean> entry : operationAttempts.entrySet()) {
      assertThat(operationAttemptContainer.shouldFailFast(entry.getKey()))
          .isEqualTo(entry.getValue());
    }
  }

  /**
   * Mock 5 OperationAttempts 'a','b','c','d','e'. OperationAttempt 'a' is in error status. Then
   * every attempt should fail fast when sequential processing.
   */
  @Test
  public void failFastWithErrorInTheBeginning() {
    final OperationAttemptContainer<TableSchemaObject, TestOperationAttempt>
        operationAttemptContainer = new OperationAttemptContainer<>(true);

    // Map value indicates this attempt should failfast or not.
    LinkedHashMap<TestOperationAttempt, Boolean> operationAttempts =
        new LinkedHashMap<>() {
          {
            put(
                mockOperationAttemptWithError(
                    new RuntimeException("Exception for operationAttempt 'a'")),
                true);
            put(mockOperationAttemptWithError(null), true);
            put(mockOperationAttemptWithError(null), true);
            put(mockOperationAttemptWithError(null), true);
            put(mockOperationAttemptWithError(null), true);
          }
        };

    operationAttemptContainer.addAll(operationAttempts.sequencedKeySet());

    for (Map.Entry<TestOperationAttempt, Boolean> entry : operationAttempts.entrySet()) {
      assertThat(operationAttemptContainer.shouldFailFast(entry.getKey()))
          .isEqualTo(entry.getValue());
    }
  }

  /**
   * Mock 5 OperationAttempts 'a','b','c','d','e'. OperationAttempt 'e' is in error status. Then
   * since 'e' is at last, only 'e' will fail-fast for this batch when sequential processing.
   */
  @Test
  public void failFastWithErrorInTheEnd() {
    final OperationAttemptContainer<TableSchemaObject, TestOperationAttempt>
        operationAttemptContainer = new OperationAttemptContainer<>(true);

    // Map value indicates this attempt should failfast or not.
    LinkedHashMap<TestOperationAttempt, Boolean> operationAttempts =
        new LinkedHashMap<>() {
          {
            put(mockOperationAttemptWithError(null), false);
            put(mockOperationAttemptWithError(null), false);
            put(mockOperationAttemptWithError(null), false);
            put(mockOperationAttemptWithError(null), false);
            put(
                mockOperationAttemptWithError(
                    new RuntimeException("Exception for operationAttempt 'e'")),
                true);
          }
        };

    operationAttemptContainer.addAll(operationAttempts.sequencedKeySet());

    for (Map.Entry<TestOperationAttempt, Boolean> entry : operationAttempts.entrySet()) {
      assertThat(operationAttemptContainer.shouldFailFast(entry.getKey()))
          .isEqualTo(entry.getValue());
    }
  }

  private TestOperationAttempt mockOperationAttemptWithError(Exception e) {
    var fixture = TEST_DATA.operationAttempt().emptyFixture();
    return (TestOperationAttempt) fixture.attempt().maybeAddFailure(e).attempt().target;
  }
}
