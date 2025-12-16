package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.List;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ExceptionUtilTest {

  @Test
  public void checkKey() {
    String key =
        ExceptionUtil.getThrowableGroupingKey(ErrorCodeV1.INVALID_REQUEST.toApiException());
    assertThat(key).isNotNull();
    assertThat(key).isEqualTo(ErrorCodeV1.INVALID_REQUEST.name());

    key = ExceptionUtil.getThrowableGroupingKey(new RuntimeException(""));
    assertThat(key).isNotNull();
    assertThat(key).isEqualTo("RuntimeException");
  }

  @Test
  public void getError() {
    String message = "test error for ids %s: %s";
    List<DocumentId> ids = List.of(DocumentId.fromString("doc1"), DocumentId.fromString("doc2"));

    // Arbitrary error code chosen for testing purposes
    ErrorCodeV1 errorCode = ErrorCodeV1.INVALID_REQUEST;
    Exception throwable = errorCode.toApiException();
    CommandResult.Error error = ExceptionUtil.getError(message, ids, throwable);
    assertThat(error).isNotNull();
    assertThat(error)
        .satisfies(
            err -> {
              assertThat(err.message())
                  .isEqualTo("test error for ids ['doc1', 'doc2']: " + errorCode.getMessage());
              assertThat(err.fields()).containsEntry("exceptionClass", "JsonApiException");
              assertThat(err.fields()).containsEntry("errorCode", errorCode.name());
            });

    throwable = new RuntimeException("Some error");
    error = ExceptionUtil.getError(message, ids, throwable);
    assertThat(error).isNotNull();
    assertThat(error)
        .satisfies(
            err -> {
              assertThat(err.message())
                  .isEqualTo(
                      "Server failed: root cause: (java.lang.RuntimeException) test error for ids ['doc1', 'doc2']: Some error");
              assertThat(err.fields())
                  .containsEntry("errorCode", "SERVER_UNHANDLED_ERROR")
                  .containsEntry("exceptionClass", "JsonApiException");
            });
  }
}
