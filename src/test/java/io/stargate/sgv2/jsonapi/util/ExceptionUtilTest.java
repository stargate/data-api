package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.List;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ExceptionUtilTest {

  @Test
  public void checkKey() {
    String key =
        ExceptionUtil.getThrowableGroupingKey(new JsonApiException(ErrorCode.CONCURRENCY_FAILURE));
    assertThat(key).isNotNull();
    assertThat(key).isEqualTo(ErrorCode.CONCURRENCY_FAILURE.name());

    key = ExceptionUtil.getThrowableGroupingKey(new RuntimeException(""));
    assertThat(key).isNotNull();
    assertThat(key).isEqualTo("RuntimeException");
  }

  @Test
  public void getError() {
    String message = "test error for ids %s: %s";
    List<DocumentId> ids = List.of(DocumentId.fromString("doc1"), DocumentId.fromString("doc2"));

    Exception throwable = new JsonApiException(ErrorCode.CONCURRENCY_FAILURE);
    CommandResult.Error error = ExceptionUtil.getError(message, ids, throwable);
    assertThat(error).isNotNull();
    assertThat(error)
        .satisfies(
            err -> {
              assertThat(err.message())
                  .isEqualTo(
                      "test error for ids ['doc1', 'doc2']: Unable to complete transaction due to concurrent transactions");
              assertThat(err.fields()).containsEntry("exceptionClass", "JsonApiException");
              assertThat(err.fields()).containsEntry("errorCode", "CONCURRENCY_FAILURE");
            });

    throwable = new RuntimeException("Some error");
    error = ExceptionUtil.getError(message, ids, throwable);
    assertThat(error).isNotNull();
    assertThat(error)
        .satisfies(
            err -> {
              assertThat(err.message())
                  .isEqualTo("test error for ids ['doc1', 'doc2']: Some error");
              assertThat(err.fields()).containsEntry("exceptionClass", "RuntimeException");
            });
  }
}
