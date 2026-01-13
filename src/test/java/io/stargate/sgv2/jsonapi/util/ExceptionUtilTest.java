package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
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
}
