package io.stargate.sgv2.jsonapi.exception;


import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class JsonApiExceptionTest {

  @Test
  public void happyPath() {

    //    var commandResult =
    //        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
    //            .addThrowable(new JsonApiException(ErrorCodeV1.INVALID_REQUEST))
    //            .build();
    //
    //    assertThat(commandResult.data()).isNull();
    //    assertThat(commandResult.status()).isEmpty();
    //    assertThat(commandResult.errors())
    //        .singleElement()
    //        .satisfies(
    //            error -> {
    //              assertThat(error.message()).isEqualTo("Request not supported by the data
    // store");
    //              assertThat(error.errorCode()).isEqualTo(ErrorCodeV1.INVALID_REQUEST.name());
    //            });
  }
}
