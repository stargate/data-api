package io.stargate.sgv2.jsonapi.grpc.retries.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.api.common.grpc.RetriableStargateBridge;
import io.stargate.sgv2.api.common.grpc.qualifier.Retriable;
import io.stargate.sgv2.common.bridge.BridgeTest;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class JsonApiGrpcRetryPolicyBridgeTest extends BridgeTest {

  @Retriable @Inject RetriableStargateBridge bridge;

  @Test
  public void checkForNull() {
    assertThat(bridge).isNull();
  }
}
