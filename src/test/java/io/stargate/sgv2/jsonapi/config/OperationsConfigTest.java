package io.stargate.sgv2.jsonapi.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import jakarta.validation.constraints.Max;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class OperationsConfigTest {

  @Inject OperationsConfig operationsConfig;

  @Test
  void defaultPageSizeUsesConfiguredConstantAndFitsMax() {
    assertThat(operationsConfig.defaultPageSize()).isEqualTo(OperationsConfig.DEFAULT_PAGE_SIZE);
    assertThat(operationsConfig.defaultPageSize()).isEqualTo(50);
  }

  @Test
  void defaultPageSizeAllowsOriginalConfigurableMaximum() throws Exception {
    var defaultPageSizeMax =
        OperationsConfig.class.getMethod("defaultPageSize").getAnnotation(Max.class).value();

    assertThat(defaultPageSizeMax).isEqualTo(OperationsConfig.MAX_CONFIGURABLE_PAGE_SIZE);
    assertThat(defaultPageSizeMax).isEqualTo(500);
    assertThat(operationsConfig.defaultPageSize())
        .isLessThanOrEqualTo(OperationsConfig.MAX_CONFIGURABLE_PAGE_SIZE);
  }
}
