package io.stargate.sgv2.jsonapi.fixtures.testdata;

import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;

public class TenantTestData extends TestDataSuplier {

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  public TenantTestData(TestData testData) {
    super(testData);
  }

  public Tenant defaultTenant() {
    return TEST_CONSTANTS.TENANT;
  }
}
