package io.stargate.sgv2.jsonapi.fixtures.testdata;

public abstract class TestDataSuplier {

  protected final TestDataNames names;
  protected final TestData testData;

  protected TestDataSuplier(TestData testData) {
    this.testData = testData;
    this.names = testData.names;
  }
}
