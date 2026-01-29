package io.stargate.sgv2.jsonapi.service.provider;



public class DefaultProviderExceptionHandler implements ProviderExceptionHandler {
  protected final ModelProvider modelProvider;
  protected final ModelType modelType;

  public DefaultProviderExceptionHandler(
      ModelProvider modelProvider,
      ModelType modelType
  ) {
    this.modelProvider = modelProvider;
    this.modelType = modelType;
  }
}
