package io.stargate.sgv2.jsonapi.service.provider;

public class RerankingProviderExceptionHandler extends DefaultProviderExceptionHandler {

  public RerankingProviderExceptionHandler(
      ModelProvider modelProvider,
      ModelType modelType
  )
  {
    super(modelProvider, modelType);
  }
}
