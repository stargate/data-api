package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.sgv2.jsonapi.exception.RerankingProviderException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RerankingProviderExceptionHandler extends DefaultProviderExceptionHandler {

  public RerankingProviderExceptionHandler(ModelProvider modelProvider, ModelType modelType) {
    super(modelProvider, modelType);
  }

  @Override
  public Throwable handle(TimeoutException exception) {

    return RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.get(
        Map.of(
            "modelProvider", modelProvider.apiName(),
            "httpStatus", "<CLIENT SOCKET TIMEOUT>",
            "errorMessage", "<CLIENT SOCKET TIMEOUT>"));
  }
}
