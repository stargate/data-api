package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.sgv2.jsonapi.exception.RequestException;

import java.util.Map;
import java.util.concurrent.TimeoutException;

public class EmbeddingProviderExceptionHandler extends DefaultProviderExceptionHandler {

  public EmbeddingProviderExceptionHandler(
      ModelProvider modelProvider,
      ModelType modelType
  )
  {
    super(modelProvider, modelType);
  }

  @Override
  public Throwable handle(TimeoutException exception) {

    return RequestException.Code.EMBEDDING_PROVIDER_TIMEOUT.get(
        Map.of(
            "modelProvider", modelProvider.apiName(),
            "httpStatus", "<CLIENT SOCKET TIMEOUT>",
            "errorMessage", "<CLIENT SOCKET TIMEOUT>"));
  }
}
