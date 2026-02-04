package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class EmbeddingProviderExceptionHandler extends DefaultProviderExceptionHandler {

  public EmbeddingProviderExceptionHandler(ModelProvider modelProvider, ModelType modelType) {
    super(modelProvider, modelType);
  }

  @Override
  public Throwable handle(TimeoutException exception) {

    return EmbeddingProviderException.Code.EMBEDDING_PROVIDER_TIMEOUT.get(
        Map.of(
            "modelProvider", modelProvider.apiName(),
            "httpStatus", "<CLIENT SOCKET TIMEOUT>",
            "errorMessage", "<CLIENT SOCKET TIMEOUT>"));
  }

  /**
   * [EGW #94]: https://github.com/riptano/embedding-gateway/issues/94. Handle UnknownHostException
   * gracefully (bad host name in templatized URL)
   */
  @Override
  public Throwable handle(UnknownHostException exception) {

    return EmbeddingProviderException.Code.EMBEDDING_PROVIDER_BAD_HOST_NAME.get(
        Map.of(
            "modelProvider", modelProvider.apiName(),
            "errorClass", exception.getClass().getSimpleName(),
            "errorMessage", exception.getMessage()));
  }
}
