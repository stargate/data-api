package io.stargate.sgv2.jsonapi.service.reranking.operation;

import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.ProviderContentTypeFilter;

/**
 * A client response filter/interceptor that validates the response from the reranking provider.
 *
 * <p>This filter checks the Content-Type of the response to ensure it is compatible with
 * 'application/json' or 'text/json'. It also verifies the presence of a JSON body in the response.
 *
 * <p>If the response fails the validation, a {@link APIException} is thrown with an appropriate
 * error message.
 */
public class RerankingProviderContentTypeFilter extends ProviderContentTypeFilter {

  public RerankingProviderContentTypeFilter() {
    super(SchemaException.Code.RERANKING_PROVIDER_UNEXPECTED_RESPONSE);
  }
}
