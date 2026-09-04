package io.stargate.sgv2.jsonapi.service.reranking.operation;

import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.ProviderContentTypeFilter;

/** Subclass for Reranking providers, see {@link ProviderContentTypeFilter} */
public class RerankingProviderContentTypeFilter extends ProviderContentTypeFilter {

  public RerankingProviderContentTypeFilter() {
    super(SchemaException.Code.RERANKING_PROVIDER_UNEXPECTED_RESPONSE);
  }
}
