package io.stargate.sgv2.jsonapi.service.reranking.operation;

import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.ProviderContentTypeFilter;
import io.stargate.sgv2.jsonapi.service.provider.ProviderContentTypeFilterTest;

public class RerankingProviderContentTypeFilterTest
    extends ProviderContentTypeFilterTest<SchemaException> {

  public RerankingProviderContentTypeFilterTest() {
    super(SchemaException.class, SchemaException.Code.RERANKING_PROVIDER_UNEXPECTED_RESPONSE);
  }

  @Override
  protected ProviderContentTypeFilter instance() {
    return new RerankingProviderContentTypeFilter();
  }
}
