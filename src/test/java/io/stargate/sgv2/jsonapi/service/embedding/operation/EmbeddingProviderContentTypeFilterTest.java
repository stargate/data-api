package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import io.stargate.sgv2.jsonapi.service.provider.ProviderContentTypeFilter;
import io.stargate.sgv2.jsonapi.service.provider.ProviderContentTypeFilterTest;

public class EmbeddingProviderContentTypeFilterTest
    extends ProviderContentTypeFilterTest<EmbeddingProviderException> {

  public EmbeddingProviderContentTypeFilterTest() {
    super(
        EmbeddingProviderException.class,
        EmbeddingProviderException.Code.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE);
  }

  @Override
  protected ProviderContentTypeFilter instance() {
    return new EmbeddingProviderContentTypeFilter();
  }
}
