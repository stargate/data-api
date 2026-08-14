package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import io.stargate.sgv2.jsonapi.service.provider.ProviderContentTypeFilter;

/** Subclass for Embedding providers, see {@link ProviderContentTypeFilter} */
public class EmbeddingProviderContentTypeFilter extends ProviderContentTypeFilter {

  public EmbeddingProviderContentTypeFilter() {
    super(EmbeddingProviderException.Code.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE);
  }
}
