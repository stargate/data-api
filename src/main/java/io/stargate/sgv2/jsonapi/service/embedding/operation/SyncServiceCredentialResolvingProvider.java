package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.egw.SyncServiceClient;
import java.util.*;

/**
 * A decorator that wraps a direct {@link EmbeddingProvider} and resolves shared-secret credentials
 * via {@link SyncServiceClient} before each {@link #vectorize} call.
 *
 * <p>When the Embedding Gateway (EGW) is disabled (standalone mode), collections configured with
 * {@code SHARED_SECRET} authentication need their credential names resolved into actual secrets.
 * This provider handles that resolution by calling {@link SyncServiceClient#getCredential} for each
 * entry in the authentication map, then passing the resolved credentials to the delegate provider.
 */
public class SyncServiceCredentialResolvingProvider extends EmbeddingProvider {

  private static final String PROVIDER_KEY = "providerKey";
  private static final String ACCESS_ID = "accessId";
  private static final String SECRET_KEY = "secretKey";
  private static final String SECRET_ID = "secretId";

  private final EmbeddingProvider delegate;
  private final SyncServiceClient syncServiceClient;
  private final Map<String, String> authentication;
  private final Tenant tenant;
  private final String authToken;

  public SyncServiceCredentialResolvingProvider(
      EmbeddingProvider delegate,
      SyncServiceClient syncServiceClient,
      Map<String, String> authentication,
      Tenant tenant,
      String authToken) {
    super(
        delegate.modelProvider(),
        delegate.providerConfig,
        delegate.modelConfig,
        delegate.serviceConfig,
        delegate.dimension,
        delegate.vectorizeServiceParameters);

    this.delegate = delegate;
    this.syncServiceClient = syncServiceClient;
    this.authentication = authentication;
    this.tenant = tenant;
    this.authToken = authToken;
  }

  @Override
  protected String errorMessageJsonPtr() {
    // Not used directly — this wrapper never makes HTTP calls itself
    return "";
  }

  @Override
  public Uni<BatchedEmbeddingResponse> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {

    return resolveCredentials()
        .flatMap(resolved -> delegate.vectorize(batchId, texts, resolved, embeddingRequestType));
  }

  @Override
  public int maxBatchSize() {
    return delegate.maxBatchSize();
  }

  /**
   * Resolves credentials by calling SyncService for each entry in the authentication map. Each
   * entry's key is the accepted credential name (e.g. "providerKey"), and the value is the
   * credential reference name stored in SyncService.
   */
  private Uni<EmbeddingCredentials> resolveCredentials() {
    String providerName = modelProvider().apiName();

    List<Uni<Map<String, String>>> resolveUnis = new ArrayList<>();
    for (String credName : authentication.values()) {
      resolveUnis.add(syncServiceClient.getCredential(authToken, tenant, providerName, credName));
    }

    return Uni.join()
        .all(resolveUnis)
        .andFailFast()
        .map(
            results -> {
              Map<String, String> allCredentials = new HashMap<>();
              for (Map<String, String> credMap : results) {
                if (credMap != null) {
                  allCredentials.putAll(credMap);
                }
              }
              return buildEmbeddingCredentials(allCredentials);
            });
  }

  private EmbeddingCredentials buildEmbeddingCredentials(Map<String, String> resolved) {
    return new EmbeddingCredentials(
        tenant,
        Optional.ofNullable(resolved.get(PROVIDER_KEY)),
        Optional.ofNullable(resolved.get(ACCESS_ID)),
        Optional.ofNullable(
            resolved.containsKey(SECRET_KEY) ? resolved.get(SECRET_KEY) : resolved.get(SECRET_ID)));
  }
}
