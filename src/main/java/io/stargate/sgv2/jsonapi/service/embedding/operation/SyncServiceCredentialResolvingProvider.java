package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.syncservice.SyncServiceClient;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SyncServiceCredentialResolvingProvider.class);

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

    // Match EGW behavior: if caller already provided credentials via headers, use those
    // directly and skip SyncService resolution
    if (hasHeaderCredentials(embeddingCredentials)) {
      return delegate.vectorize(batchId, texts, embeddingCredentials, embeddingRequestType);
    }

    return resolveCredentials()
        .flatMap(resolved -> delegate.vectorize(batchId, texts, resolved, embeddingRequestType));
  }

  private static boolean hasHeaderCredentials(EmbeddingCredentials creds) {
    return creds.apiKey().isPresent()
        || creds.accessId().isPresent()
        || creds.secretId().isPresent();
  }

  @Override
  public int maxBatchSize() {
    return delegate.maxBatchSize();
  }

  /**
   * Resolves credentials by calling SyncService for each entry in the authentication map. Each
   * entry's key is the accepted token name (e.g. "providerKey"), and the value is the credential
   * reference name stored in SyncService (e.g. "my-openai-cred").
   *
   * <p>The SyncService response map is keyed by the credential reference name, so we extract the
   * resolved secret using the credential name as key (matching EGW's EmbeddingServiceImpl
   * behavior).
   */
  private Uni<EmbeddingCredentials> resolveCredentials() {
    String providerName = modelProvider().apiName();

    // Build parallel SyncService calls and track which accepted token name each one is for
    List<String> acceptedNames = new ArrayList<>();
    List<String> credNames = new ArrayList<>();
    List<Uni<Map<String, String>>> resolveUnis = new ArrayList<>();

    for (Map.Entry<String, String> entry : authentication.entrySet()) {
      acceptedNames.add(entry.getKey());
      credNames.add(entry.getValue());
      resolveUnis.add(
          syncServiceClient.getCredential(authToken, tenant, providerName, entry.getValue()));
    }

    return Uni.join()
        .all(resolveUnis)
        .andFailFast()
        .map(
            results -> {
              // For each resolved result, extract the secret using the credential name as key
              // (SyncService response is keyed by credential reference name, not accepted name)
              Map<String, String> resolvedByAcceptedName = new HashMap<>();
              for (int i = 0; i < results.size(); i++) {
                Map<String, String> credMap = results.get(i);
                if (credMap != null) {
                  String credName = credNames.get(i);
                  String acceptedName = acceptedNames.get(i);
                  String resolvedValue = credMap.get(credName);
                  if (resolvedValue != null) {
                    resolvedByAcceptedName.put(acceptedName, resolvedValue);
                  } else {
                    LOGGER.warn(
                        "SyncService response for credential '{}' (provider '{}') did not contain"
                            + " expected key '{}'; available keys: {}",
                        credName,
                        providerName,
                        credName,
                        credMap.keySet());
                  }
                }
              }
              return buildEmbeddingCredentials(resolvedByAcceptedName);
            });
  }

  /**
   * Maps resolved credential values (keyed by accepted token name) to {@link EmbeddingCredentials}.
   * The accepted token names come from the provider's SHARED_SECRET config (e.g. "providerKey",
   * "accessId", "secretKey").
   */
  private EmbeddingCredentials buildEmbeddingCredentials(
      Map<String, String> resolvedByAcceptedName) {
    var apiKey = Optional.ofNullable(resolvedByAcceptedName.get(PROVIDER_KEY));
    var accessId = Optional.ofNullable(resolvedByAcceptedName.get(ACCESS_ID));
    var secretId =
        Optional.ofNullable(
            resolvedByAcceptedName.containsKey(SECRET_KEY)
                ? resolvedByAcceptedName.get(SECRET_KEY)
                : resolvedByAcceptedName.get(SECRET_ID));
    return new EmbeddingCredentials(tenant, apiKey, accessId, secretId, Optional.of(authToken));
  }
}
