package io.stargate.sgv3.docsapi;

/**
 * What we know about the client we are running the request for.
 *
 * <p>A component of the {@link CommandContext}
 */
public class ClientState {

  // Guessing there will be more, maybe IP etc ?
  public final String tenantId;
  public final String authToken;

  public ClientState(String tenantId, String authToken) {
    this.tenantId = tenantId;
    this.authToken = authToken;
  }
}
