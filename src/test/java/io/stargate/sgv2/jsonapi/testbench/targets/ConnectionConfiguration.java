package io.stargate.sgv2.jsonapi.testbench.targets;

/**
 * Configuraiton record of how to connect for a {@link TargetConfiguration}.
 *
 * <p>---
 *
 * @param domain domain and protocol, e.g. <code>http://localhost</code>
 * @param port port to connect to, typically <code>8181</code> ore <code>443</code> but may be
 *     different when running integration tests.
 * @param basePath base path to the API, for Astra this is <code>/api/json/v1</code> when running
 *     locally is normally <code>/v1</code>
 */
public record ConnectionConfiguration(String domain, Integer port, String basePath) {
  public ConnectionConfiguration {
    if (domain == null) {
      domain = "localhost";
    }
    if (port == null) {
      port = 8181;
    }
    if (basePath == null) {
      basePath = "/v1";
    }
  }
}
