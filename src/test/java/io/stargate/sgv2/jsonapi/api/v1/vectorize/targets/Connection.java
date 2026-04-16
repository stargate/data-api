package io.stargate.sgv2.jsonapi.api.v1.vectorize.targets;

public record Connection(String domain, Integer port, String basePath) {
  public Connection {
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
