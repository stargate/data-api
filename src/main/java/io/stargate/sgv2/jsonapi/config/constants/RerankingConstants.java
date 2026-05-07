package io.stargate.sgv2.jsonapi.config.constants;

public interface RerankingConstants {

  interface HybridSearchLimits {
    int MIN = 1;
    int DEFAULT = 50;
    int MAX = 100;
  }

  interface CollectionRerankingOptions {
    String ENABLED = "enabled";
    String SERVICE = ServiceDescConstants.SERVICE;
  }
}
