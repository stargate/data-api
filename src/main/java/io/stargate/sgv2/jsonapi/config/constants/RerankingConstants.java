package io.stargate.sgv2.jsonapi.config.constants;

public interface RerankingConstants {

  interface HybridSearchLimits {
    int MIN = 1;
    int DEFAULT = 50;
    int MAX = 100;
  }

  /** The min, default and max for the rerankCandidateLimit option of findAndRerank. */
  interface RerankCandidateLimits {
    // TODO: the design doc says "an integer greater than 1", that's different from HybridSearchLimits
    int MIN = 2;
    // TODO: the design doc says "The maximum value and the default value are TBC"
    int DEFAULT = 50;
    int MAX = 100;
  }

  interface CollectionRerankingOptions {
    String ENABLED = "enabled";
    String SERVICE = ServiceDescConstants.SERVICE;
  }
}
