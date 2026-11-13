package io.stargate.sgv2.jsonapi.config.constants;

public interface RerankingConstants {

  interface HybridSearchLimits {
    int MIN = 1;
    int DEFAULT = 50;
    int MAX = 100;
  }

  /** The min, default and max for the rerankCandidateLimit option of findAndRerank. */
  interface RerankCandidateLimits {
    // the option must be an integer greater than 1
    int MIN = 2;

    // NOTE: the design of the option does not set the default and the max yet. Until it does, they
    // are the same as in HybridSearchLimits.
    int DEFAULT = 50;
    int MAX = 100;
  }

  interface CollectionRerankingOptions {
    String ENABLED = "enabled";
    String SERVICE = ServiceDescConstants.SERVICE;
  }
}
