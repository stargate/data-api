package io.stargate.sgv2.jsonapi.config.constants;

public interface RerankingConstants {

  interface CollectionRerankingOptions {
    String ENABLED = "enabled";
    String SERVICE = CommonConstants.ServiceConfig.SERVICE;
  }

  interface RerankingService extends CommonConstants.ServiceConfig {}
}
