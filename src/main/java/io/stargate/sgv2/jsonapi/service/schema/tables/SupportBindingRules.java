package io.stargate.sgv2.jsonapi.service.schema.tables;

/**
 * Basic rules that only have a {@link SupportBindingRule#isSupported()} flag.
 */
public class SupportBindingRules extends BindingPointRules<SupportBindingRules.SupportBindingRule> {

  /**
   * Re-usable rules that the type is supported in all binding points.
   */
  public static final SupportBindingRules ALL_SUPPORTED =
      new SupportBindingRules(
          create(TypeBindingPoint.COLLECTION_VALUE, true, true),
          create(TypeBindingPoint.KEYSPACE, true, true),
          create(TypeBindingPoint.MAP_KEY, true, true),
          create(TypeBindingPoint.TABLE_COLUMN, true, true),
          create(TypeBindingPoint.UDT_FIELD, true, true)
      );

  public static final SupportBindingRules NONE_SUPPORTED =
      new SupportBindingRules(
          create(TypeBindingPoint.COLLECTION_VALUE, false, false),
          create(TypeBindingPoint.KEYSPACE, false, false),
          create(TypeBindingPoint.MAP_KEY, false, false),
          create(TypeBindingPoint.TABLE_COLUMN, false, false),
          create(TypeBindingPoint.UDT_FIELD, false, false)
      );

  public SupportBindingRules(SupportBindingRule... rules) {
    super(rules);
  }

  public static SupportBindingRule create(
      TypeBindingPoint bindingPoint, boolean supportedFromDb, boolean supportedFromUser) {
    return new SupportBindingRule(bindingPoint, supportedFromDb, supportedFromUser);
  }

  public record SupportBindingRule(TypeBindingPoint bindingPoint, boolean supportedFromDb, boolean supportedFromUser)
      implements BindingPointRule {
  }
}
