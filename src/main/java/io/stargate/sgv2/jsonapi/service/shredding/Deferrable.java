package io.stargate.sgv2.jsonapi.service.shredding;

import java.util.ArrayList;
import java.util.List;

/**
 * An object that <em>may</em> have Deferred values that use a ValueAction to get the values.
 *
 * <p>NOTE: TODO: aaron 19 march 2025 - this is improved in the following PR for findAndRerank
 */
public interface Deferrable {

  List<? extends Deferred> deferred();

  static List<? extends Deferred> deferred(Deferrable deferrable) {
    return deferred(List.of(deferrable));
  }

  static List<? extends Deferred> deferred(List<Deferrable> deferrables) {
    var result = new ArrayList<Deferred>();
    for (Deferrable deferrable : deferrables) {
      result.addAll(deferrable.deferred());
    }
    return result;
  }
}
