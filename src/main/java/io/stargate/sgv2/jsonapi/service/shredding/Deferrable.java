package io.stargate.sgv2.jsonapi.service.shredding;

import java.util.ArrayList;
import java.util.List;

public interface Deferrable {

  List<? extends NamedValue<?, ?, ?>> deferredValues();

  static List<? extends NamedValue<?, ?, ?>> deferredValues(Deferrable deferrable) {
    return deferredValues(List.of(deferrable));
  }

  static List<? extends NamedValue<?, ?, ?>> deferredValues(List<Deferrable> deferrables) {
    // flatMap streaming not happy with generics
    var result = new ArrayList<NamedValue<?, ?, ?>>();
    for (Deferrable deferrable : deferrables) {
      result.addAll(deferrable.deferredValues());
    }
    return result;
  }
}
