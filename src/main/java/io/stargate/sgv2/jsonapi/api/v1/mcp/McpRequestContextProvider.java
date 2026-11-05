package io.stargate.sgv2.jsonapi.api.v1.mcp;

import java.util.Map;
import org.eclipse.microprofile.context.spi.ThreadContextProvider;
import org.eclipse.microprofile.context.spi.ThreadContextSnapshot;
import org.slf4j.MDC;

/**
 * Keeps the MCP request identifier across asynchronous callbacks without copying other MDC data.
 */
public final class McpRequestContextProvider implements ThreadContextProvider {
  @Override
  public ThreadContextSnapshot currentContext(Map<String, String> props) {
    return snapshot(MDC.get(McpRequestLogging.REQUEST_ID));
  }

  @Override
  public ThreadContextSnapshot clearedContext(Map<String, String> props) {
    return snapshot(null);
  }

  @Override
  public String getThreadContextType() {
    return McpRequestLogging.REQUEST_ID;
  }

  private static ThreadContextSnapshot snapshot(String requestId) {
    return () -> {
      String previous = MDC.get(McpRequestLogging.REQUEST_ID);
      setRequestId(requestId);
      return () -> setRequestId(previous);
    };
  }

  private static void setRequestId(String requestId) {
    if (requestId == null) {
      MDC.remove(McpRequestLogging.REQUEST_ID);
    } else {
      MDC.put(McpRequestLogging.REQUEST_ID, requestId);
    }
  }
}
