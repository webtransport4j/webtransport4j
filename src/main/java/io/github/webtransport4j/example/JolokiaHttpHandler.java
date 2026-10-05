package io.github.webtransport4j.example;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jolokia-compatible JMX-to-HTTP bridge handler for Hawtio management and JVM diagnostics.
 *
 * <p>Enables Hawtio web consoles (standalone, Docker, or embedded) to manage WebTransport4J cluster
 * nodes via standard Jolokia 2.x REST protocol (reading MBeans, listing tree, thread dumps, GC).
 */
public class JolokiaHttpHandler implements HttpHandler {

  private static final Logger log = LoggerFactory.getLogger(JolokiaHttpHandler.class);
  private final MBeanServer mbeanServer;
  private final String nodeName;

  public JolokiaHttpHandler(String nodeName) {
    this.nodeName = nodeName != null ? nodeName : "wt-node";
    this.mbeanServer = ManagementFactory.getPlatformMBeanServer();
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    final String method = exchange.getRequestMethod();

    // CORS preflight support for Hawtio web consoles

    exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    exchange
        .getResponseHeaders()
        .set(
            "Access-Control-Allow-Headers",
            "Content-Type, Authorization, X-Requested-With, Origin, Accept");
    exchange.getResponseHeaders().set("Access-Control-Max-Age", "86400");

    if ("OPTIONS".equalsIgnoreCase(method)) {
      exchange.sendResponseHeaders(204, -1);
      return;
    }

    String path = exchange.getRequestURI().getPath();
    String contextPath = exchange.getHttpContext().getPath();
    String subPath = "";
    if (path.length() >= contextPath.length()) {
      subPath = path.substring(contextPath.length());
    }
    if (subPath.startsWith("/")) {
      subPath = subPath.substring(1);
    }

    try {
      if ("GET".equalsIgnoreCase(method) && subPath.startsWith("exec/")) {
        sendError(exchange, 405, "JMX operations require POST");
      } else if ("POST".equalsIgnoreCase(method) && subPath.startsWith("exec/")) {
        handleGet(exchange, subPath);
      } else if ("GET".equalsIgnoreCase(method)) {
        handleGet(exchange, subPath);
      } else if ("POST".equalsIgnoreCase(method)) {
        handlePost(exchange);
      } else {
        sendError(exchange, 405, "Method not allowed: " + method);
      }
    } catch (Exception e) {
      log.warn("Error handling Jolokia request {}: {}", path, e.getMessage());
      sendError(exchange, 500, e.getMessage());
    }
  }

  private void handleGet(HttpExchange exchange, String subPath) throws Exception {
    long timestamp = System.currentTimeMillis() / 1000L;

    if (subPath.isEmpty() || subPath.equalsIgnoreCase("version")) {
      String json =
          String.format(
              "{\"request\":{\"type\":\"version\"},\"status\":200,\"timestamp\":%d,"
                  + "\"value\":{\"protocol\":\"7.3\",\"agent\":\"2.1.2\","
                  + "\"info\":{\"product\":\"WebTransport4J\",\"vendor\":\"io.github.webtransport4j\","
                  + "\"version\":\"0.1.0-SNAPSHOT\",\"node\":\"%s\"}}}",
              timestamp, escapeJson(nodeName));
      sendJson(exchange, 200, json);
      return;
    }

    if (subPath.equalsIgnoreCase("list")) {
      String listJson = buildMbeansListJson(timestamp);
      sendJson(exchange, 200, listJson);
      return;
    }

    if (subPath.equalsIgnoreCase("overview") || subPath.equalsIgnoreCase("diagnostics")) {
      String overviewJson = buildDiagnosticsOverviewJson(timestamp);
      sendJson(exchange, 200, overviewJson);
      return;
    }

    if (subPath.equalsIgnoreCase("threads") || subPath.equalsIgnoreCase("threaddump")) {
      String threadsJson = buildThreadsDumpJson(timestamp);
      sendJson(exchange, 200, threadsJson);
      return;
    }

    if (subPath.startsWith("read/")) {
      String rest = subPath.substring(5);
      String[] parts = rest.split("/", 2);
      String mbeanName = URLDecoder.decode(parts[0], StandardCharsets.UTF_8.name());
      String attributeName =
          parts.length > 1 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8.name()) : null;
      String readJson = executeRead(mbeanName, attributeName, timestamp);
      sendJson(exchange, 200, readJson);
      return;
    }

    if (subPath.startsWith("search/")) {
      String patternStr = URLDecoder.decode(subPath.substring(7), StandardCharsets.UTF_8.name());
      String searchJson = executeSearch(patternStr, timestamp);
      sendJson(exchange, 200, searchJson);
      return;
    }

    if (subPath.startsWith("exec/")) {
      String rest = subPath.substring(5);
      String[] parts = rest.split("/");
      if (parts.length >= 2) {
        String mbeanName = URLDecoder.decode(parts[0], StandardCharsets.UTF_8.name());
        String operation = URLDecoder.decode(parts[1], StandardCharsets.UTF_8.name());
        String[] args = new String[parts.length - 2];
        for (int i = 2; i < parts.length; i++) {
          args[i - 2] = URLDecoder.decode(parts[i], StandardCharsets.UTF_8.name());
        }
        String execJson = executeOperation(mbeanName, operation, args, timestamp);
        sendJson(exchange, 200, execJson);
        return;
      }
    }

    sendError(exchange, 404, "Unknown Jolokia path: " + subPath);
  }

  private void handlePost(HttpExchange exchange) throws Exception {
    long timestamp = System.currentTimeMillis() / 1000L;
    byte[] bodyBytes = readBody(exchange.getRequestBody());
    String body = new String(bodyBytes, StandardCharsets.UTF_8).trim();

    if (body.startsWith("[")) {
      // Batch Jolokia requests
      List<String> items = parseSimpleJsonArray(body);
      StringBuilder sb = new StringBuilder("[");
      for (int i = 0; i < items.size(); i++) {
        if (i > 0) {
          sb.append(",");
        }
        sb.append(processSingleRequest(items.get(i), timestamp));
      }
      sb.append("]");
      sendJson(exchange, 200, sb.toString());
    } else {
      String response = processSingleRequest(body, timestamp);
      sendJson(exchange, 200, response);
    }
  }

  private String processSingleRequest(String reqJson, long timestamp) {
    String type = extractJsonField(reqJson, "type");
    if (type == null) {
      type = "read";
    }

    if ("version".equalsIgnoreCase(type)) {
      return String.format(
          "{\"request\":{\"type\":\"version\"},\"status\":200,\"timestamp\":%d,"
              + "\"value\":{\"protocol\":\"7.3\",\"agent\":\"2.1.2\","
              + "\"info\":{\"product\":\"WebTransport4J\",\"vendor\":\"io.github.webtransport4j\","
              + "\"version\":\"0.1.0-SNAPSHOT\",\"node\":\"%s\"}}}",
          timestamp, escapeJson(nodeName));
    }

    if ("list".equalsIgnoreCase(type)) {
      return buildMbeansListJson(timestamp);
    }

    if ("search".equalsIgnoreCase(type)) {
      String mbean = extractJsonField(reqJson, "mbean");
      return executeSearch(mbean != null ? mbean : "*:*", timestamp);
    }

    if ("read".equalsIgnoreCase(type)) {
      String mbean = extractJsonField(reqJson, "mbean");
      String attribute = extractJsonField(reqJson, "attribute");
      return executeRead(mbean, attribute, timestamp);
    }

    if ("exec".equalsIgnoreCase(type)) {
      String mbean = extractJsonField(reqJson, "mbean");
      String operation = extractJsonField(reqJson, "operation");
      return executeOperation(mbean, operation, new String[0], timestamp);
    }

    return String.format(
        "{\"request\":{\"type\":\"%s\"},\"status\":400,\"error\":\"Unsupported Jolokia type: %s\","
            + "\"timestamp\":%d}",
        escapeJson(type), escapeJson(type), timestamp);
  }

  private String executeRead(String mbeanName, String attributeName, long timestamp) {
    if (mbeanName == null || mbeanName.trim().isEmpty()) {
      return String.format(
          "{\"status\":400,\"error\":\"Missing mbean name\",\"timestamp\":%d}", timestamp);
    }

    try {
      ObjectName on = new ObjectName(mbeanName);
      if (attributeName != null && !attributeName.trim().isEmpty()) {
        Object val = mbeanServer.getAttribute(on, attributeName);
        StringBuilder sb = new StringBuilder();
        sb.append("{\"request\":{\"type\":\"read\",\"mbean\":\"")
            .append(escapeJson(mbeanName))
            .append("\",\"attribute\":\"")
            .append(escapeJson(attributeName))
            .append("\"},\"status\":200,\"timestamp\":")
            .append(timestamp)
            .append(",\"value\":");
        serializeValueToJson(val, sb);
        sb.append("}");
        return sb.toString();
      } else {
        // Read all attributes
        MBeanInfo info = mbeanServer.getMBeanInfo(on);
        StringBuilder sb = new StringBuilder();
        sb.append("{\"request\":{\"type\":\"read\",\"mbean\":\"")
            .append(escapeJson(mbeanName))
            .append("\"},\"status\":200,\"timestamp\":")
            .append(timestamp)
            .append(",\"value\":{");
        boolean first = true;
        for (MBeanAttributeInfo attrInfo : info.getAttributes()) {
          if (!attrInfo.isReadable()) {
            continue;
          }
          try {
            final Object val = mbeanServer.getAttribute(on, attrInfo.getName());
            if (!first) {
              sb.append(",");
            }
            first = false;
            sb.append("\"").append(escapeJson(attrInfo.getName())).append("\":");
            serializeValueToJson(val, sb);
          } catch (Exception ignored) {
            // Continue best-effort diagnostics or cleanup if this operation is unavailable.
          }
        }
        sb.append("}}");
        return sb.toString();
      }
    } catch (Exception e) {
      return String.format(
          "{\"request\":{\"type\":\"read\",\"mbean\":\"%s\"},\"status\":404,"
              + "\"error\":\"%s\",\"timestamp\":%d}",
          escapeJson(mbeanName), escapeJson(e.getMessage()), timestamp);
    }
  }

  private String executeSearch(String patternStr, long timestamp) {
    try {
      ObjectName query = new ObjectName(patternStr != null ? patternStr : "*:*");
      Set<ObjectName> names = mbeanServer.queryNames(query, null);
      StringBuilder sb = new StringBuilder();
      sb.append("{\"request\":{\"type\":\"search\",\"mbean\":\"")
          .append(escapeJson(patternStr))
          .append("\"},\"status\":200,\"timestamp\":")
          .append(timestamp)
          .append(",\"value\":[");
      boolean first = true;
      for (ObjectName on : names) {
        if (!first) {
          sb.append(",");
        }
        first = false;
        sb.append("\"").append(escapeJson(on.getCanonicalName())).append("\"");
      }
      sb.append("]}");
      return sb.toString();
    } catch (Exception e) {
      return String.format(
          "{\"request\":{\"type\":\"search\"},\"status\":400,\"error\":\"%s\",\"timestamp\":%d}",
          escapeJson(e.getMessage()), timestamp);
    }
  }

  private String executeOperation(
      String mbeanName, String operation, String[] args, long timestamp) {
    if (!"java.lang:type=Memory".equals(mbeanName) || !"gc".equals(operation) || args.length != 0) {
      return "{\"status\":403,\"error\":\"Operation is not allowed\"}";
    }
    System.gc();
    return String.format(
        "{\"request\":{\"type\":\"exec\",\"mbean\":\"%s\",\"operation\":\"gc\"},"
            + "\"status\":200,\"timestamp\":%d,\"value\":null}",
        escapeJson(mbeanName), timestamp);
  }

  private String buildMbeansListJson(long timestamp) {
    try {
      Set<ObjectName> names = mbeanServer.queryNames(null, null);
      Map<String, Map<String, Object>> domains = new TreeMap<>();

      for (ObjectName on : names) {
        String domain = on.getDomain();
        domains.computeIfAbsent(domain, k -> new TreeMap<>());
        String keyProps = on.getKeyPropertyListString();

        Map<String, Object> mbeanData = new HashMap<>();
        try {
          MBeanInfo info = mbeanServer.getMBeanInfo(on);
          mbeanData.put("class", info.getClassName());
          mbeanData.put("desc", info.getDescription());

          Map<String, Object> attrs = new TreeMap<>();
          for (MBeanAttributeInfo a : info.getAttributes()) {
            Map<String, Object> ad = new HashMap<>();
            ad.put("type", a.getType());
            ad.put("rw", a.isWritable());
            ad.put("desc", a.getDescription());
            attrs.put(a.getName(), ad);
          }
          mbeanData.put("attr", attrs);

          Map<String, Object> ops = new TreeMap<>();
          for (MBeanOperationInfo op : info.getOperations()) {
            Map<String, Object> od = new HashMap<>();
            od.put("ret", op.getReturnType());
            od.put("desc", op.getDescription());
            List<Map<String, String>> opArgs = new ArrayList<>();
            for (MBeanParameterInfo p : op.getSignature()) {
              Map<String, String> argMap = new HashMap<>();
              argMap.put("name", p.getName());
              argMap.put("type", p.getType());
              opArgs.add(argMap);
            }
            od.put("args", opArgs);
            ops.put(op.getName(), od);
          }
          mbeanData.put("op", ops);
        } catch (Exception ignored) {
          // Continue best-effort diagnostics or cleanup if this operation is unavailable.
        }
        domains.get(domain).put(keyProps, mbeanData);
      }

      StringBuilder sb = new StringBuilder();
      sb.append("{\"request\":{\"type\":\"list\"},\"status\":200,\"timestamp\":")
          .append(timestamp)
          .append(",\"value\":");
      serializeValueToJson(domains, sb);
      sb.append("}");
      return sb.toString();
    } catch (Exception e) {
      return String.format(
          "{\"status\":500,\"error\":\"%s\",\"timestamp\":%d}",
          escapeJson(e.getMessage()), timestamp);
    }
  }

  /**
   * Comprehensive live Hawtio / JVM diagnostics payload containing real-time GC, Generational ZGC
   * status, heap/non-heap memory, direct buffer pool, thread stats, and OS load.
   */
  public String buildDiagnosticsOverviewJson(long timestamp) {
    final Runtime rt = Runtime.getRuntime();
    MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
    final MemoryUsage heap = memBean.getHeapMemoryUsage();
    final MemoryUsage nonHeap = memBean.getNonHeapMemoryUsage();
    final RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

    // Inspect real Garbage Collectors
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    boolean isZgcMinor = false;
    boolean isZgcMajor = false;
    boolean isZgcSingle = false;
    boolean isG1 = false;
    boolean isParallel = false;
    boolean isShenandoah = false;

    StringBuilder gcBeansJson = new StringBuilder("[");
    long totalGcPausesMs = 0;
    long totalGcCollections = 0;

    for (int i = 0; i < gcBeans.size(); i++) {
      GarbageCollectorMXBean b = gcBeans.get(i);
      final String name = b.getName();
      long count = b.getCollectionCount();
      long time = b.getCollectionTime();
      if (count > 0) {
        totalGcCollections += count;
      }
      if (time > 0) {
        totalGcPausesMs += time;
      }

      if (i > 0) {
        gcBeansJson.append(",");
      }
      gcBeansJson.append(
          String.format(
              "{\"name\":\"%s\",\"collections\":%d,\"timeMs\":%d,\"memoryPools\":[",
              escapeJson(name), count, time));
      String[] pools = b.getMemoryPoolNames();
      if (pools != null) {
        for (int p = 0; p < pools.length; p++) {
          if (p > 0) {
            gcBeansJson.append(",");
          }
          gcBeansJson.append("\"").append(escapeJson(pools[p])).append("\"");
        }
      }
      gcBeansJson.append("]}");

      String lower = name.toLowerCase();
      if (lower.contains("zgc minor")) {
        isZgcMinor = true;
      }
      if (lower.contains("zgc major")) {
        isZgcMajor = true;
      }
      if (lower.contains("zgc") && !lower.contains("minor") && !lower.contains("major")) {
        isZgcSingle = true;
      }
      if (lower.contains("g1")) {
        isG1 = true;
      }
      if (lower.contains("parallel")) {
        isParallel = true;
      }
      if (lower.contains("shenandoah")) {
        isShenandoah = true;
      }
    }
    gcBeansJson.append("]");

    String gcType;
    if (isZgcMinor || isZgcMajor) {
      gcType = "Generational ZGC";
    } else if (isZgcSingle) {
      gcType = "ZGC";
    } else if (isG1) {
      gcType = "G1 GC";
    } else if (isParallel) {
      gcType = "Parallel GC";
    } else if (isShenandoah) {
      gcType = "Shenandoah GC";
    } else if (!gcBeans.isEmpty()) {
      gcType = gcBeans.get(0).getName();
    } else {
      gcType = "Serial GC";
    }

    int javaFeature = io.netty.util.internal.PlatformDependent.javaVersion();
    final String javaVersion = System.getProperty("java.version", String.valueOf(javaFeature));
    final String javaVmName = System.getProperty("java.vm.name", "Java HotSpot");
    final String javaVendor = System.getProperty("java.vendor", "Oracle Corporation");
    final String jvmDisplayName = String.format("Java %d · %s", javaFeature, gcType);

    // Direct memory (Netty byte buffers)
    long directMemUsed = 0;
    long directMemTotal = 0;
    long directBufferCount = 0;
    for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      if ("direct".equalsIgnoreCase(pool.getName())) {
        directMemUsed = pool.getMemoryUsed();
        directMemTotal = pool.getTotalCapacity();
        directBufferCount = pool.getCount();
      }
    }

    // Process CPU Load
    double cpuLoad = 0.0;
    try {
      Method m = osBean.getClass().getMethod("getProcessCpuLoad");
      m.setAccessible(true);
      Object val = m.invoke(osBean);
      if (val instanceof Double && (Double) val >= 0.0) {
        cpuLoad = (Double) val * 100.0;
      }
    } catch (Exception ignored) {
      // Continue best-effort diagnostics or cleanup if this operation is unavailable.
    }

    // JVM Input Arguments
    List<String> inputArgs = runtimeBean.getInputArguments();
    StringBuilder argsJson = new StringBuilder("[");
    for (int i = 0; i < inputArgs.size(); i++) {
      if (i > 0) {
        argsJson.append(",");
      }
      argsJson.append("\"").append(escapeJson(inputArgs.get(i))).append("\"");
    }
    argsJson.append("]");

    String envGc = System.getenv("JAVA_GC");
    if (envGc == null || envGc.trim().isEmpty()) {
      envGc = System.getenv("WT4J_GC");
    }
    if (envGc == null || envGc.trim().isEmpty()) {
      envGc = "ZGC";
    }

    return String.format(
        "{\"status\":200,\"nodeName\":\"%s\",\"timestamp\":%d,"
            + "\"jvm\":{\"displayName\":\"%s\",\"gcType\":\"%s\",\"isGenerationalZgc\":%b,"
            + "\"configuredGc\":\"%s\","
            + "\"feature\":%d,\"version\":\"%s\",\"vmName\":\"%s\",\"vendor\":\"%s\","
            + "\"uptimeSeconds\":%d,\"uptimeMs\":%d,\"inputArguments\":%s,\"gcBeans\":%s},"
            + "\"memory\":{"
            + "\"heapUsed\":%d,\"heapCommitted\":%d,\"heapMax\":%d,"
            + "\"nonHeapUsed\":%d,\"nonHeapCommitted\":%d,"
            + "\"directMemoryUsed\":%d,\"directMemoryTotal\":%d,\"directBufferCount\":%d,"
            + "\"jvmTotal\":%d,\"jvmFree\":%d,\"jvmMax\":%d},"
            + "\"gc\":{\"totalCollections\":%d,\"totalPauseMs\":%d},"
            + "\"threads\":{\"active\":%d,\"peak\":%d,\"totalStarted\":%d},"
            + "\"os\":{\"name\":\"%s\",\"arch\":\"%s\",\"processors\":%d,\"cpuPercent\":%.2f}}",
        escapeJson(nodeName),
        timestamp,
        escapeJson(jvmDisplayName),
        escapeJson(gcType),
        (isZgcMinor || isZgcMajor),
        escapeJson(envGc),
        javaFeature,
        escapeJson(javaVersion),
        escapeJson(javaVmName),
        escapeJson(javaVendor),
        runtimeBean.getUptime() / 1000L,
        runtimeBean.getUptime(),
        argsJson,
        gcBeansJson,
        heap.getUsed(),
        heap.getCommitted(),
        heap.getMax(),
        nonHeap.getUsed(),
        nonHeap.getCommitted(),
        directMemUsed,
        directMemTotal,
        directBufferCount,
        rt.totalMemory(),
        rt.freeMemory(),
        rt.maxMemory(),
        totalGcCollections,
        totalGcPausesMs,
        threadBean.getThreadCount(),
        threadBean.getPeakThreadCount(),
        threadBean.getTotalStartedThreadCount(),
        escapeJson(osBean.getName()),
        escapeJson(osBean.getArch()),
        osBean.getAvailableProcessors(),
        cpuLoad);
  }

  /** Generates real-time thread dump and thread state visualizer for Hawtio console. */
  public String buildThreadsDumpJson(long timestamp) {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threads = threadBean.dumpAllThreads(true, true);

    int runnable = 0;
    int waiting = 0;
    int timedWaiting = 0;
    int blocked = 0;

    StringBuilder sb = new StringBuilder();
    sb.append("{\"status\":200,\"nodeName\":\"")
        .append(escapeJson(nodeName))
        .append("\",\"timestamp\":")
        .append(timestamp)
        .append(",\"threadCount\":")
        .append(threads.length)
        .append(",\"threads\":[");

    for (int i = 0; i < threads.length; i++) {
      ThreadInfo ti = threads[i];
      if (ti == null) {
        continue;
      }
      if (i > 0) {
        sb.append(",");
      }

      Thread.State state = ti.getThreadState();
      switch (state) {
        case RUNNABLE:
          runnable++;
          break;
        case WAITING:
          waiting++;
          break;
        case TIMED_WAITING:
          timedWaiting++;
          break;
        case BLOCKED:
          blocked++;
          break;
        default:
          // Other thread states do not contribute to these counters.
          break;
      }

      sb.append("{\"id\":")
          .append(ti.getThreadId())
          .append(",\"name\":\"")
          .append(escapeJson(ti.getThreadName()))
          .append("\",\"state\":\"")
          .append(state.name())
          .append("\",\"suspended\":")
          .append(ti.isSuspended())
          .append(",\"inNative\":")
          .append(ti.isInNative())
          .append(",\"lockName\":")
          .append(ti.getLockName() != null ? "\"" + escapeJson(ti.getLockName()) + "\"" : "null")
          .append(",\"stackTrace\":[");

      StackTraceElement[] st = ti.getStackTrace();
      for (int s = 0; s < Math.min(st.length, 12); s++) {
        if (s > 0) {
          sb.append(",");
        }
        sb.append("\"").append(escapeJson(st[s].toString())).append("\"");
      }
      sb.append("]}");
    }

    sb.append("],\"summary\":{\"runnable\":")
        .append(runnable)
        .append(",\"waiting\":")
        .append(waiting)
        .append(",\"timedWaiting\":")
        .append(timedWaiting)
        .append(",\"blocked\":")
        .append(blocked)
        .append("}}");

    return sb.toString();
  }

  // --- Serialization & JSON Helpers ---

  @SuppressWarnings("unchecked")
  private void serializeValueToJson(Object obj, StringBuilder sb) {
    if (obj == null) {
      sb.append("null");
      return;
    }
    if (obj instanceof String) {
      sb.append("\"").append(escapeJson((String) obj)).append("\"");
    } else if (obj instanceof Number || obj instanceof Boolean) {
      sb.append(obj);
    } else if (obj instanceof CompositeData) {
      CompositeData cd = (CompositeData) obj;
      sb.append("{");
      boolean first = true;
      for (String key : cd.getCompositeType().keySet()) {
        if (!first) {
          sb.append(",");
        }
        first = false;
        sb.append("\"").append(escapeJson(key)).append("\":");
        serializeValueToJson(cd.get(key), sb);
      }
      sb.append("}");
    } else if (obj instanceof TabularData) {
      TabularData td = (TabularData) obj;
      sb.append("[");
      boolean first = true;
      for (Object row : td.values()) {
        if (!first) {
          sb.append(",");
        }
        first = false;
        serializeValueToJson(row, sb);
      }
      sb.append("]");
    } else if (obj instanceof Map<?, ?>) {
      Map<?, ?> map = (Map<?, ?>) obj;
      sb.append("{");
      boolean first = true;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (!first) {
          sb.append(",");
        }
        first = false;
        sb.append("\"").append(escapeJson(String.valueOf(entry.getKey()))).append("\":");
        serializeValueToJson(entry.getValue(), sb);
      }
      sb.append("}");
    } else if (obj instanceof Collection<?>) {
      Collection<?> col = (Collection<?>) obj;
      sb.append("[");
      boolean first = true;
      for (Object item : col) {
        if (!first) {
          sb.append(",");
        }
        first = false;
        serializeValueToJson(item, sb);
      }
      sb.append("]");
    } else if (obj.getClass().isArray()) {
      sb.append("[");
      int len = Array.getLength(obj);
      for (int i = 0; i < len; i++) {
        if (i > 0) {
          sb.append(",");
        }
        serializeValueToJson(Array.get(obj, i), sb);
      }
      sb.append("]");
    } else {
      sb.append("\"").append(escapeJson(obj.toString())).append("\"");
    }
  }

  private static String escapeJson(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\b", "\\b")
        .replace("\f", "\\f")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }

  private static byte[] readBody(InputStream is) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    byte[] buf = new byte[2048];
    int r;
    while ((r = is.read(buf)) != -1) {
      if (bos.size() + r > 16384) {
        throw new IOException("Management request exceeds maximum size");
      }
      bos.write(buf, 0, r);
    }
    return bos.toByteArray();
  }

  private static void sendJson(HttpExchange exchange, int status, String json) throws IOException {
    byte[] resp = json.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
    exchange.sendResponseHeaders(status, resp.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(resp);
    }
  }

  private static void sendError(HttpExchange exchange, int status, String message)
      throws IOException {
    String json =
        String.format(
            "{\"status\":%d,\"error\":\"%s\",\"timestamp\":%d}",
            status, escapeJson(message), System.currentTimeMillis() / 1000L);
    sendJson(exchange, status, json);
  }

  private static String extractJsonField(String json, String field) {
    Pattern p = Pattern.compile("\"" + Pattern.quote(field) + "\"\\s*:\\s*\"([^\"]+)\"");
    Matcher m = p.matcher(json);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  private static List<String> parseSimpleJsonArray(String json) {
    List<String> list = new ArrayList<>();
    String trimmed = json.trim();
    if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
      list.add(trimmed);
      return list;
    }
    int depth = 0;
    int start = 1;
    for (int i = 1; i < trimmed.length() - 1; i++) {
      char c = trimmed.charAt(i);
      if (c == '{' || c == '[') {
        depth++;
      } else if (c == '}' || c == ']') {
        depth--;
      } else if (c == ',' && depth == 0) {
        String item = trimmed.substring(start, i).trim();
        if (!item.isEmpty()) {
          list.add(item);
        }
        start = i + 1;
      }
    }
    String last = trimmed.substring(start, trimmed.length() - 1).trim();
    if (!last.isEmpty()) {
      list.add(last);
    }
    return list;
  }
}
