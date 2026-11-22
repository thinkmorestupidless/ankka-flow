package com.thinkmorestupidless.ankka.flow.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The two ways a JSON document is written: RFC 8785 for a schema's fingerprint, as ankka computes
 * it, and the descriptor's canonical layout (keys sorted, two-space indent, a trailing newline), as
 * the descriptor module writes it. Ported from that module's {@code Canonical} and {@code Json}.
 */
public final class Canonical {
  private Canonical() {}

  /** {@code sha256:} and the lowercase hex SHA-256 of the document under RFC 8785. */
  public static String fingerprint(byte[] schema) {
    JsonNode node;
    try {
      node = new ObjectMapper().readTree(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("is not JSON: " + e.getMessage(), e);
    }
    if (node == null || node.isMissingNode()) {
      throw new IllegalArgumentException("is not JSON: it is empty");
    }
    StringBuilder out = new StringBuilder();
    rfc8785(node, out);
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(out.toString().getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder("sha256:");
      for (byte b : digest) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void rfc8785(JsonNode node, StringBuilder out) {
    if (node.isNull()) {
      out.append("null");
    } else if (node.isBoolean()) {
      out.append(node.booleanValue() ? "true" : "false");
    } else if (node.isNumber()) {
      out.append(number(node.doubleValue()));
    } else if (node.isTextual()) {
      string(node.textValue(), out);
    } else if (node.isArray()) {
      out.append('[');
      for (int i = 0; i < node.size(); i++) {
        if (i > 0) out.append(',');
        rfc8785(node.get(i), out);
      }
      out.append(']');
    } else {
      // UTF-16 code unit order is String's natural order.
      TreeMap<String, JsonNode> fields = new TreeMap<>();
      for (Map.Entry<String, JsonNode> e : node.properties()) {
        fields.put(e.getKey(), e.getValue());
      }
      out.append('{');
      boolean first = true;
      for (Map.Entry<String, JsonNode> e : fields.entrySet()) {
        if (!first) out.append(',');
        first = false;
        string(e.getKey(), out);
        out.append(':');
        rfc8785(e.getValue(), out);
      }
      out.append('}');
    }
  }

  /** Quote, backslash and control characters escaped; everything else as it is. */
  static void string(String s, StringBuilder out) {
    out.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        case '\b' -> out.append("\\b");
        case '\f' -> out.append("\\f");
        default -> {
          if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
          else out.append(c);
        }
      }
    }
    out.append('"');
  }

  /**
   * ECMAScript's Number::toString: an integer below 1e21 as its digits, otherwise the shortest
   * round-trip digits, with an exponent at or past 1e21 and below 1e-6.
   */
  static String number(double d) {
    if (d == 0.0) return "0";
    if (d == Math.rint(d) && Math.abs(d) < 1e21) {
      return BigDecimal.valueOf(d).toBigInteger().toString();
    }
    String repr = Double.toString(d);
    String mantissa = repr;
    int exp = 0;
    int e = repr.indexOf('E');
    if (e >= 0) {
      mantissa = repr.substring(0, e);
      exp = Integer.parseInt(repr.substring(e + 1));
    }
    boolean negative = mantissa.startsWith("-");
    String digitsWithPoint = negative ? mantissa.substring(1) : mantissa;
    int dot = digitsWithPoint.indexOf('.');
    String intPart = dot >= 0 ? digitsWithPoint.substring(0, dot) : digitsWithPoint;
    String frac = dot >= 0 ? digitsWithPoint.substring(dot + 1) : "";
    frac = frac.replaceAll("0+$", "");
    String all = intPart + frac;
    String digits = all.replaceAll("^0+", "");
    if (digits.isEmpty()) digits = "0";
    int leadingZeros = all.length() - all.replaceAll("^0+", "").length();
    int n = intPart.length() + exp - leadingZeros;
    int k = digits.length();
    String body;
    if (k <= n && n <= 21) {
      body = digits + "0".repeat(n - k);
    } else if (0 < n && n <= 21) {
      body = digits.substring(0, n) + "." + digits.substring(n);
    } else if (-6 < n && n <= 0) {
      body = "0." + "0".repeat(-n) + digits;
    } else {
      int x = n - 1;
      String m = k == 1 ? digits : digits.charAt(0) + "." + digits.substring(1);
      body = m + "e" + (x >= 0 ? "+" : "-") + Math.abs(x);
    }
    return (negative ? "-" : "") + body;
  }

  /**
   * The descriptor's layout over a tree of {@link Map}s, {@link List}s and strings: keys sorted,
   * two-space indent, {@code ": "} between a key and its value, a trailing newline.
   */
  static String pretty(Object value) {
    StringBuilder out = new StringBuilder();
    pretty(value, out, 0);
    return out.append('\n').toString();
  }

  private static void pretty(Object value, StringBuilder out, int depth) {
    if (value instanceof Map<?, ?> m) {
      if (m.isEmpty()) {
        out.append("{}");
        return;
      }
      TreeMap<String, Object> sorted = new TreeMap<>();
      m.forEach((k, v) -> sorted.put((String) k, v));
      out.append("{\n");
      int i = 0;
      for (Map.Entry<String, Object> e : sorted.entrySet()) {
        out.append("  ".repeat(depth + 1));
        string(e.getKey(), out);
        out.append(": ");
        pretty(e.getValue(), out, depth + 1);
        if (++i < sorted.size()) out.append(',');
        out.append('\n');
      }
      out.append("  ".repeat(depth)).append('}');
    } else if (value instanceof List<?> l) {
      if (l.isEmpty()) {
        out.append("[]");
        return;
      }
      out.append("[\n");
      List<?> items = new ArrayList<>(l);
      for (int i = 0; i < items.size(); i++) {
        out.append("  ".repeat(depth + 1));
        pretty(items.get(i), out, depth + 1);
        if (i < items.size() - 1) out.append(',');
        out.append('\n');
      }
      out.append("  ".repeat(depth)).append(']');
    } else {
      string(String.valueOf(value), out);
    }
  }
}
