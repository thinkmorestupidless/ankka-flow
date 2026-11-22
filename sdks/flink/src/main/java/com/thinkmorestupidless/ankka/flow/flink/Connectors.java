package com.thinkmorestupidless.ankka.flow.flink;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * The Kafka source and sink of a port, from its settings and nothing else (research R8): a source
 * reads with its group from the committed offsets and commits them on each checkpoint; a sink
 * writes in transactions under its prefix when its outlet is exactly once.
 */
public final class Connectors {
  private Connectors() {}

  /** The system property kafka-clients 4 reads before it calls a token endpoint (V2). */
  public static final String ALLOWED_URLS = "org.apache.kafka.sasl.oauthbearer.allowed.urls";

  static final String TOKEN_ENDPOINT = "sasl.oauthbearer.token.endpoint.url";

  /** Where a group with no committed offset starts. */
  public enum Reset {
    EARLIEST,
    LATEST,
    NONE
  }

  public static Reset reset(Settings.InletSettings in) {
    String v = in.kafka().getOrDefault("auto.offset.reset", "earliest");
    return switch (v) {
      case "earliest" -> Reset.EARLIEST;
      case "latest" -> Reset.LATEST;
      case "none" -> Reset.NONE;
      default -> throw new IllegalArgumentException(
          "inlet '" + in.port() + "': auto.offset.reset is '" + v + "', not earliest, latest or none");
    };
  }

  public static Properties sourceProperties(Settings.InletSettings in) {
    Properties p = new Properties();
    p.putAll(in.kafka());
    p.remove("auto.offset.reset");
    p.setProperty("group.id", in.group());
    p.setProperty("client.id.prefix", in.clientId());
    p.setProperty("enable.auto.commit", "false");
    p.setProperty("commit.offsets.on.checkpoint", "true");
    return p;
  }

  @SuppressWarnings("deprecation") // the connector takes kafka-clients' deprecated strategy only
  private static OffsetsInitializer startingOffsets(Reset reset) {
    return OffsetsInitializer.committedOffsets(switch (reset) {
      case EARLIEST -> OffsetResetStrategy.EARLIEST;
      case LATEST -> OffsetResetStrategy.LATEST;
      case NONE -> OffsetResetStrategy.NONE;
    });
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  static <A> TypeInformation<Record<A>> recordType() {
    return (TypeInformation<Record<A>>) (TypeInformation) new GenericTypeInfo<>(Record.class);
  }

  public static <A> KafkaSource<Record<A>> source(Settings.InletSettings in, Codec<A> codec) {
    return KafkaSource.<Record<A>>builder()
        .setProperties(sourceProperties(in))
        .setTopics(in.topic())
        .setGroupId(in.group())
        .setStartingOffsets(startingOffsets(reset(in)))
        .setDeserializer(new Deserializer<>(codec))
        .build();
  }

  public static DeliveryGuarantee guarantee(Settings.OutletSettings out) {
    return out.delivery() == Settings.Delivery.EXACTLY_ONCE
        ? DeliveryGuarantee.EXACTLY_ONCE
        : DeliveryGuarantee.AT_LEAST_ONCE;
  }

  public static Optional<String> transactionalIdPrefix(Settings.OutletSettings out) {
    return out.delivery() == Settings.Delivery.EXACTLY_ONCE ? out.transactionalIdPrefix() : Optional.empty();
  }

  public static Properties sinkProperties(Settings.OutletSettings out) {
    Properties p = new Properties();
    p.putAll(out.kafka());
    p.setProperty("client.id", out.clientId());
    return p;
  }

  public static <A> KafkaSink<Record<A>> sink(Settings.OutletSettings out, Codec<A> codec) {
    Properties props = sinkProperties(out);
    KafkaSinkBuilder<Record<A>> b = KafkaSink.<Record<A>>builder()
        .setKafkaProducerConfig(props)
        .setDeliveryGuarantee(guarantee(out))
        .setRecordSerializer(new Serializer<>(out.topic(), codec));
    String servers = props.getProperty("bootstrap.servers");
    if (servers != null) b.setBootstrapServers(servers);
    transactionalIdPrefix(out).ifPresent(b::setTransactionalIdPrefix);
    return b.build();
  }

  /**
   * Adds every token endpoint the settings name to {@link #ALLOWED_URLS}, keeping what it already
   * holds, before any source or sink makes a client in this JVM.
   */
  public static void allowTokenEndpoints(Settings settings) {
    Set<String> urls = new LinkedHashSet<>();
    String before = System.getProperty(ALLOWED_URLS);
    if (before != null && !before.isBlank()) {
      for (String u : before.split(",")) if (!u.isBlank()) urls.add(u.trim());
    }
    List<String> named = new ArrayList<>();
    settings.inlets().forEach(i -> Optional.ofNullable(i.kafka().get(TOKEN_ENDPOINT)).ifPresent(named::add));
    settings.outlets().forEach(o -> Optional.ofNullable(o.kafka().get(TOKEN_ENDPOINT)).ifPresent(named::add));
    if (named.isEmpty()) return;
    urls.addAll(named);
    System.setProperty(ALLOWED_URLS, String.join(",", urls));
  }

  static final class Deserializer<A> implements KafkaRecordDeserializationSchema<Record<A>> {
    private static final long serialVersionUID = 1L;
    private final Codec<A> codec;

    Deserializer(Codec<A> codec) {
      this.codec = codec;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<Record<A>> out) {
      List<Header> headers = new ArrayList<>();
      r.headers().forEach(h -> headers.add(new Header(h.key(), h.value())));
      String key = r.key() == null ? null : new String(r.key(), java.nio.charset.StandardCharsets.UTF_8);
      out.collect(new Record<>(key, codec.decode(r.value()), headers, r.timestamp()));
    }

    @Override
    public TypeInformation<Record<A>> getProducedType() {
      return recordType();
    }
  }

  static final class Serializer<A> implements KafkaRecordSerializationSchema<Record<A>> {
    private static final long serialVersionUID = 1L;
    private final String topic;
    private final Codec<A> codec;

    Serializer(String topic, Codec<A> codec) {
      this.topic = topic;
      this.codec = codec;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Record<A> r, KafkaSinkContext context, Long timestamp) {
      byte[] key = r.key() == null ? null : r.key().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      List<org.apache.kafka.common.header.Header> headers = new ArrayList<>();
      r.headers().forEach(h -> headers.add(new RecordHeader(h.key(), h.value())));
      return new ProducerRecord<>(topic, null, r.timestamp(), key, codec.encode(r.value()), headers);
    }
  }
}
