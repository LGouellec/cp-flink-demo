package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.example.avro.ClickEvent;
import org.example.avro.PopularProduct;
import org.example.avro.Product;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class PopularProductPerRegion {

    public static final Duration WINDOW_SIZE = Duration.ofMinutes(1);
    public static final String TOPIC_INPUT = "clickstream";
    public static final String TOPIC_OUTPUT = "popularproduct";
    public static final String BOOSTRAP_SERVER = "kafka.confluent.svc.cluster.local:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://schemaregistry.confluent.svc.cluster.local:8081";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        configureEnvironment(params, env);

        String inputTopic = System.getenv().getOrDefault("TOPIC_INPUT_KAFKA", TOPIC_INPUT);
        String outputTopic = System.getenv().getOrDefault("TOPIC_OUTPUT_KAFKA", TOPIC_OUTPUT);
        String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", SCHEMA_REGISTRY_URL);

        Properties kafkaProps = createKafkaProperties(args);

        ConfluentRegistryAvroDeserializationSchema<ClickEvent> clickEventConfluentRegistryAvroDeserializationSchema =
                ConfluentRegistryAvroDeserializationSchema.forSpecific(ClickEvent.class, schemaRegistryUrl);

        ConfluentRegistryAvroSerializationSchema<PopularProduct> popularProductConfluentRegistryAvroSerializationSchema =
                ConfluentRegistryAvroSerializationSchema.forSpecific(PopularProduct.class,TOPIC_OUTPUT + "-value", schemaRegistryUrl);

        KafkaSource<ClickEvent> source = KafkaSource.<ClickEvent>builder()
                .setTopics(inputTopic)
                .setValueOnlyDeserializer(clickEventConfluentRegistryAvroDeserializationSchema)
                .setProperties(kafkaProps)
                .build();

        WatermarkStrategy<ClickEvent> watermarkStrategy = WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofMillis(200))
                .withIdleness(Duration.ofSeconds(5))
                .withTimestampAssigner((clickEvent, l) -> clickEvent.getEventTime().toEpochMilli());

        DataStream<ClickEvent> clicks = env.fromSource(source, watermarkStrategy, "ClickEvent PopularProduct Source");

        WindowAssigner<Object, TimeWindow> assigner = TumblingEventTimeWindows.of(WINDOW_SIZE);

        DataStream<PopularProduct> statistics = clicks
                .keyBy(ClickEvent::getUserLocation)
                .window(assigner)
                .process(new ProcessWindowFunction<ClickEvent, PopularProduct, CharSequence, TimeWindow>() {
                    @Override
                    public void process(
                            CharSequence location,
                            ProcessWindowFunction<ClickEvent, PopularProduct, CharSequence, TimeWindow>.Context context,
                            Iterable<ClickEvent> elements,
                            Collector<PopularProduct> collector) throws Exception {

                        Map<String, Long> productCounts = new HashMap<>();
                        for (ClickEvent click : elements) {
                            Long newVal = productCounts.getOrDefault(click.getProductId().toString(), 0L);
                            productCounts.put(click.getProductId().toString(), newVal + 1);
                        }

                        List<Product> top3 = productCounts
                                .entrySet()
                                .stream()
                                .map(e -> new Tuple2<>(e.getKey(), e.getValue()))
                                .sorted((a, b) -> Long.compare(b.f1, a.f1))
                                .limit(3)
                                .map((tuple) -> new Product(tuple.f0, tuple.f1))
                                .collect(Collectors.toList());

                        collector.collect(new PopularProduct(
                                new Date(context.window().getStart()).toInstant(),
                                new Date(context.window().getEnd()).toInstant(),
                                location,
                                top3));
                    }
                })
                .name("ClickEvent PopularProduct Counter");

        statistics.sinkTo(
                        KafkaSink.<PopularProduct>builder()
                                .setBootstrapServers(kafkaProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                                .setKafkaProducerConfig(kafkaProps)
                                .setRecordSerializer(
                                        KafkaRecordSerializationSchema.builder()
                                                .setTopic(outputTopic)
                                                .setValueSerializationSchema(popularProductConfluentRegistryAvroSerializationSchema)
                                                .build())
                                .build())
                .name("ClickEvent PopularProduct Sink");

        env.execute("Popular Products");
    }


    private static Properties createKafkaProperties(String[] args) {

        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVER);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "click-event-popular-product");

        System.getenv().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("KAFKA_"))
                .forEach(entry -> {
                    String newKey = entry.getKey().replace("KAFKA_", "").replace("_", ".").toLowerCase();
                    System.out.println(newKey + "=" + entry.getValue());
                    kafkaProps.put(newKey, entry.getValue());
                });

        return kafkaProps;
    }

    private static void configureEnvironment(
            final ParameterTool params,
            final StreamExecutionEnvironment env) {
        env.enableCheckpointing(1000);
    }
}
