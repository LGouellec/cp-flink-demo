package org.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.avro.ClickEvent;

import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ClickEventGenerator {
    public static final int EVENTS_PER_WINDOW = 1000;

    private static final Map<String, List<String>> catalog = new HashMap<>();

    // this calculation is only accurate as long as EVENTS_PER_WINDOW divides the
    // window size
    // 15s
    public static final int WINDOW_SECONDS_SIZE = 15;
    public static final long DELAY = WINDOW_SECONDS_SIZE * 1000 / EVENTS_PER_WINDOW;
    public static final String TOPIC_INPUT = "input";
    public static final String BOOSTRAP_SERVER = "kafka.confluent.svc.cluster.local:9092";

    private static void initializeCatalog() {

        catalog.put("Tops", Arrays.asList(
                "Swiftly Tech Short Sleeve", "Metal Vent Tech Short Sleeve", "Align Tank", "Cool Racerback",
                "Surge Warm Long Sleeve", "Energy Bra", "Like a Cloud Bra", "Wunder Puff Vest",
                "Perfectly Oversized Crew", "Scuba Oversized Full Zip", "License to Train Tee", "Back in Action LS",
                "Rest Less Pullover", "Define Jacket", "Hooded Define Jacket", "Wunder Puff Jacket",
                "Perfectly Oversized Cropped Crew", "Everywhere Belt Bag", "Invigorate Tank", "Power Pivot Tank",
                "Align Bodysuit", "Energy Long Sleeve", "Train to Be Tank", "Swiftly Tech Racerback",
                "Sculpt Tank", "Down for It All Vest", "Wunder Puff Cropped Jacket", "Like a Cloud Longline Bra",
                "Ribbed Waist Define Jacket", "Fast and Free Jacket"));

        catalog.put("Bottoms", Arrays.asList(
                "Align Pant 25\"", "Align Pant 28\"", "Wunder Train High-Rise Tight", "Fast and Free Tight",
                "Base Pace High-Rise Tight", "Invigorate High-Rise Tight", "Hotty Hot Short", "Track That Short",
                "Pace Rival Skirt", "Align High-Rise Short", "Commission Pant Slim", "ABC Jogger",
                "License to Train Short", "Pace Breaker Short", "Everlux High-Rise Crop", "Commission Classic-Fit Pant",
                "Warpstreme Jogger", "Softstreme High-Rise Pant", "Stretch High-Rise Pant", "On the Fly Jogger",
                "City Sweat Jogger", "Surge Jogger", "Studio Jogger", "Modal Fleece Wide-Leg Pant",
                "Loungeful High-Rise Pant", "Commission Pleated Pant", "Stretch High-Rise Short", "Warpstreme Short",
                "Metal Vent Tech Short", "Stretch Tech Short"));

        catalog.put("Outerwear", Arrays.asList(
                "Wunder Puff Parka", "Another Mile Jacket", "Down for It All Jacket", "Rain Rebel Jacket",
                "Expeditionist Jacket", "Cloudscape Jacket", "Pack It Down Vest", "Define Jacket Luon",
                "Perfectly Oversized Hoodie", "Half Zip Scuba Hoodie", "City Sweat Zip Hoodie", "At Ease Hoodie",
                "Metal Vent Tech Hoodie", "Surge Warm Full Zip", "Wunder Puff Long Vest", "Insulated Quilted Jacket",
                "Lab City Parka", "Storm Field Jacket", "Winter Warrior Parka", "Reversible Down Jacket",
                "Lightweight Hiking Jacket", "Packable Rain Jacket", "Wunder Puff Bomber", "All Season Softshell",
                "Mountain Trek Anorak", "Hybrid Puffer", "Studio Softshell", "Lightweight Running Vest",
                "Thermal Training Jacket", "Insulated Gilet"));

        catalog.put("Accessories", Arrays.asList(
                "Everywhere Belt Bag 1L", "Everywhere Belt Bag 2L", "Back to Life Sport Bottle", "Back to Life Tumbler",
                "Metal Vent Tech Cap", "Fast and Free Running Hat", "Wunder Puff Mittens", "Wunder Puff Scarf",
                "Yoga Mat 5mm", "Reversible Yoga Mat", "The Yoga Block", "Double Roller",
                "Loop It Up Mat Strap", "All Day Backpack", "City Adventurer Backpack", "Everywhere Backpack",
                "Mini Belt Bag", "Wunder Puff Ear Warmer", "Running Gloves", "Surge Warm Gloves",
                "Train Hard Headband", "Metal Vent Tech Beanie", "License to Train Cap", "Crossbody Camera Bag",
                "Everywhere Sling Bag", "Phone Crossbody", "Fast and Free Belt Bag", "Align Yoga Strap",
                "Mesh Duffel Bag", "City Sweat Duffel"));

        catalog.put("Footwear", Arrays.asList(
                "Chargefeel Low", "Chargefeel Mid", "Strongfeel Training Shoe", "Blissfeel Running Shoe",
                "Restfeel Slide", "All Day Sneaker", "Chargefeel 2", "City Cross Sneaker",
                "Hikefeel Trail Shoe", "Flowfeel Slide", "Studio Sneaker", "Lightform Training Shoe",
                "Hikefeel Mid", "Windfeel Running Shoe", "Softfeel Slip-On", "Hybrid Trainer",
                "Urban Trail Runner", "Everyday Trainer", "Climbfeel Approach Shoe", "Swim Slide",
                "Outdoor Sandal", "Runfeel Speed Shoe", "Cushfeel Trainer", "Liftfeel Gym Shoe",
                "Peak Trail Runner", "All Weather Trainer", "Roadfeel Runner", "Gripfeel Cross Trainer",
                "Stretchfeel Sneaker", "Coolfeel Mesh Trainer"));

        catalog.put("Bags", Arrays.asList(
                "Everywhere Tote Bag", "Wunderlust Duffel", "Pack It Down Tote", "City Adventure Tote",
                "Convertible Backpack Tote", "Studio Bag", "Travel Yoga Bag", "Lightweight Gym Bag",
                "Mini Tote", "Large Shopper Bag", "Packable Backpack", "Crossbody Tote",
                "Lab Messenger Bag", "Laptop Sleeve Tote", "Day Tripper Tote", "Wunderlust Backpack",
                "Weekender Duffel", "Beach Tote", "Dry Bag", "Roll Top Backpack",
                "Mountain Pack", "Compact Sling", "Gear Organizer Pouch", "Mini Crossbody",
                "City Commuter Bag", "Everyday Sling", "Hybrid Tote Backpack", "Sport Sack",
                "Adventure Satchel", "Essential Sling Bag"));

        catalog.put("Yoga Gear", Arrays.asList(
                "Reversible Yoga Mat 5mm", "Travel Yoga Mat", "Yoga Mat Towel", "Yoga Block Foam",
                "Yoga Strap Cotton", "Balance Ball", "Yoga Wheel", "Yoga Knee Pad",
                "Meditation Cushion", "Bolster Pillow", "Eye Pillow", "Sandbag",
                "Yoga Blanket", "Foam Roller", "Cork Yoga Block", "Yoga Mat Cleaner",
                "Hot Yoga Mat Towel", "Grip Socks", "Meditation Bench", "Stretching Strap",
                "Resistance Band Set", "Yoga Bag", "Flow Mat 3mm", "Eco-Friendly Mat",
                "Travel Mat Bag", "Foldable Yoga Mat", "Yoga Disc Cushion", "Yoga Alignment Mat",
                "Dual Layer Mat", "Yoga Prop Set"));

        catalog.put("Men's Training", Arrays.asList(
                "Metal Vent Tech Short Sleeve", "License to Train Short", "City Sweat Jogger", "Surge Jogger",
                "Commission Pant", "Pace Breaker Short", "At Ease Hoodie", "Surge Warm Full Zip",
                "ABC Jogger", "Surge Jacket", "Like Nothing Short", "T.H.E. Short",
                "Surge Tight", "Surge Warm Jogger", "License to Train Pant", "Always in Motion Boxer",
                "License to Train Hoodie", "City Sweat Pullover Hoodie", "Fast and Free Short Sleeve", "Surge Half Zip",
                "Metal Vent Tech Long Sleeve", "City Sweat Short", "At Ease Pant", "Surge Hybrid Pant",
                "Commission Jogger", "Stretch Warp Pant", "Warpstreme Jogger", "License to Train LS",
                "Fast and Free LS", "Pace Rival Short"));

        catalog.put("Women's Training", Arrays.asList(
                "Invigorate High-Rise Tight", "Wunder Train High-Rise Tight", "Base Pace High-Rise Tight",
                "Align High-Rise Short",
                "Swiftly Tech Short Sleeve", "Cool Racerback", "Energy Bra", "Like a Cloud Bra",
                "Sculpt Tank", "Rest Less Pullover", "Define Jacket", "Hooded Define Jacket",
                "Perfectly Oversized Crew", "Scuba Oversized Full Zip", "Power Pivot Tank", "Align Bodysuit",
                "Energy Long Sleeve", "Train to Be Tank", "Down for It All Vest", "Like a Cloud Longline Bra",
                "Fast and Free Jacket", "Everywhere Belt Bag", "Invigorate Tank", "Align Tank",
                "Swiftly Tech Racerback", "Wunder Puff Jacket", "Wunder Puff Vest", "Ribbed Waist Define Jacket",
                "Fast and Free Tight", "Surge Warm Long Sleeve"));

        catalog.put("Seasonal", Arrays.asList(
                "Holiday Scarf", "Winter Wool Beanie", "Fleece Mittens", "Thermal Socks",
                "Rainproof Poncho", "Summer Tank", "Lightweight Running Cap", "Sun Hat",
                "Beach Sandals", "Festival Bag", "Travel Hoodie", "Insulated Raincoat",
                "Winter Running Tights", "Snow Boots", "Waterproof Backpack", "Ski Gloves",
                "Puffer Jacket", "Quilted Vest", "Holiday Sweater", "Ski Pants",
                "Thermal Base Layer", "Rain Jacket", "Summer Shorts", "Breathable Tee",
                "Outdoor Hiking Pants", "Windbreaker", "Down Scarf", "Ear Warmers",
                "Rain Pants", "Beach Cover-Up"));

    }

    public static void main(String[] args) throws Exception {

        initializeCatalog();

        String topic = System.getenv().getOrDefault("TOPIC_KAFKA", TOPIC_INPUT);

        Properties kafkaProps = createKafkaProperties(args);

        KafkaProducer<String, ClickEvent> producer = new KafkaProducer<>(kafkaProps);

        ClickIterator clickIterator = new ClickIterator(catalog);

        while (true) {

            ProducerRecord<String, ClickEvent> record = new ProducerRecord<>(topic,
                    null,
                    clickIterator.next());

            producer.send(record);

            Thread.sleep(DELAY);
        }
    }

    private static Properties createKafkaProperties(String[] args) {

        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVER);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        System.getenv().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("KAFKA_"))
                .forEach(entry -> {
                    String newKey = entry.getKey().replace("KAFKA_", "").replace("_", ".").toLowerCase();
                    System.out.println(newKey + "=" + entry.getValue());
                    kafkaProps.put(newKey, entry.getValue());
                });

        return kafkaProps;
    }

    static class ClickIterator {

        private static final SecureRandom RND = new SecureRandom();

        private static final List<String> cities = Arrays.asList(
                "Paris",
                "London",
                "New York",
                "Tokyo",
                "Sydney",
                "Toronto",
                "Berlin",
                "Madrid",
                "Rome",
                "Lisbon",
                "Dubai",
                "Singapore",
                "Cape Town",
                "San Francisco",
                "Buenos Aires",
                "Cairo",
                "Bangkok",
                "Moscow",
                "Seoul",
                "Amsterdam"
            );

        private static final String[] ADJECTIVES = {
                "swift", "brave", "quiet", "bold", "clever", "quick", "bright", "calm", "eager", "fresh",
                "gentle", "happy", "jolly", "kind", "lively", "merry", "neat", "proud", "sharp", "witty",
                "solid", "steady", "sunny", "noble", "sly", "chill", "plucky", "snappy", "keen", "spry"
        };

        private static final String[] NOUNS = {
                "lynx", "falcon", "tiger", "eagle", "panda", "otter", "wolf", "sparrow", "badger", "bison",
                "cougar", "dolphin", "heron", "ibis", "jaguar", "koala", "lemur", "moose", "narwhal", "ocelot",
                "puma", "quokka", "raven", "stoat", "tapir", "urchin", "viper", "walrus", "yak", "zorilla"
        };

        /** Génère un login de la forme adjectif_nomNNN (NNN = 000..999) */
        private static String generateUserId() {
            String a = ADJECTIVES[RND.nextInt(ADJECTIVES.length)];
            String n = NOUNS[RND.nextInt(NOUNS.length)];
            String num = String.format("%03d", RND.nextInt(1000));
            return a + "_" + n + num;
        }

        private final ArrayList<Map.Entry<String, List<String>>> catalogList;

        ClickIterator(Map<String, List<String>> catalog) {
            catalogList = new ArrayList<>(catalog.entrySet());
        }

        ClickEvent next() {
            Map.Entry<String, List<String>> randomEntry =
                    catalogList.get(ThreadLocalRandom.current().nextInt(catalogList.size()));
            
            return new ClickEvent(
                    UUID.randomUUID().toString(),
                    generateUserId(),
                    randomEntry.getValue().get(ThreadLocalRandom.current().nextInt(randomEntry.getValue().size())),
                    cities.get(ThreadLocalRandom.current().nextInt(cities.size())),
                    nextTimestamp().toInstant(),
                    randomEntry.getKey());
        }

        private Date nextTimestamp() {
            long nextTimestamp = System.currentTimeMillis();
            return new Date(nextTimestamp);
        }
        
    }
}
