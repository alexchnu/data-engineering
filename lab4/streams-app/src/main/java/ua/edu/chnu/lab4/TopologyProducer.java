package ua.edu.chnu.lab4;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

@ApplicationScoped
public class TopologyProducer {

    private static final String INPUT_TOPIC = "trips-topic1";
    private static final String AGGREGATED_TOPIC = "trips-aggregated";

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        ObjectMapperSerde<TripEvent> tripSerde = new ObjectMapperSerde<>(TripEvent.class);
        ObjectMapperSerde<DayAggregation> aggSerde = new ObjectMapperSerde<>(DayAggregation.class);

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), tripSerde))
                .groupBy(
                        (key, trip) -> trip.start_time.substring(0, 10),
                        Grouped.with(Serdes.String(), tripSerde))
                .aggregate(
                        DayAggregation::new,
                        (day, trip, agg) -> agg.updateTrip(trip),
                        Materialized.<String, DayAggregation>as(
                                Stores.persistentKeyValueStore("trips-day-store"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggSerde))
                .toStream()
                .to(AGGREGATED_TOPIC, Produced.with(Serdes.String(), aggSerde));

        return builder.build();
    }
}
