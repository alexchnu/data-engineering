package ua.edu.chnu.lab4;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RegisterForReflection
public class DayAggregation {

    public long count;
    public double durationSum;
    public double avgDuration;

    public String popularStartStation;
    public Map<String, Long> stationCounts = new HashMap<>();

    public List<String> top3Stations = new ArrayList<>();
    public Map<String, Long> allStationCounts = new HashMap<>();

    public DayAggregation updateTrip(TripEvent trip) {
        count++;
        durationSum += trip.tripduration;
        avgDuration = BigDecimal.valueOf(durationSum / count)
                .setScale(2, RoundingMode.HALF_UP).doubleValue();

        String from = trip.from_station_name;
        long fromCount = stationCounts.merge(from, 1L, Long::sum);
        if (popularStartStation == null || fromCount > stationCounts.getOrDefault(popularStartStation, 0L)) {
            popularStartStation = from;
        }

        allStationCounts.merge(from, 1L, Long::sum);
        allStationCounts.merge(trip.to_station_name, 1L, Long::sum);
        top3Stations = allStationCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(3)
                .map(Map.Entry::getKey)
                .toList();

        return this;
    }
}
