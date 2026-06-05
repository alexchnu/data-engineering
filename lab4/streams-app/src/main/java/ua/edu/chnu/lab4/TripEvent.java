package ua.edu.chnu.lab4;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class TripEvent {

    public String trip_id;
    public String start_time;
    public double tripduration;
    public String from_station_name;
    public String to_station_name;
}
