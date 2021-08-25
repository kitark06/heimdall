package com.kartikiyer.cdc.model;

import java.util.*;
import java.util.stream.Collectors;

public class RowEventData {

    Set<String> primaryTypes = new HashSet<>(Arrays
            .asList("BOOLEAN", "INT16", "INT32", "INT64", "FLOAT32", "FLOAT64"));

    EventType eventType;
    List<CellData> columns = new LinkedList<>();
    long timeStamp;

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public List<CellData> getColumns() {
        return columns;
    }

    public void setColumns(List<CellData> columns) {
        this.columns = columns;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "com.kartik.RowEventData{" +
                "eventType=" + eventType +
                ", columns=" + columns +
                ", timeStamp=" + timeStamp +
                '}';
    }

    public String getJson() {
        return "{" +
                columns.stream()
                        .map(x -> {
                            if (primaryTypes.contains(x.dataType) || primaryTypes.contains(x.dataType.toUpperCase()))
                                return "\"" + x.name + "\"" + ":" + x.value;
                            else
                                return "\"" + x.name + "\"" + ":" + "\"" + x.value + "\"";
                        })
                        .collect(Collectors.joining(", "))
                + "}";
    }
}
