
package com.kartikiyer.cdc.serde;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.kartikiyer.cdc.model.CellData;
import com.kartikiyer.cdc.model.EventType;
import com.kartikiyer.cdc.model.RowEventData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class StringDeSerializer implements Serializable, DebeziumDeserializationSchema<RowEventData> {
    private static final long serialVersionUID = -3168848963265670603L;

    TypeInformation<RowEventData> struct;

    @Override
    public void deserialize(SourceRecord record, Collector<RowEventData> out) {

        RowEventData rowEventData = new RowEventData();

        Struct event = ((Struct) record.value());
        Struct before = (Struct) event.get("before");
        Struct after = (Struct) event.get("after");
        Struct changes;

        long timestamp = Long.parseLong(((Struct)event.get("source")).get("ts_ms").toString());
        if (timestamp == 0)
            timestamp = Long.parseLong(event.get("ts_ms").toString());

        rowEventData.setTimeStamp(timestamp);

        // insert event
        if (before == null) {
            changes = after;
            rowEventData.setEventType(EventType.INSERT);
        }
        // delete event
        else if (after == null) {
            changes = before;
            rowEventData.setEventType(EventType.DELETE);
        }
        // update event
        else {
            changes = after;
            rowEventData.setEventType(EventType.UPDATE);
        }

        List<CellData> columns = new LinkedList<>();
        changes.schema().fields().forEach(field -> {
                    String name = field.name();
                    String value = changes.get(name).toString();
                    String datatype = field.schema().type().getName();

                    columns.add(new CellData(name, value, datatype));
                }
        );

        rowEventData.setColumns(columns);
        out.collect(rowEventData);
    }

    @Override
    public TypeInformation<RowEventData> getProducedType() {
        return struct;
    }
}

