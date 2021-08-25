
package com.kartikiyer.cdc.model;

public class CellData {

    String name;
    String value;
    String dataType;

    public CellData(String name, String value, String dataType) {
        this.name = name;
        this.value = value;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    @Override
    public String toString() {
        return "com.kartik.CellData{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                ", datatype='" + dataType + '\'' +
                '}';
    }
}

