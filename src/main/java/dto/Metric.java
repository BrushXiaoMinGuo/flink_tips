package dto;

public class Metric {
    public String name;
    private long timestamp;
    public int value;

    public Metric() {
    }

    public Metric(String name, int value) {
        this.name = name;
        this.value = value;
    }

    public Metric(String name, long timestamp, int value) {
        this.name = name;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
