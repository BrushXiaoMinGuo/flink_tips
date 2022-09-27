package dto;

public class Metric {


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public Metric() {
    }

    public Metric(String name, int value) {
        this.name = name;
        this.value = value;
    }

    public String name;

    @Override
    public String toString() {
        return "Metric{" +
                "name='" + name + '\'' +
                ", value=" + value +
                '}';
    }

    public int value;
}
