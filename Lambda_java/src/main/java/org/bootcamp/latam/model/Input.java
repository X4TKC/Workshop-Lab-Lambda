package org.bootcamp.latam.model;

public class Input {
    private String name;
    private String value;

    public Input() {
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

    public void setValue(String type) {
        this.value = type;
    }

    @Override
    public String toString() {
        return "Input{" +
                "name='" + name + '\'' +
                ", type='" + value + '\'' +
                '}';
    }
}
