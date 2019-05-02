package club.cleland.spark_learning.spark_sql.core;

import java.io.Serializable;

public class Person implements Serializable {
    private String name;
    private Double core;

    public Person(String name, Double core) {
        this.name = name;
        this.core = core;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getCore() {
        return core;
    }

    public void setCore(Double core) {
        this.core = core;
    }
}
