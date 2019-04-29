package club.cleland.spark_learn.core;

import scala.math.Ordered;
import java.io.Serializable;

public class SecondSortKey implements Ordered<SecondSortKey>, Serializable {
    private String first;
    private int second;

    public SecondSortKey(String first, int second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public int compare(SecondSortKey that) {
        int comp = this.getFirst().compareTo(that.getFirst());
        if (comp == 0){
            return Integer.valueOf(this.getSecond()).compareTo(that.getSecond());
        }
        return comp;
    }

    public boolean $less(SecondSortKey that) {
        return false;
    }

    public boolean $greater(SecondSortKey that) {
        return false;
    }

    public boolean $less$eq(SecondSortKey that) {
        return false;
    }

    public boolean $greater$eq(SecondSortKey that) {
        return false;
    }

    public int compareTo(SecondSortKey that) {
        int comp = this.getFirst().compareTo(that.getFirst());
        if (comp == 0){
            return Integer.valueOf(this.getSecond()).compareTo(that.getSecond());
        }
        return comp;
    }
}
