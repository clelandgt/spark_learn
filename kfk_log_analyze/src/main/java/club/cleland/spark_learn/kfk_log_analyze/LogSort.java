package club.cleland.spark_learn.kfk_log_analyze;

import scala.Serializable;
import scala.math.Ordered;

public class LogSort implements Ordered<LogSort>, Serializable {
    private long timeStamp;
    private long upTraffic;
    private long downTraffic;

    public LogSort(){

    }

    public LogSort(long timeStamp, long upTraffic, long downTraffic) {
        this.timeStamp = timeStamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public long getUpTraffic() {
        return upTraffic;
    }

    public long getDownTraffic() {
        return downTraffic;
    }

    public int compare(LogSort that) {
        int comp = Long.valueOf(this.getUpTraffic()).compareTo(that.getUpTraffic());
        if(comp == 0){
            comp = Long.valueOf(this.getDownTraffic()).compareTo(that.getDownTraffic());
        }
        if(comp == 0){
            comp = Long.valueOf(this.getTimeStamp()).compareTo(that.getTimeStamp());
        }

        return comp;
    }

    public boolean $less(LogSort that) {
        //return super.$less(that);
        return false;
    }

    public boolean $greater(LogSort that) {
        //return super.$greater(that);
        return false;
    }

    public boolean $less$eq(LogSort that) {
        //return super.$less$eq(that);
        return false;
    }

    public boolean $greater$eq(LogSort that) {
        //return super.$greater$eq(that);
        return false;
    }

    public int compareTo(LogSort that) {
        int comp = Long.valueOf(this.getUpTraffic()).compareTo(that.getUpTraffic());
        if(comp == 0){
            comp = Long.valueOf(this.getDownTraffic()).compareTo(that.getDownTraffic());
        }
        if(comp == 0){
            comp = Long.valueOf(this.getTimeStamp()).compareTo(that.getTimeStamp());
        }

        return comp;
    }
}
