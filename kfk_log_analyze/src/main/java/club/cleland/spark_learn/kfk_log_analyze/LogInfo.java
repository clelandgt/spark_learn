package club.cleland.spark_learn.kfk_log_analyze;

import java.io.Serializable;

public class LogInfo implements Serializable {
    private long timeStamp;
    private long upTraffic;
    private long downTraffic;

    public LogInfo(){

    }

    public LogInfo(long timeStamp, long upTraffic, long downTraffic) {
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

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public void setUpTraffic(long upTraffic) {
        this.upTraffic = upTraffic;
    }

    public void setDownTraffic(long downTraffic) {
        this.downTraffic = downTraffic;
    }
}
