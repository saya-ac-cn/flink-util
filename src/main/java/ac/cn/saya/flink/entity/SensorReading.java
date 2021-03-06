package ac.cn.saya.flink.entity;

/**
 * 传感器
 * @Title: SensorReading
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/29 08:37
 * @Description:
 */

public class SensorReading {
    /**
     * 传感器id
     */
    public String id;

    /**
     * 上报时间
     */
    public Long timestamp;

    /**
     * 温度
     */
    public Double temperature;

    public SensorReading() {
    }

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading:{" +
                "id:" + id +
                ", timestamp:" + timestamp +
                ", temperature:" + temperature +
                '}';
    }

//    @Override
//    public String toString() {
//        return  id +
//                "," + timestamp +
//                "," + temperature;
//    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }
}
