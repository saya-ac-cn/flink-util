package ac.cn.saya.flink.source;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 自定义传感器数据来源
 * @Title: DiySensorSource
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/29 09:42
 * @Description:
 */

public class DiySensorSource implements SourceFunction<SensorReading> {

    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random random = new Random();
        Map<String, Double> sensorMap = new HashMap<String, Double>();
        for (int i = 0; i < 10;i++){
            // 第一次生成传感器温度，并指定温度范围（第一次）
            sensorMap.put("sensor_"+(i+1),60+random.nextGaussian()*20);
        }
        while (running){
            for (String sensorId:sensorMap.keySet()) {
                double temperature = sensorMap.get(sensorId) + random.nextGaussian();
                sensorMap.put(sensorId,temperature);
                ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),temperature));
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
