package coprocess;

import data.Breach;
import data.Liability;
import data.Setting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CoProcessTopology {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<Setting, Long> settingStream = env.addSource(new SettingsSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(Setting::getSelectionId);

        KeyedStream<Liability, Long> liabilityStream = env.addSource(new LiabilitiesSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(Liability::getSelectionId);


        SingleOutputStreamOperator<Breach> breachStream = liabilityStream
                .connect(settingStream)
                .flatMap(new BreachCalculator());

        breachStream.print();

        env.execute();
    }
}
