package coprocess;

import data.Breach;
import data.Liability;
import data.Setting;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CoProcessTableTopology {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        KeyedStream<Liability, Long> liabilityStream = env.addSource(new LiabilitiesSource())
                .keyBy(Liability::getSelectionId);

        KeyedStream<Setting, Long> settingStream = env.addSource(new SettingsSource())
                .keyBy(Setting::getSelectionId);

        tEnv.createTemporaryView("liabilities", liabilityStream);
        tEnv.createTemporaryView("settings", settingStream);

        Table breachTable = tEnv.sqlQuery("SELECT l.eventTime, l.selectionId, l.liability AS liabilityAtBreach, s.liabilityLimit AS limitAtBreach " +
                " FROM liabilities AS l, " +
                " settings AS s " +
                " WHERE l.selectionId = s.selectionId " +
                " AND l.liability > s.liabilityLimit " +
                " AND l.eventTime > s.eventTime");

        DataStream<Breach> breachStream = tEnv.toDataStream(breachTable, Breach.class);
        breachStream.print();
        env.execute();
    }
}
