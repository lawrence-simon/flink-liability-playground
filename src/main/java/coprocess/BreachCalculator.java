package coprocess;

import data.Breach;
import data.Liability;
import data.Setting;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;


public class BreachCalculator extends RichCoFlatMapFunction<Liability, Setting, Breach> {

    private ValueState<Setting> settingState;

    @Override
    public void open(Configuration parameters) throws Exception {
        settingState = getRuntimeContext().getState(new ValueStateDescriptor<>("settingState", Setting.class));
    }

    @Override
    public void flatMap1(Liability liability, Collector<Breach> collector) throws Exception {
        Setting setting = settingState.value();
        if (setting == null) {
            throw new Exception("Setting state is not yet initialized for liability with selectionID " + liability.getSelectionId());
        }

        double currentLimit = setting.getLiabilityLimit();
        double currentLiability = liability.getLiability();
        if (currentLiability > currentLimit) {
            collector.collect(new Breach(liability.getEventTime(), liability.getSelectionId(), currentLiability, currentLimit));
        }
    }

    @Override
    public void flatMap2(Setting setting, Collector<Breach> collector) throws Exception {
        Setting currentSetting = settingState.value();
        if (currentSetting == null) {
            settingState.update(setting);
        } else if (setting.getEventTime().isAfter(currentSetting.getEventTime())) {
            settingState.update(setting);
        }
    }
}
