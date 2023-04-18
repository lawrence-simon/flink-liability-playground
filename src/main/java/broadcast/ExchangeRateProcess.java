package broadcast;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExchangeRateProcess extends BroadcastProcessFunction<String, String, String> {

    private static final Logger log = LoggerFactory.getLogger(ExchangeRateProcess.class);

    public static final MapStateDescriptor<String, String> EXCHANGE_RATES_DESCRIPTOR = new MapStateDescriptor<>(
            "exchangeRatesState",
            String.class,
            String.class
    );


    @Override
    public void processElement(String input, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
        ReadOnlyBroadcastState<String, String> state = readOnlyContext.getBroadcastState(EXCHANGE_RATES_DESCRIPTOR);
        String key = state.get("key");
        collector.collect(input + " " + key);
    }

    @Override
    public void processBroadcastElement(String exchangeRates, Context context, Collector<String> collector) throws Exception {
        BroadcastState<String, String> state = context.getBroadcastState(EXCHANGE_RATES_DESCRIPTOR);
        state.put("key", exchangeRates);
    }
}
