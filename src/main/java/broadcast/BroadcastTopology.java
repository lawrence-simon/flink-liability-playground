package broadcast;

import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static broadcast.ExchangeRateProcess.EXCHANGE_RATES_DESCRIPTOR;

public class BroadcastTopology {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> exchangeRateStream = env.addSource(new ExchangeRateSource());

        DataStreamSource<String> stringStream = env.addSource(new SimpleStringSource());

        BroadcastStream<String> broadcastExchangeRatesStream = exchangeRateStream.broadcast(EXCHANGE_RATES_DESCRIPTOR);

        stringStream
                .connect(broadcastExchangeRatesStream)
                .process(new ExchangeRateProcess())
                .print();

        //stringStream.print();

        env.execute();
    }

}
