package broadcast;


import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class ExchangeRateSource extends RichSourceFunction<String> {

    private static final long POLL_INTERVAL = 30000;
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);

    protected static OkHttpClient okHttpClient = new OkHttpClient();

    protected static Logger log = LoggerFactory.getLogger(ExchangeRateSource.class);

    private transient ScheduledFuture<?> scheduledFuture;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        this.scheduledFuture = SCHEDULER.scheduleAtFixedRate(
                () -> this.pollExchangeRates().ifPresent(sourceContext::collect),
                0,
                POLL_INTERVAL,
                TimeUnit.MILLISECONDS
        );

        while (!this.scheduledFuture.isDone()) {
            Thread.sleep(10000);
        }
    }

    @Override
    public void cancel() {
        this.scheduledFuture.cancel(true);
    }

    public Optional<String> pollExchangeRates() {
        //System.out.println("Requesting new exchange rates");
        log.info("Requesting new exchange rates");
        Request request = new Request.Builder().url("http://cxs.qa.internal/CurrencyExchange/v1/retrieveExchangeRates?fromCurrency=GBP").build();
        try {
            Response response = okHttpClient.newCall(request).execute();
            if (response.isSuccessful()) {
                String body = Objects.requireNonNull(response.body()).string();
                return Optional.of(body);
            }
        } catch (IOException e) {
            System.out.println(e);
        }
        return Optional.empty();
    }
}
