package coprocess;

import data.Setting;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SettingsSource implements SourceFunction<Setting> {

    private static final Instant START_INSTANT = Instant.MIN;

    private static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Setting> ctx) throws Exception {

        //Initial Setting for all selection IDs.
        addSetting(ctx, 0, 1, 200.0);
        addSetting(ctx, 0, 2, 200.0);
        addSetting(ctx, 0, 3, 200.0);

        //No adjustment for selection 1

        //No adjustment for selection 2

        //For selection 3, reduce the limit to 100 after processing 2 liabilities.
        addSetting(ctx, 15, 3, 100.0);

        while(running) {
            //Do nothing
        }
    }

    private void addSetting(SourceContext<Setting> ctx, int delay, long selectionId, double liabilityLimit) {
        executor.schedule(() -> ctx.collect(new Setting(START_INSTANT.plus(Duration.ofMinutes(delay)), selectionId, liabilityLimit)),
                delay,
                TimeUnit.SECONDS);
    }

    @Override
    public void cancel() {
        running = false;
    }
}
