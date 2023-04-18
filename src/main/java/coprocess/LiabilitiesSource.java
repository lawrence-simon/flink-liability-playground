package coprocess;

import data.Liability;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LiabilitiesSource implements SourceFunction<Liability> {

    private final static Instant START_INSTANT = Instant.MIN;

    private volatile boolean running = true;

    private static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Override
    public void run(SourceContext<Liability> ctx) throws Exception {

        //Thread.sleep(10000);

        //SelectionId = 1
        addLiability(ctx, 0, 1, 100.0);
        addLiability(ctx, 1, 1, 200.0);
        addLiability(ctx, 2, 1, 300.0);
        addLiability(ctx, 3, 1, 400.0);

        //SelectionId = 2
        addLiability(ctx, 4, 2, 100.0);
        addLiability(ctx, 5, 2, 150.0);
        addLiability(ctx, 6, 2, 100.0);
        addLiability(ctx, 7, 2, 125.0);

        //SelectionId = 3
        addLiability(ctx, 8, 3, 100.0);
        addLiability(ctx, 9, 3, 101.0);
        addLiability(ctx, 18, 3, 102.0);
        addLiability(ctx, 19, 3, 103.0);

        while (running) {
            //Do nothing
        }
    }

    private void addLiability(SourceContext<Liability> ctx, int delay, int selectionId, double liability) {
        executor.schedule(() -> ctx.collect(new Liability(START_INSTANT.plus(Duration.ofMinutes(delay)), selectionId, liability)),
                delay,
                TimeUnit.SECONDS);
    }

    @Override
    public void cancel() {
        running = false;
    }
}
