package broadcast;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.time.LocalDateTime;

class SimpleStringSource extends RichSourceFunction<String> {

    private boolean go = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (go) {
            Thread.sleep(5000);
            sourceContext.collect(LocalDateTime.now().toString());
        }
    }

    @Override
    public void cancel() {
        go = false;
    }
}
