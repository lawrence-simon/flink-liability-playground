package data;

import java.time.Instant;

public class Setting {

    private Instant eventTime;
    private long selectionId;
    private double liabilityLimit;

    public Setting() {
    }

    public Setting(Instant eventTime, long selectionId, double liabilityLimit) {
        this.eventTime = eventTime;
        this.selectionId = selectionId;
        this.liabilityLimit = liabilityLimit;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    public long getSelectionId() {
        return selectionId;
    }

    public void setSelectionId(long selectionId) {
        this.selectionId = selectionId;
    }

    public double getLiabilityLimit() {
        return liabilityLimit;
    }

    public void setLiabilityLimit(double liabilityLimit) {
        this.liabilityLimit = liabilityLimit;
    }
}
