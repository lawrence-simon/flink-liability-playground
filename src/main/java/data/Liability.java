package data;

import java.time.Instant;

public class Liability {

    private Instant eventTime;
    private long selectionId;
    private double liability;

    public Liability() {
    }

    public Liability(Instant eventTime, long selectionId, double liability) {
        this.eventTime = eventTime;
        this.selectionId = selectionId;
        this.liability = liability;
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

    public double getLiability() {
        return liability;
    }

    public void setLiability(double liability) {
        this.liability = liability;
    }
}
