package data;

import java.time.Instant;

public class Breach {

    private Instant eventTime;
    private long selectionId;
    private double liabilityAtBreach;
    private double limitAtBreach;

    public Breach(Instant eventTime, long selectionId, double liabilityAtBreach, double limitAtBreach) {
        this.eventTime = eventTime;
        this.selectionId = selectionId;
        this.liabilityAtBreach = liabilityAtBreach;
        this.limitAtBreach = limitAtBreach;
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

    public double getLiabilityAtBreach() {
        return liabilityAtBreach;
    }

    public void setLiabilityAtBreach(double liabilityAtBreach) {
        this.liabilityAtBreach = liabilityAtBreach;
    }

    public double getLimitAtBreach() {
        return limitAtBreach;
    }

    public void setLimitAtBreach(double limitAtBreach) {
        this.limitAtBreach = limitAtBreach;
    }

    @Override
    public String toString() {
        return "Breach{" +
                "timestamp=" + eventTime +
                ", selectionId=" + selectionId +
                ", liabilityAtBreach=" + liabilityAtBreach +
                ", limitAtBreach=" + limitAtBreach +
                '}';
    }
}
