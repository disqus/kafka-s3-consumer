package kafka.s3.consumer;

import java.util.Date;

public class PartitionKey {
    private Date date;
    private String extraPath;

    public PartitionKey (Date date, String extraPath) {
        setDate(date);
        setExtraPath(extraPath);
    }

    public Date getDate() {
        return date;
    }

    public String getExtraPath() {
        return extraPath;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public void setExtraPath(String extraPath) {
        if (extraPath.startsWith("/")) {
            extraPath = extraPath.substring(1);
        }

        if (extraPath.endsWith("/")) {
            extraPath = extraPath.substring(0, extraPath.length() - 1);
        }

        this.extraPath = extraPath;
    }

    public String toString() {
        return "[date=" + date + " extraPath=" + extraPath + "]";
    }

    public boolean equals(Object other) {
        if (this == other) return true;

        if (other == null || (this.getClass() != other.getClass())){
            return false;
        }

        return this.hashCode() == other.hashCode();
    }

    public int hashCode() {
        return String.format("%s %s", date, extraPath).hashCode();
    }

    public int compareTo(PartitionKey p) {
        return this.hashCode() - p.hashCode();
    }
}
