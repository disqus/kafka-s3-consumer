package kafka.s3.consumer;

import java.util.Date;

public class PartitionKey {
    private String path;

    public PartitionKey (String path) {
        setPath(path);
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        this.path = path;
    }

    public String toString() {
        return "[path=" + path + "]";
    }

    public boolean equals(Object other) {
        if (this == other) return true;

        if (other == null || (this.getClass() != other.getClass())) {
            return false;
        }

        return this.hashCode() == other.hashCode();
    }

    public int hashCode() {
        return this.toString().hashCode();
    }

    public int compareTo(PartitionKey p) {
        return this.hashCode() - p.hashCode();
    }
}
