package com.nordstrom.kafka.connect.sqs;

import java.time.LocalDateTime;
import java.util.Objects;

public final class PendingReceipt {
    private final String handle;
    private final LocalDateTime createdAt;

    public PendingReceipt(String handle) {
        this(handle, LocalDateTime.now());
    }

    public PendingReceipt(String handle, LocalDateTime createdAt) {
        this.handle = handle;
        this.createdAt = createdAt;
    }

    public String getHandle() {
        return handle;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PendingReceipt that = (PendingReceipt) o;
        return handle.equals(that.handle) && createdAt.equals(that.createdAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(handle, createdAt);
    }

    @Override
    public String toString() {
        return String.format("PendingReceipt{handle='%s', createdAt=%s}", handle, createdAt);
    }
}
