package com.nordstrom.kafka.connect.sqs;

import java.util.List;
import java.util.Objects;

public final class DeleteBatch {
    private final String queueUrl;
    private final List<PendingReceipt> receipts;

    public DeleteBatch(String queueUrl, List<PendingReceipt> receipts) {
        this.queueUrl = queueUrl;
        this.receipts = receipts;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public List<PendingReceipt> getReceipts() {
        return receipts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteBatch that = (DeleteBatch) o;
        return queueUrl.equals(that.queueUrl) && receipts.equals(that.receipts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queueUrl, receipts);
    }
}
