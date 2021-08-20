package com.nordstrom.kafka.connect.sqs;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class PendingReceipts {
    private static final int MAX_BATCH_SIZE = 10;

    private Map<String, List<PendingReceipt>> pendingReceiptsByQueue = new HashMap<>();

    public boolean isEmpty() {
        return size() == 0;
    }

    public int size() {
        return pendingReceiptsByQueue.values()
                .stream()
                .map(List::size)
                .reduce(0, Integer::sum);
    }

    public void add(String queueUrl, PendingReceipt receipt) {
        pendingReceiptsByQueue
                .computeIfAbsent(queueUrl, q -> new LinkedList<>())
                .add(receipt);
    }

    /**
     * Remove from the pile of pending receipts grouped in batches.
     *
     * @param maxAge If a receipt is younger than this age it is not removed
     * @return As few batches as possible
     */
    public List<DeleteBatch> removeInBatches(Duration maxAge) {
        LocalDateTime threshold = LocalDateTime.now().minus(maxAge);
        List<DeleteBatch> batches = pendingReceiptsByQueue.entrySet()
                .stream()
                .flatMap(entry -> {
                    List<PendingReceipt> entryReceipts = entry.getValue().stream()
                            .filter(receipt -> receipt.getCreatedAt().isAfter(threshold) ||
                                    receipt.getCreatedAt().isEqual(threshold))
                            .collect(Collectors.toList());
                    return partitionInBatches(entryReceipts).map(batchReceipts -> new DeleteBatch(entry.getKey(), batchReceipts));
                })
                .collect(Collectors.toList());

        Map<String, List<PendingReceipt>> previousPendingReceiptsByQueue = pendingReceiptsByQueue;
        pendingReceiptsByQueue = new HashMap<>();
        for (Map.Entry<String, List<PendingReceipt>> entry : previousPendingReceiptsByQueue.entrySet()) {
            for (PendingReceipt receipt : entry.getValue()) {
                if (receipt.getCreatedAt().isBefore(threshold)) {
                    add(entry.getKey(), receipt);
                }
            }
        }

        return batches;
    }

    private Stream<List<PendingReceipt>> partitionInBatches(List<PendingReceipt> list) {
        return IntStream.range(0, list.size() / PendingReceipts.MAX_BATCH_SIZE)
                .boxed()
                .map(i -> list.subList(i * PendingReceipts.MAX_BATCH_SIZE, (i + 1) * PendingReceipts.MAX_BATCH_SIZE));
    }
}
