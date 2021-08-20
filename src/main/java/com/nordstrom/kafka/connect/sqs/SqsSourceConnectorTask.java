/*
 * Copyright 2019 Nordstrom, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.nordstrom.kafka.connect.sqs;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.model.Message;
import com.nordstrom.kafka.connect.About;

public class SqsSourceConnectorTask extends SourceTask {
    private static final Duration MAX_AGE = Duration.ofSeconds(20); // TODO: consider making this configurable

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private SqsClient client;
    private SqsSourceConnectorConfig config;
    private PendingReceipts pendingReceipts;
    private int remainingRetries;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.kafka.connect.connector.Task#version()
     */
    @Override
    public String version() {
        return About.CURRENT_VERSION;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.kafka.connect.source.SourceTask#start(java.util.Map)
     */
    @Override
    public void start(Map<String, String> props) {
        log.info("task.start");
        Guard.verifyNotNull(props, "Task properties");

        config = new SqsSourceConnectorConfig(props);
        client = new SqsClient(config.originalsWithPrefix(SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_CONFIG_PREFIX.getValue()));
        pendingReceipts = new PendingReceipts();
        remainingRetries = config.getMaxRetries();

        log.info("task.start.OK, sqs.queue.url={}, topics={}", config.getQueueUrl(), config.getTopics());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.kafka.connect.source.SourceTask#poll()
     */
    @Override
    public List<SourceRecord> poll() {
        log.debug(".poll:valid-state={}", isValidState());

        if (!isValidState()) {
            throw new IllegalStateException("Task is not properly initialized");
        }

        deleteReceipts(MAX_AGE);

        List<Message> messages;

        try {
            // Read messages from the queue.
            messages = client.receive(config.getQueueUrl(), config.getMaxMessages(),
                    config.getWaitTimeSeconds());
            log.debug(".poll:url={}, max={}, wait={}, size={}", config.getQueueUrl(), config.getMaxMessages(),
                    config.getWaitTimeSeconds(), messages.size());
        } catch (final RuntimeException e) {
            if (remainingRetries > 0) {
                log.warn("Receiving messages failed from url {}, remaining retries: {} (reason: {}),",
                        config.getQueueUrl(), remainingRetries, e.getMessage());
                remainingRetries--;
                throw new SqsRetriableException(e);
            } else {
                log.error("An Exception occurred while receiving messages from url {} with timeout {} after {} retries:",
                        config.getQueueUrl(), config.getWaitTimeSeconds(), config.getMaxRetries(), e);
                throw e;
            }
        }

        remainingRetries = config.getMaxRetries();
        // Create a SourceRecord for each message in the queue.
        return messages.stream().map(message -> {

            Map<String, String> sourcePartition = Collections.singletonMap(SqsConnectorConfigKeys.SQS_QUEUE_URL.getValue(),
                    config.getQueueUrl());
            Map<String, String> sourceOffset = new HashMap<>();
            // Save the message id and receipt-handle. receipt-handle is needed to delete
            // the message once the record is committed.
            sourceOffset.put(SqsConnectorConfigKeys.SQS_MESSAGE_ID.getValue(), message.getMessageId());
            sourceOffset.put(SqsConnectorConfigKeys.SQS_MESSAGE_RECEIPT_HANDLE.getValue(), message.getReceiptHandle());
            log.trace(".poll:source-partition={}", sourcePartition);
            log.trace(".poll:source-offset={}", sourceOffset);

            final String body = message.getBody();
            final String key = message.getMessageId();
            final String topic = config.getTopics();
            return new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, key, Schema.STRING_SCHEMA,
                    body);
        }).collect(Collectors.toList());
    }

    /* (non-Javadoc)
     * @see org.apache.kafka.connect.source.SourceTask#commitRecord(org.apache.kafka.connect.source.SourceRecord, org.apache.kafka.clients.producer.RecordMetadata)
     */
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
        Guard.verifyNotNull(record, "record");
        final String receipt = record.sourceOffset().get(SqsConnectorConfigKeys.SQS_MESSAGE_RECEIPT_HANDLE.getValue())
                .toString();
        String queueUrl = config.getQueueUrl();
        log.debug(".commit-record:url={}, receipt-handle={}", queueUrl, receipt);

        pendingReceipts.add(queueUrl, new PendingReceipt(receipt));
        deleteReceipts(MAX_AGE);
        super.commitRecord(record, metadata);
    }

    private void deleteReceipts(Duration maxAge) {
        List<DeleteBatch> batches = pendingReceipts.removeInBatches(maxAge);
        for (DeleteBatch batch : batches) {
            List<PendingReceipt> failedReceipts = client.batchDelete(batch.getQueueUrl(), batch.getReceipts());
            // FIXME: this will retry forever, even expired handles
//            for (PendingReceipt failedReceipt: failedReceipts) {
//                pendingReceipts.add(batch.getQueueUrl(), failedReceipt);
//            }
        }
    }

    private void deleteAllReceipts() {
        // TODO: should we wait before retrying? should we retry at all?
//        while(!pendingReceipts.isEmpty()) {
            deleteReceipts(Duration.ZERO);
//        }
    }


    /*
     * (non-Javadoc)
     *
     * @see org.apache.kafka.connect.source.SourceTask#stop()
     */
    @Override
    public void stop() {
        deleteAllReceipts();
        log.info("task.stop:OK");
    }

    /**
     * Test that we have both the task configuration and SQS client properly
     * initialized.
     *
     * @return true if task is in a valid state.
     */
    private boolean isValidState() {
        return null != config && null != client;
    }

}
