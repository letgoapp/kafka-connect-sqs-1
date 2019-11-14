package com.nordstrom.kafka.connect.sqs;

import org.apache.kafka.common.errors.RetriableException;

public class SqsRetriableException extends RetriableException {
    public SqsRetriableException(Throwable cause) {
        super(cause);
    }
}
