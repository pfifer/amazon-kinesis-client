/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package software.amazon.kinesis.checkpoint;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import software.amazon.kinesis.coordinator.RecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.retrieval.IKinesisProxy;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import lombok.Data;

/**
 * A class encapsulating the 2 pieces of state stored in a checkpoint.
 */
@Data public class Checkpoint {

    private final ExtendedSequenceNumber checkpoint;
    private final ExtendedSequenceNumber pendingCheckpoint;

    /**
     * Constructor.
     *
     * @param checkpoint the checkpoint sequence number - cannot be null or empty.
     * @param pendingCheckpoint the pending checkpoint sequence number - can be null.
     */
    public Checkpoint(ExtendedSequenceNumber checkpoint, ExtendedSequenceNumber pendingCheckpoint) {
        if (checkpoint == null || checkpoint.getSequenceNumber().isEmpty()) {
            throw new IllegalArgumentException("Checkpoint cannot be null or empty");
        }
        this.checkpoint = checkpoint;
        this.pendingCheckpoint = pendingCheckpoint;
    }

    /**
     * This class provides some methods for validating sequence numbers. It provides a method
     * {@link #validateSequenceNumber(String)} which validates a sequence number by attempting to get an iterator from
     * Amazon Kinesis for that sequence number. (e.g. Before checkpointing a client provided sequence number in
     * {@link RecordProcessorCheckpointer#checkpoint(String)} to prevent invalid sequence numbers from being checkpointed,
     * which could prevent another shard consumer instance from processing the shard later on). This class also provides a
     * utility function {@link #isDigits(String)} which is used to check whether a string is all digits
     */
    @Slf4j
    public static class SequenceNumberValidator {
        private IKinesisProxy proxy;
        private String shardId;
        private boolean validateWithGetIterator;
        private static final int SERVER_SIDE_ERROR_CODE = 500;

        /**
         * Constructor.
         *
         * @param proxy Kinesis proxy to be used for getIterator call
         * @param shardId ShardId to check with sequence numbers
         * @param validateWithGetIterator Whether to attempt to get an iterator for this shard id and the sequence numbers
         *        being validated
         */
        public SequenceNumberValidator(IKinesisProxy proxy, String shardId, boolean validateWithGetIterator) {
            this.proxy = proxy;
            this.shardId = shardId;
            this.validateWithGetIterator = validateWithGetIterator;
        }

        /**
         * Validates the sequence number by attempting to get an iterator from Amazon Kinesis. Repackages exceptions from
         * Amazon Kinesis into the appropriate KCL exception to allow clients to determine exception handling strategies
         *
         * @param sequenceNumber The sequence number to be validated. Must be a numeric string
         * @throws IllegalArgumentException Thrown when sequence number validation fails.
         * @throws ThrottlingException Thrown when GetShardIterator returns a ProvisionedThroughputExceededException which
         *         indicates that too many getIterator calls are being made for this shard.
         * @throws KinesisClientLibDependencyException Thrown when a service side error is received. This way clients have
         *         the option of retrying
         */
        public void validateSequenceNumber(String sequenceNumber)
            throws IllegalArgumentException, ThrottlingException, KinesisClientLibDependencyException {
            boolean atShardEnd = ExtendedSequenceNumber.SHARD_END.getSequenceNumber().equals(sequenceNumber);

            if (!atShardEnd && !isDigits(sequenceNumber)) {
                SequenceNumberValidator.log.info("Sequence number must be numeric, but was {}", sequenceNumber);
                throw new IllegalArgumentException("Sequence number must be numeric, but was " + sequenceNumber);
            }
            try {
                if (!atShardEnd &&validateWithGetIterator) {
                    proxy.getIterator(shardId, ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), sequenceNumber);
                    SequenceNumberValidator.log.info("Validated sequence number {} with shard id {}", sequenceNumber, shardId);
                }
            } catch (InvalidArgumentException e) {
                SequenceNumberValidator.log.info("Sequence number {} is invalid for shard {}", sequenceNumber, shardId, e);
                throw new IllegalArgumentException("Sequence number " + sequenceNumber + " is invalid for shard "
                        + shardId, e);
            } catch (ProvisionedThroughputExceededException e) {
                // clients should have back off logic in their checkpoint logic
                SequenceNumberValidator.log.info("Exceeded throughput while getting an iterator for shard {}", shardId, e);
                throw new ThrottlingException("Exceeded throughput while getting an iterator for shard " + shardId, e);
            } catch (AmazonServiceException e) {
                SequenceNumberValidator.log.info("Encountered service exception while getting an iterator for shard {}", shardId, e);
                if (e.getStatusCode() >= SERVER_SIDE_ERROR_CODE) {
                    // clients can choose whether to retry in their checkpoint logic
                    throw new KinesisClientLibDependencyException("Encountered service exception while getting an iterator"
                            + " for shard " + shardId, e);
                }
                // Just throw any other exceptions, e.g. 400 errors caused by the client
                throw e;
            }
        }

        void validateSequenceNumber(ExtendedSequenceNumber checkpoint)
            throws IllegalArgumentException, ThrottlingException, KinesisClientLibDependencyException {
            validateSequenceNumber(checkpoint.getSequenceNumber());
            if (checkpoint.getSubSequenceNumber() < 0) {
                throw new IllegalArgumentException("SubSequence number must be non-negative, but was "
                        + checkpoint.getSubSequenceNumber());
            }
        }

        /**
         * Checks if the string is composed of only digits.
         *
         * @param string
         * @return true for a string of all digits, false otherwise (including false for null and empty string)
         */
        public static boolean isDigits(String string) {
            if (string == null || string.length() == 0) {
                return false;
            }
            for (int i = 0; i < string.length(); i++) {
                if (!Character.isDigit(string.charAt(i))) {
                    return false;
                }
            }
            return true;
        }
    }
}
