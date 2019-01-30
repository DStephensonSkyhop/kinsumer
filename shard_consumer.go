// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

const (
	// getRecordsLimit is the max number of records in a single request. This effectively limits the
	// total processing speed to getRecordsLimit*5/n where n is the number of parallel clients trying
	// to consume from the same kinesis stream
	getRecordsLimit = 10000 // 10,000 is the max according to the docs

	// maxErrorRetries is how many times we will retry on a shard error
	maxErrorRetries = 3

	// errorSleepDuration is how long we sleep when an error happens, this is multiplied by the number
	// of retries to give a minor backoff behavior
	errorSleepDuration = 1 * time.Second
)

// getShardIterator gets a shard iterator after the last sequence number we read or at the start of the stream
func getShardIterator(k kinesisiface.KinesisAPI, streamName string, shardID string, sequenceNumber string, iteratorType string) (string, error) {
	shardIteratorType := kinesis.ShardIteratorTypeAfterSequenceNumber

	// If we do not have a sequenceNumber yet we need to get a shardIterator
	// from the horizon
	ps := aws.String(sequenceNumber)
	if sequenceNumber == "" || iteratorType == kinesis.ShardIteratorTypeLatest {
		fmt.Printf("No Sequence Number, starting from latest, Shard ID: %v\n", shardID)
		shardIteratorType = iteratorType
		ps = nil
	}

	resp, err := k.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:                aws.String(shardID),
		ShardIteratorType:      &shardIteratorType,
		StartingSequenceNumber: ps,
		StreamName:             aws.String(streamName),
	})
	return aws.StringValue(resp.ShardIterator), err
}

// getRecords returns the next records and shard iterator from the given shard iterator
func getRecords(k kinesisiface.KinesisAPI, iterator string) (records []*kinesis.Record, nextIterator string, lag time.Duration, err error) {
	params := &kinesis.GetRecordsInput{
		Limit:         aws.Int64(getRecordsLimit),
		ShardIterator: aws.String(iterator),
	}

	output, err := k.GetRecords(params)

	if err != nil {
		return nil, "", 0, err
	}

	records = output.Records
	nextIterator = aws.StringValue(output.NextShardIterator)
	lag = time.Duration(aws.Int64Value(output.MillisBehindLatest)) * time.Millisecond

	return records, nextIterator, lag, nil
}

// captureShard blocks until we capture the given shardID
func (k *Kinsumer) captureShard(shardID string) (*Checkpointer, error) {
	// Attempt to capture the shard in dynamo
	for {
		// Ask the checkpointer to capture the shard
		checkpointer, err := Capture(
			shardID,
			k.checkpointTableName,
			k.dynamodb,
			k.clientName,
			k.clientID,
			k.maxAgeForClientRecord,
			k.config.stats,
			k.logger)
		if err != nil {
			return nil, err
		}

		if checkpointer != nil {
			return checkpointer, nil
		}

		// Throttle requests so that we don't hammer dynamo
		select {
		case <-k.stop:
			// If we are told to stop consuming we should stop attempting to capture
			return nil, nil
		case <-time.After(k.config.throttleDelay):
		}
	}
}

// consume is a blocking call that captures then consumes the given shard in a loop.
// It is also responsible for writing out the checkpoint updates to dynamo.
// TODO: There are no tests for this file. Not sure how to even unit test this.
func (k *Kinsumer) consume(shardID string) {
	defer k.waitGroup.Done()

	// capture the checkpointer
	checkpointer, err := k.captureShard(shardID)
	if err != nil {
		k.shardErrors <- ShardConsumerError{ShardID: shardID, Action: "captureShard", Error: err, Level: FatalLevel}
		return
	}

	// if we failed to capture the checkpointer but there was no errors
	// we must have stopped, so don't process this shard at all
	if checkpointer == nil {
		return
	}

	sequenceNumber := checkpointer.GetSequenceNumber()

	// finished means we have reached the end of the shard but haven't necessarily processed/committed everything
	finished := false
	/*
		// Make sure we release the shard when we are done.
		defer func() {
			innerErr := checkpointer.Release()
			if innerErr != nil {
				k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.Release", err: innerErr}
				return
			}
		}()
	*/

	// Get the starting shard iterator
	iterator, err := getShardIterator(k.kinesis, k.streamName, shardID, sequenceNumber, k.config.shardIteratorType)
	if err != nil {
		k.shardErrors <- ShardConsumerError{ShardID: shardID, Action: "getShardIterator", Error: err, Level: FatalLevel}
		return
	}
	k.logger.Debugf("getShardIterator, SequenceNumber: %v, Shard ID: %v\n", sequenceNumber, shardID)

	// no throttle on the first request.
	nextThrottle := time.After(0)

	retryCount := 0

	var lastSeqNum string
mainloop:
	for {
		// We have reached the end of the shard's data. Set Finished in dynamo and stop processing.
		if iterator == "" && !finished {
			checkpointer.Finish(lastSeqNum)
			finished = true
			return
		}

		// Handle async actions, and throttle requests to keep kinesis happy
		select {
		case <-k.stop:
			return
		case <-nextThrottle:
		}

		// Reset the nextThrottle
		nextThrottle = time.After(k.config.throttleDelay)

		if finished {
			continue mainloop
		}

		// Get records from kinesis
		records, next, lag, err := getRecords(k.kinesis, iterator)

		if err != nil {
			retryCount++
			k.logger.Debugf("Failed to get records... retrying (%v/%v)", retryCount, k.config.shardRetryLimit)

			if awsErr, ok := err.(awserr.Error); ok {
				k.logger.Debugf("Failed to get records, AWS error: %v, %v", awsErr.Message(), awsErr.OrigErr())

				// Critical AWS error
				if strings.Contains(awsErr.Message(), "Signature expired") == true {
					k.shardErrors <- ShardConsumerError{ShardID: shardID, Action: "getRecords", Error: errors.New("Signature Expired"), Level: FatalLevel}
					return
				}
			}

			// Close shard if maximum number of retries is exceeded.
			// If we can't get records from this shard, we will close the entire app, instead of this single shard
			// because the checkpointer would need to be released, and the responsibility for that was moved to the application.
			if retryCount >= k.config.shardRetryLimit {
				k.shardErrors <- ShardConsumerError{ShardID: shardID, Action: "getRecords",
					Error: fmt.Errorf("Failed to get records, retry limit exceeded (%v/%v)", retryCount, k.config.shardRetryLimit),
					Level: FatalLevel,
				}
				return
			}

			// casting retryCount here to time.Duration purely for the multiplication, there is
			// no meaning to retryCount nanoseconds
			time.Sleep(errorSleepDuration * time.Duration(retryCount))
			continue mainloop
		}
		retryCount = 0

		// Put all the records we got onto the channel
		k.config.stats.EventsFromKinesis(len(records), shardID, lag)
		if len(records) > 0 {
			retrievedAt := time.Now()
			for _, record := range records {
			RecordLoop:
				// Loop until we stop or the record is consumed, checkpointing if necessary.
				for {
					select {
					case <-k.stop:
						return
					case k.records <- &ConsumedRecord{
						Record:       record,
						Checkpointer: checkpointer,
						retrievedAt:  retrievedAt,
					}:
						break RecordLoop
					}
				}
			}

			// Update the last sequence number we saw, in case we reached the end of the stream.
			lastSeqNum = aws.StringValue(records[len(records)-1].SequenceNumber)
		}
		iterator = next
	}
}
