// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/pkg/errors"
)

const (
	// getRecordsLimit is the max number of records in a single request. This effectively limits the
	// total processing speed to getRecordsLimit*5/n where n is the number of parallel clients trying
	// to consume from the same kinesis stream
	getRecordsLimit = 10000 // 10,000 is the max according to the docs

	// maxErrorRetries is how many times we will retry on a shard error
	//maxErrorRetries = 3

	// errorSleepDuration is how long we sleep when an error happens, this is multiplied by the number
	// of retries to give a minor backoff behavior
	errorSleepDuration = 1 * time.Second
)

// getShardIterator gets a shard iterator after the last sequence number we read or at the start of the stream
func (k *Kinsumer) getShardIterator(streamName string, shardID string, sequenceNumber string, iteratorType string) (string, error) {
	// Use "AFTER_SEQUENCE_NUMBER" because we already processed the record at
	// the given sequenceNumber
	shardIteratorType := kinesis.ShardIteratorTypeAfterSequenceNumber

	// If we do not have a sequenceNumber yet we need to get a shardIterator
	// from the horizon
	ps := aws.String(sequenceNumber)
	if sequenceNumber == "" {
		k.logger.Debugf("No Sequence Number, starting from %s, Shard ID: %s", iteratorType, shardID)
		shardIteratorType = iteratorType
		ps = nil
	}

	resp, err := k.kinesis.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:                aws.String(shardID),
		ShardIteratorType:      &shardIteratorType,
		StartingSequenceNumber: ps,
		StreamName:             aws.String(streamName),
	})
	return aws.StringValue(resp.ShardIterator), err
}

// subscribeToShard gets an event stream after the last sequence number we read or at the start of the stream
// When the SubscribeToShard call succeeds, your consumer starts receiving events of type
// SubscribeToShardEvent over the HTTP/2 connection for up to 5 minutes, after which time
// you need to call SubscribeToShard again to renew the subscription if you want to
// continue to receive records.
func (k *Kinsumer) subscribeToShard(consumerARN *string, shardID string, sequenceNumber string, iteratorType string) (*kinesis.SubscribeToShardOutput, error) {
	// Use "AFTER_SEQUENCE_NUMBER" because we already processed the record at
	// the given sequenceNumber
	shardIteratorType := kinesis.ShardIteratorTypeAfterSequenceNumber

	// If we do not have a sequenceNumber yet we need to get a shardIterator
	// from the horizon
	ps := aws.String(sequenceNumber)
	if sequenceNumber == "" {
		k.logger.Debugf("No Sequence Number, starting from %s, Shard ID: %s", iteratorType, shardID)
		shardIteratorType = iteratorType
		ps = nil
	}

	// Subscribe to shard
	out, err := k.kinesis.SubscribeToShard(&kinesis.SubscribeToShardInput{
		ConsumerARN: consumerARN,
		ShardId:     aws.String(shardID),
		StartingPosition: &kinesis.StartingPosition{
			SequenceNumber: ps,
			Type:           &shardIteratorType,
		},
	})
	return out, err
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
		k.shardErrors <- ShardConsumerSignal{ShardID: shardID, Action: "captureShard", Error: err, Level: FatalLevel}
		return
	}

	// if we failed to capture the checkpointer but there was no errors
	// we must have stopped, so don't process this shard at all
	if checkpointer == nil {
		return
	}

	sequenceNumber := checkpointer.GetSequenceNumber()

	// Get the starting shard iterator
	iterator, err := k.getShardIterator(k.streamName, shardID, sequenceNumber, k.config.shardIteratorType)
	if err != nil {
		k.shardErrors <- ShardConsumerSignal{ShardID: shardID, Action: "getShardIterator", Error: err, Level: FatalLevel}
		return
	}
	k.logger.Debugf("getShardIterator, SequenceNumber: %s, Shard ID: %s", sequenceNumber, shardID)

	// no throttle on the first request.
	nextThrottle := time.After(0)

	retryCount := 0
	var lastSeqNum string

mainloop:
	for {
		// Handle async actions, and throttle requests to keep kinesis happy
		select {
		case <-k.stop:
			return
		case <-nextThrottle:
		}

		// Reset the nextThrottle
		nextThrottle = time.After(k.config.throttleDelay)

		// Get records from kinesis
		records, next, lag, err := getRecords(k.kinesis, iterator)
		if err != nil {
			retryCount++
			k.logger.Debugf("Failed to get records... retrying (%v/%v)", retryCount, k.config.shardRetryLimit)
			if awsErr, ok := err.(awserr.Error); ok {
				k.logger.Debugf("Failed to get records, AWS error: %v, %v", awsErr.Message(), awsErr.OrigErr())

				// Critical AWS error
				if strings.Contains(awsErr.Message(), "Signature expired") {
					k.shardErrors <- ShardConsumerSignal{ShardID: shardID, Action: "getRecords", Error: errors.New("signature expired"), Level: FatalLevel}
					return
				}
			}
			// Close shard if maximum number of retries is exceeded.
			// If we can't get records from this shard, we will close the entire app, instead of this single shard
			// because the checkpointer would need to be released, and the responsibility for that was moved to the application.
			if retryCount >= k.config.shardRetryLimit {
				k.shardErrors <- ShardConsumerSignal{ShardID: shardID, Action: "getRecords",
					Error: fmt.Errorf("failed to get records, retry limit exceeded (%v/%v)", retryCount, k.config.shardRetryLimit),
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

		// If the shard has been closed, the shard iterator can't return more data and GetRecords
		// returns null in NextShardIterator.
		if next == "" {
			if len(records) > 0 {
				// This should not happen, if it does then the AWS docs on how this works
				// is misleading and we need to move this logic AFTER the RecordLoop: which
				// will open us up to the possibility that the app will checkpoint before
				// we mark a checkpointer as almost finished.
				k.shardErrors <- ShardConsumerSignal{ShardID: shardID, Action: "getRecords", Error: errors.Errorf("records found in an event with a nil NextShardIterator!"), Level: FatalLevel}
				return
			}
			k.endOfShard(checkpointer, lastSeqNum)
			return
		}

		iterator = next

		// Put all the records we got onto the channel
		k.config.stats.EventsFromKinesis(len(records), shardID, lag)
		if len(records) > 0 {
			retrievedAt := time.Now()
			for _, record := range records {
			RecordLoop:
				// Loop until we stop or the record is consumed
				// We pass the checkpointer with the record
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
	}
}

// consumeWithFanOut is a blocking call that captures then consumes the given shard in a loop.
// It is also responsible for writing out the checkpoint updates to dynamo.
// TODO: There are no tests for this file. Not sure how to even unit test this.
func (k *Kinsumer) consumeWithFanOut(shardID string, registered *kinesis.RegisterStreamConsumerOutput) {
	defer k.waitGroup.Done()

	// capture the checkpointer
	checkpointer, err := k.captureShard(shardID)
	if err != nil {
		k.shardErrors <- ShardConsumerSignal{ShardID: shardID, Action: "captureShard", Error: err, Level: FatalLevel}
		return
	}

	// if we failed to capture the checkpointer but there was no errors
	// we must have stopped, so don't process this shard at all
	if checkpointer == nil {
		return
	}

	sequenceNumber := checkpointer.GetSequenceNumber()

	// Subscribe to the shard using enhanced fan-out
	out, err := k.subscribeToShard(registered.Consumer.ConsumerARN, shardID, sequenceNumber, k.config.shardIteratorType)
	if err != nil {
		k.shardErrors <- ShardConsumerSignal{ShardID: shardID, Action: "subscribeToShard", Error: err, Level: FatalLevel}
		return
	}
	k.logger.Debugf("subscribeToShard, SequenceNumber: %v, Shard ID: %v", sequenceNumber, shardID)

	// A shard iterator expires 5 minutes after it is returned to the requester.
	// You need to call SubscribeToShard again to renew the subscription if you
	// want to continue to receive records. If you call SubscribeToShard with
	// the same ConsumerARN and ShardId within 5 seconds of a successful call,
	// you'll get a ResourceInUseException.
	subscribeInterval := 4 * time.Minute

	nextSubscribe := time.After(subscribeInterval)
	var lastSeqNum string

	for {
		var evt kinesis.SubscribeToShardEventStreamEvent
		select {
		case <-k.stop:
			return
		case <-nextSubscribe:
			k.logger.Debugf("re-subscribeToShard Shard %s at continuation sequence number %s", shardID, sequenceNumber)
			var err error
			out, err = k.subscribeToShard(registered.Consumer.ConsumerARN, shardID, sequenceNumber, k.config.shardIteratorType)
			if err != nil {
				k.shardErrors <- ShardConsumerSignal{ShardID: shardID, Action: "subscribeToShard", Error: err, Level: FatalLevel}
				return
			}
			nextSubscribe = time.After(subscribeInterval)
			continue
		// Receive records from kinesis
		case evt = <-out.GetEventStream().Events():
		}

		// A nil event does NOT necessarily mean we finished processing this shard.
		// This sometimes happens in rapid succession.
		if evt == nil {
			k.logger.Tracef("kinesis pushed a nil record to the kinsumer from shard %s", shardID)
			continue
		}

		event := evt.(*kinesis.SubscribeToShardEvent)

		// If event.ContinuationSequenceNumber returns null, it indicates that a shard
		// split or merge has occurred that involved this shard. This shard is now in a
		// CLOSED state, and you have read all available data records from this shard
		if event.ContinuationSequenceNumber == nil {
			if len(event.Records) > 0 {
				// This should not happen, if it does then the AWS docs on how this works
				// is misleading and we need to move this logic AFTER the RecordLoop: which
				// will open us up to the possibility that the app will checkpoint before
				// we mark a checkpointer as almost finished.
				k.shardErrors <- ShardConsumerSignal{ShardID: shardID, Action: "subscribeToShard", Error: errors.Errorf("records found in an event with a nil ContinuationSequenceNumber!"), Level: FatalLevel}
				return
			}
			k.endOfShard(checkpointer, lastSeqNum)
			return
		}

		// Use this as SequenceNumber in the next call to SubscribeToShard.
		// Use ContinuationSequenceNumber for checkpointing because it captures
		// your shard progress even when no data is written to the shard.
		sequenceNumber = *event.ContinuationSequenceNumber

		records := event.Records

		// Kinesis doesn't mind pushing us events with zero records
		if len(records) == 0 {
			k.logger.Tracef("No records in this push %s", *event.ContinuationSequenceNumber)
			continue
		}

		// Report the lag and number of events we got to our stats receiver
		lag := time.Duration(aws.Int64Value(event.MillisBehindLatest)) * time.Millisecond
		k.config.stats.EventsFromKinesis(len(records), shardID, lag)

		// Send all the records we got down the channel
		retrievedAt := time.Now()
		for _, record := range records {
		RecordLoop:
			// Loop until we stop or the record is consumed
			// We pass the checkpointer with the record
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
			// Update the last sequence number we saw, in case we reached the end of the stream.
			lastSeqNum = aws.StringValue(records[len(records)-1].SequenceNumber)
		}
	}
}

// endOfShard marks the shard's checkpointer as finsihed an releases it if it deems
// necessary. Start and end are values used to determine if we started at the end of a shard.
func (k *Kinsumer) endOfShard(checkpointer *Checkpointer, finalSeqNumber string) {
	shardID := checkpointer.GetShardID()
	k.logger.Debugf("End of shard detected. Marking this checkpointer for %s as Finished", shardID)
	// Mark the checkpointer as finished. The application is responsible
	// for proecessing the records associated with it, calling Commit() with
	// with the finalSeqNumber so that it gets marked as finished in dynamo,
	// checking if this checkpointer is finished and, if so, releasing it.
	checkpointer.Finish(finalSeqNumber)
	// This means that we have a checkpoint in the database that
	// starts at the end of a shard. We can end up in the situation
	// if the application fails to Finish a checkpointer.
	if finalSeqNumber == "" {
		// Releasing a checkpointer is the reponsibility of the application that is
		// using this library. If we have a checkpoint at the end of a shard that means
		// the app processed everything and checkpointed it but has not yet released it.
		// We should let the application handle closing
		// this out but there is an edge case when refreshing shards that the app will
		// drop his reference to this checkpointer before he has the chance to release it.
		// In the off case that the app attempts to checkpoint after we released it the
		// checkpoint attempt will be ignored because we dont care to checkpoint a
		// checkpointer that is already completed. Same goes if the app tries to release it.
		k.logger.Warnf("Looks like this checkpointer that was left open for shard %s. Releasing it now", shardID)
		checkpointer.CommitWithSequenceNumber(finalSeqNumber)
		checkpointer.Release()
	}
}
