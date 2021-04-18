// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	statsd "github.com/ericksonjoseph/kinsumer/statsd"
)

// Note: Not thread safe!

type Checkpointer struct {
	shardID               string
	tableName             string
	dynamodb              dynamodbiface.DynamoDBAPI
	sequenceNumber        string
	ownerName             string
	ownerID               string
	maxAgeForClientRecord time.Duration
	stats                 statsd.StatReceiver
	captured              bool
	dirty                 bool
	mutex                 sync.Mutex
	finished              bool
	logger                loggerInterface
}

type CheckpointRecord struct {
	Shard          string
	SequenceNumber *string // last read sequence number, null if the shard has never been consumed
	LastUpdate     int64   // timestamp of last commit/ownership change
	OwnerName      *string // uuid of owning client, null if the shard is unowned
	Finished       *int64  // timestamp of when the shard was fully consumed, null if it's active

	// Columns added to the table that are never used for decision making in the
	// library, rather they are useful for manual troubleshooting
	OwnerID       *string
	LastUpdateRFC string
	FinishedRFC   *string
}

// capture is a non-blocking call that attempts to capture the given shard/checkpoint.
// It returns a checkpointer on success, or nil if it fails to capture the checkpoint
func Capture(
	shardID string,
	tableName string,
	dynamodbiface dynamodbiface.DynamoDBAPI,
	ownerName string,
	ownerID string,
	maxAgeForClientRecord time.Duration,
	stats statsd.StatReceiver,
	log loggerInterface) (*Checkpointer, error) {

	cutoff := time.Now().Add(-maxAgeForClientRecord).UnixNano()

	// Grab the entry from dynamo assuming there is one
	resp, err := dynamodbiface.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"Shard": {S: aws.String(shardID)},
		},
	})

	if err != nil {
		return nil, fmt.Errorf("error calling GetItem on shard checkpoint: %v", err)
	}

	// Convert to struct so we can work with the values
	var record CheckpointRecord
	if err = dynamodbattribute.UnmarshalMap(resp.Item, &record); err != nil {
		return nil, err
	}

	// If the record is marked as owned by someone else, and has not expired
	if record.OwnerID != nil && record.LastUpdate > cutoff {
		// We fail to capture it
		return nil, nil
	}

	// Make sure the Shard is set in case there was no record
	record.Shard = shardID

	// Mark us as the owners
	record.OwnerID = &ownerID
	record.OwnerName = &ownerName

	// Update timestamp
	now := time.Now()
	record.LastUpdate = now.UnixNano()
	record.LastUpdateRFC = now.UTC().Format(time.RFC1123Z)

	item, err := dynamodbattribute.MarshalMap(record)
	if err != nil {
		return nil, err
	}

	attrVals, err := dynamodbattribute.MarshalMap(map[string]interface{}{
		":cutoff":   aws.Int64(cutoff),
		":nullType": aws.String("NULL"),
	})
	if err != nil {
		return nil, err
	}
	if _, err = dynamodbiface.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
		// The OwnerID doesn't exist if the entry doesn't exist, but PutItem with a marshaled
		// CheckpointRecord sets a nil OwnerID to the NULL type.
		ConditionExpression: aws.String(
			"attribute_not_exists(OwnerID) OR attribute_type(OwnerID, :nullType) OR LastUpdate <= :cutoff"),
		ExpressionAttributeValues: attrVals,
	}); err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "ConditionalCheckFailedException" {
			// We failed to capture it
			return nil, nil
		}
		return nil, err
	}

	checkpointer := &Checkpointer{
		shardID:               shardID,
		tableName:             tableName,
		dynamodb:              dynamodbiface,
		ownerName:             ownerName,
		ownerID:               ownerID,
		stats:                 stats,
		sequenceNumber:        aws.StringValue(record.SequenceNumber),
		maxAgeForClientRecord: maxAgeForClientRecord,
		captured:              true,
		logger:                log,
	}

	return checkpointer, nil
}

// Commit writes the latest SequenceNumber consumed to dynamo and updates LastUpdate.
// Returns true if we set Finished in dynamo because the library user finished consuming the shard.
// Once that has happened, the checkpointer should be released and never grabbed again.
func (cp *Checkpointer) Commit() (bool, error) {
	return cp.CommitWithSequenceNumber(cp.sequenceNumber)
}

// CommitWithSequenceNumber writes the provided SequenceNumber to dynamo and updates LastUpdate.
// Returns true if we set Finished in dynamo because the library user finished consuming the shard.
// Once that has happened, the checkpointer should be released and never grabbed again.
func (cp *Checkpointer) CommitWithSequenceNumber(sequenceNumber string) (bool, error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	if !cp.captured {
		// Another thread may have decided to release this checkpointer
		// because it realized it was at the end of the stream
		cp.logger.Debugf("Attempted to commit a checkpointer that has already been released: %s", cp.shardID)
		return true, nil
	}
	if !cp.dirty && !cp.finished {
		return false, nil
	}
	now := time.Now()

	sn := &sequenceNumber
	if sequenceNumber == "" {
		// We are not allowed to pass empty strings to dynamo, so instead pass a nil *string
		// to 'unset' it
		sn = nil
	}

	record := CheckpointRecord{
		Shard:          cp.shardID,
		SequenceNumber: sn,
		LastUpdate:     now.UnixNano(),
		LastUpdateRFC:  now.UTC().Format(time.RFC1123Z),
	}
	finished := false
	if cp.finished {
		record.Finished = aws.Int64(now.UnixNano())
		record.FinishedRFC = aws.String(now.UTC().Format(time.RFC1123Z))
		finished = true
	}
	record.OwnerID = &cp.ownerID
	record.OwnerName = &cp.ownerName

	item, err := dynamodbattribute.MarshalMap(&record)
	if err != nil {
		return false, err
	}

	attrVals, err := dynamodbattribute.MarshalMap(map[string]interface{}{
		":ownerID": aws.String(cp.ownerID),
	})
	if err != nil {
		return false, err
	}

	err = cp.dynamoTableActive()
	if err != nil {
		return false, fmt.Errorf("CommitWithSequenceNumber, Inactive Table: %s", err)
	}

	if _, err = cp.dynamodb.PutItem(&dynamodb.PutItemInput{
		TableName:                 aws.String(cp.tableName),
		Item:                      item,
		ConditionExpression:       aws.String("OwnerID = :ownerID"),
		ExpressionAttributeValues: attrVals,
	}); err != nil {
		return false, fmt.Errorf("error committing checkpoint: %s, shard ID: %v", err, cp.shardID)
	}

	if sn != nil {
		cp.stats.Checkpoint()
	}
	cp.dirty = false
	return finished, nil
}

// Release releases our ownership of the checkpoint in dynamo so another client can take it
func (cp *Checkpointer) Release() error {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	if !cp.captured {
		// Another thread may have decided to release this checkpointer
		// because it realized it was at the end of the stream
		cp.logger.Debugf("Attempted to release a checkpointer that has already been released: %s", cp.shardID)
		return nil
	}
	now := time.Now()

	cp.logger.Debugf("Attempting to release checkpointer for Shard with ID: %v", cp.shardID)
	attrVals, err := dynamodbattribute.MarshalMap(map[string]interface{}{
		":ownerID":        aws.String(cp.ownerID),
		":sequenceNumber": aws.String(cp.sequenceNumber),
		":lastUpdate":     aws.Int64(now.UnixNano()),
		":lastUpdateRFC":  aws.String(now.UTC().Format(time.RFC1123Z)),
	})
	if err != nil {
		return err
	}
	if _, err = cp.dynamodb.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String(cp.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Shard": {S: aws.String(cp.shardID)},
		},
		UpdateExpression: aws.String("REMOVE OwnerID, OwnerName " +
			"SET LastUpdate = :lastUpdate, LastUpdateRFC = :lastUpdateRFC, " +
			"SequenceNumber = :sequenceNumber"),
		ConditionExpression:       aws.String("OwnerID = :ownerID"),
		ExpressionAttributeValues: attrVals,
	}); err != nil {
		return fmt.Errorf("error releasing checkpoint: %s", err)
	}

	if cp.sequenceNumber != "" {
		cp.stats.Checkpoint()
	}

	cp.captured = false
	if cp.finished == false {
		cp.logger.Debugf("Releasing unfinished checkpoint for Shard with ID: %v", cp.shardID)
	}

	cp.logger.Debugf("Successfully released checkpointer for Shard with ID: %v", cp.shardID)
	return nil
}

// Update updates the current sequenceNumber of the checkpoint, marking it dirty if necessary
func (cp *Checkpointer) Update(sequenceNumber string) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	cp.dirty = cp.dirty || cp.sequenceNumber != sequenceNumber
	cp.sequenceNumber = sequenceNumber
}

// Finish marks the given sequence number as the final one for the shard.
func (cp *Checkpointer) Finish() {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	cp.finished = true
}

func (cp *Checkpointer) GetShardID() string {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	return cp.shardID
}
func (cp *Checkpointer) GetSequenceNumber() string {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	return cp.sequenceNumber
}

// LoadCheckpoints returns checkpoint records from dynamo mapped by shard id.
func LoadCheckpoints(db dynamodbiface.DynamoDBAPI, tableName string) (map[string]*CheckpointRecord, error) {
	params := &dynamodb.ScanInput{
		TableName:      aws.String(tableName),
		ConsistentRead: aws.Bool(true),
	}

	var records []*CheckpointRecord
	var innerError error
	err := db.ScanPages(params, func(p *dynamodb.ScanOutput, lastPage bool) (shouldContinue bool) {
		for _, item := range p.Items {
			var record CheckpointRecord
			innerError = dynamodbattribute.UnmarshalMap(item, &record)
			if innerError != nil {
				return false
			}
			records = append(records, &record)
		}

		return !lastPage
	})

	if innerError != nil {
		return nil, innerError
	}

	if err != nil {
		return nil, err
	}

	checkpointMap := make(map[string]*CheckpointRecord, len(records))
	for _, checkpoint := range records {
		checkpointMap[checkpoint.Shard] = checkpoint
	}
	return checkpointMap, nil
}

// dynamoTableActive returns an error if the given table is not ACTIVE
func (cp *Checkpointer) dynamoTableActive() error {
	out, err := cp.dynamodb.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(cp.tableName),
	})
	if err != nil {
		return fmt.Errorf("error describing table %s: %v", cp.tableName, err)
	}
	status := aws.StringValue(out.Table.TableStatus)
	if status != "ACTIVE" && status != "UPDATING" {
		return fmt.Errorf("table %s exists but state '%s' is not 'ACTIVE' or 'UPDATING'", cp.tableName, status)
	}
	return nil
}
