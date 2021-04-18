// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type loggerInterface interface {
	Debug(args ...interface{})
	Debugf(s string, args ...interface{})
	Errorf(s string, args ...interface{})
	Tracef(s string, args ...interface{})
}

type ShardConsumerError struct {
	ShardID string
	Action  string
	Error   error
	Level   uint32 // error levels consist of: WARNING, CRITICAL, FATAL
}

type ConsumedRecord struct {
	Record       *kinesis.Record // Record retrieved from kinesis
	Checkpointer *Checkpointer   // Object that will store the checkpoint back to the database
	retrievedAt  time.Time       // Time the record was retrieved from Kinesis
}

// Kinsumer is a Kinesis Consumer that tries to reduce duplicate reads while allowing for multiple
// clients each processing multiple shards
type Kinsumer struct {
	kinesis               kinesisiface.KinesisAPI   // interface to the kinesis service
	dynamodb              dynamodbiface.DynamoDBAPI // interface to the dynamodb service
	streamName            string                    // name of the kinesis stream to consume from
	enhancedFanOutMode    bool                      // Whether this client runs as an enhanced fan-out consumer events are pushed
	streamARN             string                    // ARN of the kinesis stream to consume from when using enhanced fan-out
	shardIDs              []string                  // all the shards in the stream, for detecting when the shards change
	stop                  chan struct{}             // channel used to signal to all the go routines that we want to stop consuming
	stoprequest           chan bool                 // channel used internally to signal to the main go routine to stop processing
	records               chan *ConsumedRecord      // channel for the go routines to put the consumed records on
	output                chan *ConsumedRecord      // unbuffered channel used to communicate from the main loop to the Next() method
	errors                chan *ShardConsumerError  // channel used to communicate errors back to the caller
	waitGroup             sync.WaitGroup            // waitGroup to sync the consumers go routines on
	mainWG                sync.WaitGroup            // WaitGroup for the mainLoop
	shardErrors           chan ShardConsumerError   // all the errors found by the consumers that were not handled
	clientsTableName      string                    // dynamo table of info about each client
	checkpointTableName   string                    // dynamo table of the checkpoints for each shard
	metadataTableName     string                    // dynamo table of metadata about the leader and shards
	clientID              string                    // identifier to differentiate between the running clients
	clientName            string                    // display name of the client - used just for debugging
	totalClients          int                       // The number of clients that are currently working on this stream
	thisClient            int                       // The (sorted by name) index of this client in the total list
	config                Config                    // configuration struct
	numberOfRuns          int32                     // Used to atomically make sure we only ever allow one Run() to be called
	isLeader              bool                      // Whether this client is the leader
	leaderLost            chan bool                 // Channel that receives an event when the node loses leadership
	leaderWG              sync.WaitGroup            // waitGroup for the leader loop
	maxAgeForClientRecord time.Duration             // Cutoff for client/checkpoint records we read from dynamodb before we assume the record is stale
	maxAgeForLeaderRecord time.Duration             // Cutoff for leader/shard cache records we read from dynamodb before we assume the record is stale
	logger                loggerInterface           // Cutoff for leader/shard cache records we read from dynamodb before we assume the record is stale
}

// New returns a Kinsumer Interface with default kinesis and dynamodb instances, to be used in ec2 instances to get default auth and config
func New(logger loggerInterface, streamName, applicationName, clientName string, config Config) (*Kinsumer, error) {
	s, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	return NewWithSession(logger, s, streamName, applicationName, clientName, config)
}

// NewWithSession should be used if you want to override the Kinesis and Dynamo instances with a non-default aws session
func NewWithSession(logger loggerInterface, session *session.Session, streamName, applicationName, clientName string, config Config) (*Kinsumer, error) {
	k := kinesis.New(session)
	d := dynamodb.New(session)

	return NewWithInterfaces(logger, k, d, streamName, applicationName, clientName, config)
}

// NewWithInterfaces allows you to override the Kinesis and Dynamo instances for mocking or using a local set of servers
func NewWithInterfaces(logger loggerInterface, kinesisi kinesisiface.KinesisAPI, dynamodb dynamodbiface.DynamoDBAPI, streamName, applicationName, clientName string, config Config) (*Kinsumer, error) {
	if logger == nil {
		return nil, ErrNoLoggerInterface
	}
	if kinesisi == nil {
		return nil, ErrNoKinesisInterface
	}
	if dynamodb == nil {
		return nil, ErrNoDynamoInterface
	}
	if streamName == "" {
		return nil, ErrNoStreamName
	}
	if applicationName == "" {
		return nil, ErrNoApplicationName
	}
	if err := validateConfig(&config); err != nil {
		return nil, err
	}

	// enhanced fan-out mode requires the ARN For whatever reason
	var streamARN string
	if config.enhancedFanOutMode {
		stream, err := kinesisi.DescribeStream(&kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			return nil, errors.Wrap(err, "describe-stream to get ARN for enhanced fan-out")
		}
		streamARN = aws.StringValue(stream.StreamDescription.StreamARN)
		logger.Debugf("found the streamARN for %s: %s", streamName, streamARN)
	}

	consumer := &Kinsumer{
		kinesis:               kinesisi,
		dynamodb:              dynamodb,
		streamName:            streamName,
		enhancedFanOutMode:    config.enhancedFanOutMode,
		streamARN:             streamARN,
		stoprequest:           make(chan bool),
		records:               make(chan *ConsumedRecord, config.bufferSize),
		output:                make(chan *ConsumedRecord),
		errors:                make(chan *ShardConsumerError, 10),
		shardErrors:           make(chan ShardConsumerError, 10),
		checkpointTableName:   applicationName + "_checkpoints",
		clientsTableName:      applicationName + "_clients",
		metadataTableName:     applicationName + "_metadata",
		clientID:              clientName,
		clientName:            clientName,
		config:                config,
		maxAgeForClientRecord: config.shardCheckFrequency * 5,
		maxAgeForLeaderRecord: config.leaderActionFrequency * 5,
		logger:                logger,
	}
	return consumer, nil
}

// refreshShards registers our client, refreshes the lists of clients and shards, checks if we
// have become/unbecome the leader, and returns whether the shards/clients changed.
//TODO: Write unit test - needs dynamo _and_ kinesis mocking
func (k *Kinsumer) refreshShards() (bool, error) {
	var shardIDs []string

	if err := registerWithClientsTable(k.dynamodb, k.clientID, k.clientName, k.clientsTableName); err != nil {
		return false, err
	}

	//TODO: Move this out of refreshShards and into refreshClients
	clients, err := getClients(k.dynamodb, k.clientID, k.clientsTableName, k.maxAgeForClientRecord)
	if err != nil {
		return false, err
	}

	totalClients := len(clients)
	thisClient := 0

	found := false
	for i, c := range clients {
		if c.ID == k.clientID {
			thisClient = i
			found = true
			break
		}
	}

	if !found {
		return false, ErrThisClientNotInDynamo
	}

	if thisClient == 0 && !k.isLeader {
		k.becomeLeader()
	} else if thisClient != 0 && k.isLeader {
		k.unbecomeLeader()
	}

	shardIDs, err = loadShardIDsFromDynamo(k.dynamodb, k.metadataTableName)
	//k.logger.Debug("refreshShards - loaded shardIDs: ", shardIDs)
	//k.logger.Debug("refreshShards - current shardIDs: ", k.shardIDs)
	if err != nil {
		k.logger.Debug("refreshShards - load shard IDs error: ", err)
		return false, err
	}

	changed := (totalClients != k.totalClients) ||
		(thisClient != k.thisClient) ||
		(len(k.shardIDs) != len(shardIDs))

	//k.logger.Debug("refreshShards - changed: ", changed)
	if !changed {
		for idx := range shardIDs {
			if shardIDs[idx] != k.shardIDs[idx] {
				changed = true
				break
			}
		}
	}

	if changed {
		k.shardIDs = shardIDs
	}

	k.thisClient = thisClient
	k.totalClients = totalClients

	return changed, nil
}

// startConsumers launches a shard consumer for each shard we should own
// TODO: Can we unit test this at all?
func (k *Kinsumer) startConsumers(registered *kinesis.RegisterStreamConsumerOutput) error {
	k.stop = make(chan struct{})
	assigned := false

	k.logger.Debug("StartConsumers - thisClient / shardIDs: ", k.thisClient, len(k.shardIDs))
	if k.thisClient >= len(k.shardIDs) {
		return nil
	}

	k.logger.Debug("StartConsumers - creating shards: ", len(k.shardIDs))
	for i, shard := range k.shardIDs {
		// Evenly distributes the shards between clients
		if (i % k.totalClients) == k.thisClient {
			k.waitGroup.Add(1)
			assigned = true

			k.logger.Debug("StartConsumers - beginning consumption for shard: ", shard)
			if registered == nil {
				go k.consume(shard)
			} else {
				go k.consumeWithFanOut(shard, registered)
			}
		}
	}
	// This may happen if we have more clients than shards
	if len(k.shardIDs) != 0 && !assigned {
		return ErrNoShardsAssigned
	}

	return nil
}

// stopConsumers stops all our shard consumers
func (k *Kinsumer) stopConsumers() {
	close(k.stop)
	k.waitGroup.Wait()
DrainLoop:
	for {
		select {
		case <-k.records:
		default:
			break DrainLoop
		}
	}
}

// dynamoTableActive returns an error if the given table is not ACTIVE
func (k *Kinsumer) dynamoTableActive(name string) error {
	out, err := k.dynamodb.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		return fmt.Errorf("error describing table %s: %v", name, err)
	}
	status := aws.StringValue(out.Table.TableStatus)
	if status != "ACTIVE" {
		return fmt.Errorf("table %s exists but state '%s' is not 'ACTIVE'", name, status)
	}
	return nil
}

// dynamoTableExists returns an true if the given table exists
func (k *Kinsumer) dynamoTableExists(name string) bool {
	_, err := k.dynamodb.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	})
	return err == nil
}

// dynamoCreateTableIfNotExists creates a table with the given name and distKey
// if it doesn't exist and will wait until it is created
func (k *Kinsumer) dynamoCreateTableIfNotExists(name, distKey string) error {
	if k.dynamoTableExists(name) {
		return nil
	}
	_, err := k.dynamodb.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{{
			AttributeName: aws.String(distKey),
			AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
		}},
		KeySchema: []*dynamodb.KeySchemaElement{{
			AttributeName: aws.String(distKey),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		}},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(k.config.dynamoReadCapacity),
			WriteCapacityUnits: aws.Int64(k.config.dynamoWriteCapacity),
		},
		TableName: aws.String(name),
	})
	if err != nil {
		return err
	}
	err = k.dynamodb.WaitUntilTableExistsWithContext(
		aws.BackgroundContext(),
		&dynamodb.DescribeTableInput{
			TableName: aws.String(name),
		},
		request.WithWaiterDelay(request.ConstantWaiterDelay(k.config.dynamoWaiterDelay)),
	)
	return err
}

// dynamoDeleteTableIfExists delete a table with the given name if it exists
// and will wait until it is deleted
func (k *Kinsumer) dynamoDeleteTableIfExists(name string) error {
	if !k.dynamoTableExists(name) {
		return nil
	}
	_, err := k.dynamodb.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		return err
	}
	err = k.dynamodb.WaitUntilTableNotExistsWithContext(
		aws.BackgroundContext(),
		&dynamodb.DescribeTableInput{
			TableName: aws.String(name),
		},
		request.WithWaiterDelay(request.ConstantWaiterDelay(k.config.dynamoWaiterDelay)),
	)
	return err
}

// kinesisStreamReady returns an error if the given stream is not ACTIVE
func (k *Kinsumer) kinesisStreamReady() error {
	out, err := k.kinesis.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(k.streamName),
	})
	if err != nil {
		return fmt.Errorf("error describing stream %s: %v", k.streamName, err)
	}

	status := aws.StringValue(out.StreamDescription.StreamStatus)
	if status != "ACTIVE" {
		return fmt.Errorf("stream %s exists but state '%s' is not 'ACTIVE'", k.streamName, status)
	}

	return nil
}

// Run runs the main kinesis consumer process. This is a non-blocking call, use Stop() to force it to return.
// This goroutine is responsible for startin/stopping consumers, aggregating all consumers' records,
// updating Checkpointers as records are consumed, and refreshing our shard/client list and leadership
//TODO: Can we unit test this at all?
// funcRefresh - function pointer that will be called anytime the shards are being refreshed.
func (k *Kinsumer) Run() error {
	if err := k.dynamoTableActive(k.checkpointTableName); err != nil {
		return err
	}
	if err := k.dynamoTableActive(k.clientsTableName); err != nil {
		return err
	}
	if err := k.kinesisStreamReady(); err != nil {
		return err
	}

	allowRun := atomic.CompareAndSwapInt32(&k.numberOfRuns, 0, 1)
	if !allowRun {
		return ErrRunTwice
	}

	if _, err := k.refreshShards(); err != nil {
		deregErr := deregisterFromClientsTable(k.dynamodb, k.clientID, k.clientsTableName)
		if deregErr != nil {
			return fmt.Errorf("error in kinsumer Run initial refreshShards: (%v); "+
				"error deregistering from clients table: (%v)", err, deregErr)
		}
		return fmt.Errorf("error in kinsumer Run initial refreshShards: %v", err)
	}

	// enhancedFanOutRegistration is not nil if we are running in
	// enhanced fan-out mode
	var enhancedFanOutRegistration *kinesis.RegisterStreamConsumerOutput

	if k.enhancedFanOutMode {
		var err error
		enhancedFanOutRegistration, err = k.kinesis.RegisterStreamConsumer(&kinesis.RegisterStreamConsumerInput{
			ConsumerName: aws.String(k.clientID),
			StreamARN:    aws.String(k.streamARN),
		})
		if err != nil {
			if awse, ok := err.(awserr.Error); ok && awse.Code() == "ResourceInUseException" {
				k.logger.Errorf("StreamConsumer for client %s already exists. Perhaps an improper shutdown occured. Remove it manually in AWS", k.clientID)
			}
			return errors.Wrap(err, "RegisterStreamConsumer")
		}
		k.logger.Debugf("RegisterStreamConsumer successful (enhanced-fan-out): %s", *enhancedFanOutRegistration.Consumer.ConsumerARN)

		// Wait a few seconds for the register to complete. I would like to call ListStreamConsumer
		// to make sure its complete but AWS puts a hard limit of 5 req/second per stream on that
		// endpoint which makes it difficult for us to use it with multiple consumers :/
		k.logger.Debug("Sleeping to give the newly registered stream consumer time become ACTIVE")
		time.Sleep(10 * time.Second)
	}

	k.mainWG.Add(1)
	go func() {
		defer k.mainWG.Done()

		defer func() {

			// Deregister is a nice to have but clients also time out if they
			// fail to deregister, so ignore error here.
			err := deregisterFromClientsTable(k.dynamodb, k.clientID, k.clientsTableName)
			if err != nil {
				k.errors <- &ShardConsumerError{
					Action: "deregisterFromClientsTable",
					Error:  fmt.Errorf("error deregistering client: %s", err),
					Level:  WarnLevel,
				}
			}
			k.unbecomeLeader()
			// Do this outside the k.isLeader check in case k.isLeader was false because
			// we lost leadership but haven't had time to shutdown the goroutine yet.
			k.leaderWG.Wait()
		}()

		// Run any other last min cleanup actions
		defer k.abort()

		// We close k.output so that Next() stops, this is also the reason
		// we can't allow Run() to be called after Stop() has happened
		defer close(k.output)

		shardChangeTicker := time.NewTicker(k.config.shardCheckFrequency)
		defer func() {
			shardChangeTicker.Stop()
		}()

		if err := k.startConsumers(enhancedFanOutRegistration); err != nil {
			k.errors <- &ShardConsumerError{
				Action: "startConsumers",
				Error:  fmt.Errorf("error starting consumers: %s", err),
				Level:  WarnLevel,
			}
			return
		}

		// defer stopping the Consumers after we've started them
		defer k.stopConsumers()

		var record *ConsumedRecord
		for {
			var (
				input  chan *ConsumedRecord
				output chan *ConsumedRecord
			)

			// We only want to be handing one record from the consumers
			// to the user of kinsumer at a time. We do this by only reading
			// one record off the records queue if we do not already have a
			// record to give away
			if record != nil {
				output = k.output
			} else {
				input = k.records
			}

			select {
			case <-k.stoprequest:
				return
			case record = <-input:
			case output <- record:
				record.Checkpointer.Update(aws.StringValue(record.Record.SequenceNumber))
				record = nil
			case se := <-k.shardErrors:
				k.logger.Errorf("shard error (%s) in %s: %s %d", se.ShardID, se.Action, se.Error, se.Level)
				k.errors <- &se
			case <-shardChangeTicker.C:
				changed, err := k.refreshShards()
				if err != nil {
					k.abort()
					k.errors <- &ShardConsumerError{
						Action: "refreshShards",
						Error:  fmt.Errorf("error refreshing shards: %s (will not attempt to shutdown... aborting)", err),
						Level:  FatalLevel, // fatal because of likely dynamo db issues
					}
				} else if changed {
					k.logger.Debug("Refreshing Shards.... ")

					shardChangeTicker.Stop()
					k.stopConsumers()
					record = nil
					if err := k.startConsumers(enhancedFanOutRegistration); err != nil {
						k.errors <- &ShardConsumerError{
							Action: "startConsumers",
							Error:  fmt.Errorf("error restarting consumers: %s (shutting down)", err),
							Level:  WarnLevel,
						}
						// Return allows us to shut down this consumer gracefully
						return
					}

					// Notify directly to any consumers that the shards are being refreshed.
					// To avoid any errors when dealing with shard size changes, this needs to be instant.
					k.errors <- &ShardConsumerError{
						Action: "refreshShards",
						Error:  fmt.Errorf("successfully Refreshed Shards"),
						Level:  InfoLevel,
					}

					//oneTimeChange = false
					// We create a new shardChangeTicker here so that the time it takes to stop and
					// start the consumers is not included in the wait for the next tick.
					shardChangeTicker = time.NewTicker(k.config.shardCheckFrequency)
					k.logger.Debug("successfully Refreshed Shards")
				}
			}
		}
	}()

	return nil
}

// abort is called before sending a fatal error back to the application
// so it must only contain actions that can be run in the face of a fatal
// error.
func (k *Kinsumer) abort() {
	k.logger.Debug("aborting the kinsumer")
	// If we are running in enhanced fan-out.. we will want to deregister the consumer
	// and allow it to re-register if its starts up again.
	if k.enhancedFanOutMode {
		_, err := k.kinesis.DeregisterStreamConsumer(&kinesis.DeregisterStreamConsumerInput{
			ConsumerName: aws.String(k.clientID),
			StreamARN:    aws.String(k.streamARN),
		})
		if err != nil {
			// If we fail to deregister, you will get a ResourceInUseException when the
			// app starts up again
			k.errors <- &ShardConsumerError{
				Action: "deregisterEnhancedFanOutConsumer",
				Error:  err,
				Level:  WarnLevel,
			}
		}
	}
}

// Stop stops the consumption of kinesis events
//TODO: Can we unit test this at all?
func (k *Kinsumer) Stop() {
	k.stoprequest <- true
	k.mainWG.Wait()
}

// Next is a blocking function used to get the next record from the kinesis queue, or errors that
// occurred during the processing of kinesis. It's up to the caller to stop processing by calling 'Stop()'
//
// if err is non nil an error occurred in the system.
// if err is nil and data is nil then kinsumer has been stopped
func (k *Kinsumer) Next() (data *ConsumedRecord, err *ShardConsumerError) {
	select {
	case err = <-k.errors:
		return nil, err
	case record, ok := <-k.output:
		if ok {
			k.config.stats.EventToClient(*record.Record.ApproximateArrivalTimestamp, record.retrievedAt, len(record.Record.Data))
			data = record
		}
	}

	return data, err
}

// CreateRequiredTables will create the required dynamodb tables
// based on the applicationName
func (k *Kinsumer) CreateRequiredTables() error {
	g := &errgroup.Group{}

	g.Go(func() error {
		return k.dynamoCreateTableIfNotExists(k.clientsTableName, "ID")
	})
	g.Go(func() error {
		return k.dynamoCreateTableIfNotExists(k.checkpointTableName, "Shard")
	})
	g.Go(func() error {
		return k.dynamoCreateTableIfNotExists(k.metadataTableName, "Key")
	})

	return g.Wait()
}

// DeleteTables will delete the dynamodb tables that were created
// based on the applicationName
func (k *Kinsumer) DeleteTables() error {
	g := &errgroup.Group{}

	g.Go(func() error {
		return k.dynamoDeleteTableIfExists(k.clientsTableName)
	})
	g.Go(func() error {
		return k.dynamoDeleteTableIfExists(k.checkpointTableName)
	})
	g.Go(func() error {
		return k.dynamoDeleteTableIfExists(k.metadataTableName)
	})

	return g.Wait()
}
