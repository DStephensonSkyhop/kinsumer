// Copyright (c) 2016 Twitch Interactive

package kinsumer

import "errors"

// A constant exposing all logging levels
var AllLevels = []uint32{
	PanicLevel,
	FatalLevel,
	ErrorLevel,
	WarnLevel,
	InfoLevel,
	DebugLevel,
	TraceLevel,
}

// These are the different logging levels
const (
	// PanicLevel level, highest level of severity. Logs and then calls panic with the
	// message passed to Debug, Info, ...
	PanicLevel uint32 = iota
	// FatalLevel level. Logs and then calls `logger.Exit(1)`. It will exit even if the
	// logging level is set to Panic.
	FatalLevel
	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	ErrorLevel
	// WarnLevel level. Non-critical entries that deserve eyes.
	WarnLevel
	// InfoLevel level. General operational entries about what's going on inside the
	// application.
	InfoLevel
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel
	// TraceLevel level. Designates finer-grained informational events than the Debug.
	TraceLevel
)

var (
	ErrNoLoggerInterface = errors.New("Need a logger implementation")
	// ErrRunTwice - Run() can only ever be run once
	ErrRunTwice = errors.New("Run() can only ever be run once")
	// ErrNoKinesisInterface - Need a kinesis instance
	ErrNoKinesisInterface = errors.New("Need a kinesis instance")
	// ErrNoDynamoInterface - Need a dynamodb instance
	ErrNoDynamoInterface = errors.New("Need a dynamodb instance")
	// ErrNoStreamName - Need a kinesis stream name
	ErrNoStreamName = errors.New("Need a kinesis stream name")
	// ErrNoApplicationName - Need an application name for the dynamo table names
	ErrNoApplicationName = errors.New("Need an application name for the dynamo table names")
	// ErrThisClientNotInDynamo - Unable to find this client in the client list
	ErrThisClientNotInDynamo = errors.New("Unable to find this client in the client list")
	// ErrNoShardsAssigned - We found shards, but got assigned none
	ErrNoShardsAssigned = errors.New("We found shards, but got assigned none")

	// ErrConfigInvalidThrottleDelay - ThrottleDelay config value must be at least 200ms
	ErrConfigInvalidThrottleDelay = errors.New("ThrottleDelay config value must be at least 200ms (preferably 250ms)")
	// ErrConfigInvalidCommitFrequency - CommitFrequency config value is mandatory
	ErrConfigInvalidCommitFrequency = errors.New("CommitFrequency config value is mandatory")
	// ErrConfigInvalidShardCheckFrequency - ShardCheckFrequency config value is mandatory
	ErrConfigInvalidShardCheckFrequency = errors.New("ShardCheckFrequency config value is mandatory")
	// ErrConfigInvalidLeaderActionFrequency - LeaderActionFrequency config value is mandatory
	ErrConfigInvalidLeaderActionFrequency = errors.New("LeaderActionFrequency config value is mandatory and must be at least as long as ShardCheckFrequency")
	// ErrConfigInvalidBufferSize - BufferSize config value is mandatory
	ErrConfigInvalidBufferSize = errors.New("BufferSize config value is mandatory")
	// ErrConfigInvalidStats - Stats cannot be nil
	ErrConfigInvalidStats = errors.New("Stats cannot be nil")
	// ErrConfigInvalidDynamoCapacity - Dynamo read/write capacity cannot be 0
	ErrConfigInvalidDynamoCapacity = errors.New("Dynamo read/write capacity cannot be 0")

	// ErrKinesisCantDescribeStream - Unable to describe stream
	ErrKinesisCantDescribeStream = errors.New("Unable to describe stream")
	// ErrKinesisBeingCreated -Str eam is busy being created
	ErrKinesisBeingCreated = errors.New("Stream is busy being created")
	// ErrKinesisBeingDeleted -Str eam is busy being deleted
	ErrKinesisBeingDeleted = errors.New("Stream is busy being deleted")
)
