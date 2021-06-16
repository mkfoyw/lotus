package sealiface

import "time"

// this has to be in a separate package to not make lotus API depend on filecoin-ffi

type Config struct {
	// 0 = no limit
	MaxWaitDealsSectors uint64

	// includes failed, 0 = no limit
	MaxSealingSectors uint64

	// includes failed, 0 = no limit
	MaxSealingSectorsForDeals uint64

	WaitDealsDelay time.Duration

	AlwaysKeepUnsealedCopy bool

	BatchPreCommits   bool
	MaxPreCommitBatch int
	MinPreCommitBatch int
	// PreCommit 消息能够等待被提交的最长时间间隔
	PreCommitBatchWait time.Duration
	// 表示在计算最晚需要进行提交已经流逝的时间
	PreCommitBatchSlack time.Duration

	AggregateCommits bool
	MinCommitBatch   int
	MaxCommitBatch   int
	CommitBatchWait  time.Duration
	CommitBatchSlack time.Duration

	TerminateBatchMax  uint64
	TerminateBatchMin  uint64
	TerminateBatchWait time.Duration
}
