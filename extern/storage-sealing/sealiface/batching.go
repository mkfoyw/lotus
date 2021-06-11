package sealiface

import (
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-state-types/abi"
)

type CommitBatchRes struct {
	Sectors []abi.SectorNumber

	FailedSectors map[abi.SectorNumber]string

	Msg   *cid.Cid
	Error string // if set, means that all sectors are failed, implies Msg==nil
}

// 表示批量提交扇区中各个扇区的结果。
type PreCommitBatchRes struct {
	// 批量提交的扇区ID
	Sectors []abi.SectorNumber

	// 批量提交的消息ID
	Msg   *cid.Cid
	Error string // if set, means that all sectors are failed, implies Msg==nil
}
