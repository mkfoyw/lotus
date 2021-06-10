package sealing

import (
	"sync"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/storage-sealing/sealiface"
)

type statSectorState int

const (
	sstStaging statSectorState = iota
	sstSealing
	sstFailed
	sstProving
	nsst
)

// 对当前正在活跃扇区进行相关统计
type SectorStats struct {
	lk sync.Mutex

	// 当前扇区所处的阶段
	bySector map[abi.SectorID]statSectorState
	// 各个阶段总的扇区数量
	totals [nsst]uint64
}

func (ss *SectorStats) updateSector(cfg sealiface.Config, id abi.SectorID, st SectorState) (updateInput bool) {
	ss.lk.Lock()
	defer ss.lk.Unlock()

	//正在密封数量
	preSealing := ss.curSealingLocked()
	preStaging := ss.curStagingLocked()

	// update totals
	// 获取以前的状态， 并更新
	oldst, found := ss.bySector[id]
	if found {
		ss.totals[oldst]--
	}

	// 获取当前扇区的所处的状态， 并更新
	sst := toStatState(st)
	ss.bySector[id] = sst
	ss.totals[sst]++

	// check if we may need be able to process more deals
	sealing := ss.curSealingLocked()
	staging := ss.curStagingLocked()

	log.Debugw("sector stats", "sealing", sealing, "staging", staging)

	if cfg.MaxSealingSectorsForDeals > 0 && // max sealing deal sector limit set
		preSealing >= cfg.MaxSealingSectorsForDeals && // we were over limit
		sealing < cfg.MaxSealingSectorsForDeals { // and we're below the limit now
		updateInput = true
	}

	if cfg.MaxWaitDealsSectors > 0 && // max waiting deal sector limit set
		preStaging >= cfg.MaxWaitDealsSectors && // we were over limit
		staging < cfg.MaxWaitDealsSectors { // and we're below the limit now
		updateInput = true
	}

	return updateInput
}

// curSealingLocked 返回当前正在密封的扇区
func (ss *SectorStats) curSealingLocked() uint64 {
	return ss.totals[sstStaging] + ss.totals[sstSealing] + ss.totals[sstFailed]
}

// curStagingLocked 返回当前正在等待去密封的数量
func (ss *SectorStats) curStagingLocked() uint64 {
	return ss.totals[sstStaging]
}

// return the number of sectors currently in the sealing pipeline
// curSealing 返回当前正在密封的数量
func (ss *SectorStats) curSealing() uint64 {
	ss.lk.Lock()
	defer ss.lk.Unlock()

	return ss.curSealingLocked()
}

// return the number of sectors waiting to enter the sealing pipeline
// curStaging 返回当前等待进入密封的扇区数量
func (ss *SectorStats) curStaging() uint64 {
	ss.lk.Lock()
	defer ss.lk.Unlock()

	return ss.curStagingLocked()
}
