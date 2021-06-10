package sealing

import (
	"time"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
)

type mutator interface {
	apply(state *SectorInfo)
}

// globalMutator is an event which can apply in every state
// globalMutator 表示这个事件引发的的输入动作能够运用到状态机的所有阶段
type globalMutator interface {
	// applyGlobal applies the event to the state. If if returns true,
	//  event processing should be interrupted
	applyGlobal(state *SectorInfo) bool
}

type Ignorable interface {
	Ignore()
}

// Global events

type SectorRestart struct{}

func (evt SectorRestart) applyGlobal(*SectorInfo) bool { return false }

type SectorFatalError struct{ error }

func (evt SectorFatalError) FormatError(xerrors.Printer) (next error) { return evt.error }

func (evt SectorFatalError) applyGlobal(state *SectorInfo) bool {
	log.Errorf("Fatal error on sector %d: %+v", state.SectorNumber, evt.error)
	// TODO: Do we want to mark the state as unrecoverable?
	//  I feel like this should be a softer error, where the user would
	//  be able to send a retry event of some kind
	return true
}

type SectorForceState struct {
	State SectorState
}

func (evt SectorForceState) applyGlobal(state *SectorInfo) bool {
	state.State = evt.State
	return true
}

// Normal path

// 创建一个空的扇区事件
type SectorStart struct {
	ID         abi.SectorNumber
	SectorType abi.RegisteredSealProof
}

func (evt SectorStart) apply(state *SectorInfo) {
	state.SectorNumber = evt.ID
	state.SectorType = evt.SectorType
}

// 创建一个垃圾扇区事件
type SectorStartCC struct {
	ID         abi.SectorNumber
	SectorType abi.RegisteredSealProof
}

func (evt SectorStartCC) apply(state *SectorInfo) {
	state.SectorNumber = evt.ID
	state.SectorType = evt.SectorType
}

type SectorAddPiece struct{}

// 开始添加一个 Piece 到一个扇区事件
func (evt SectorAddPiece) apply(state *SectorInfo) {
	if state.CreationTime == 0 {
		state.CreationTime = time.Now().Unix()
	}
}

// 完成添加一个Piece 到扇区事件
type SectorPieceAdded struct {
	NewPieces []Piece
}

func (evt SectorPieceAdded) apply(state *SectorInfo) {
	state.Pieces = append(state.Pieces, evt.NewPieces...)
}

// 添加一个 piece 到扇区失败事件
type SectorAddPieceFailed struct{ error }

func (evt SectorAddPieceFailed) FormatError(xerrors.Printer) (next error) { return evt.error }
func (evt SectorAddPieceFailed) apply(si *SectorInfo)                     {}

// 开始填充扇区的剩余空间事件
type SectorStartPacking struct{}

func (evt SectorStartPacking) apply(*SectorInfo) {}

func (evt SectorStartPacking) Ignore() {}

// 完成填充扇区的剩余空间事件
type SectorPacked struct{ FillerPieces []abi.PieceInfo }

func (evt SectorPacked) apply(state *SectorInfo) {
	for idx := range evt.FillerPieces {
		state.Pieces = append(state.Pieces, Piece{
			Piece:    evt.FillerPieces[idx],
			DealInfo: nil, // filler pieces don't have deals associated with them
		})
	}
}

// 已经获取扇区Ticket 事件
type SectorTicket struct {
	TicketValue abi.SealRandomness
	TicketEpoch abi.ChainEpoch
}

func (evt SectorTicket) apply(state *SectorInfo) {
	state.TicketEpoch = evt.TicketEpoch
	state.TicketValue = evt.TicketValue
}

// Tikect 以及过期事件
type SectorOldTicket struct{}

func (evt SectorOldTicket) apply(*SectorInfo) {}

// 扇区P1 完成事件
type SectorPreCommit1 struct {
	PreCommit1Out storage.PreCommit1Out
}

func (evt SectorPreCommit1) apply(state *SectorInfo) {
	state.PreCommit1Out = evt.PreCommit1Out
	state.PreCommit2Fails = 0
}

// 扇区 P2 完成事件
type SectorPreCommit2 struct {
	Sealed   cid.Cid
	Unsealed cid.Cid
}

func (evt SectorPreCommit2) apply(state *SectorInfo) {
	commd := evt.Unsealed
	state.CommD = &commd
	commr := evt.Sealed
	state.CommR = &commr
}

// 扇区批量提交事件
type SectorPreCommitBatch struct{}

func (evt SectorPreCommitBatch) apply(*SectorInfo) {}

type SectorPreCommitBatchSent struct {
	Message cid.Cid
}

func (evt SectorPreCommitBatchSent) apply(state *SectorInfo) {
	state.PreCommitMessage = &evt.Message
}

// 扇区 P1 信息已经在连上事件
type SectorPreCommitLanded struct {
	TipSet TipSetToken
}

func (evt SectorPreCommitLanded) apply(si *SectorInfo) {
	si.PreCommitTipSet = evt.TipSet
}

// P1 生成失败事件
type SectorSealPreCommit1Failed struct{ error }

func (evt SectorSealPreCommit1Failed) FormatError(xerrors.Printer) (next error) { return evt.error }

// P1 生成失败事件， 重置扇区信息
func (evt SectorSealPreCommit1Failed) apply(si *SectorInfo) {
	si.InvalidProofs = 0 // reset counter
	si.PreCommit2Fails = 0
}

// P2 生成失败事件， 记录P2 已经生成的次数
type SectorSealPreCommit2Failed struct{ error }

func (evt SectorSealPreCommit2Failed) FormatError(xerrors.Printer) (next error) { return evt.error }
func (evt SectorSealPreCommit2Failed) apply(si *SectorInfo) {
	si.InvalidProofs = 0 // reset counter
	si.PreCommit2Fails++
}

// PreCommit 上链失败事件
type SectorChainPreCommitFailed struct{ error }

func (evt SectorChainPreCommitFailed) FormatError(xerrors.Printer) (next error) { return evt.error }
func (evt SectorChainPreCommitFailed) apply(*SectorInfo)                        {}

// PreCommit 上链成功事件
type SectorPreCommitted struct {
	Message          cid.Cid
	PreCommitDeposit big.Int
	PreCommitInfo    miner.SectorPreCommitInfo
}

func (evt SectorPreCommitted) apply(state *SectorInfo) {
	state.PreCommitMessage = &evt.Message
	state.PreCommitDeposit = evt.PreCommitDeposit
	state.PreCommitInfo = &evt.PreCommitInfo
}

// SectorSeed 已经准备成功
type SectorSeedReady struct {
	SeedValue abi.InteractiveSealRandomness
	SeedEpoch abi.ChainEpoch
}

func (evt SectorSeedReady) apply(state *SectorInfo) {
	state.SeedEpoch = evt.SeedEpoch
	state.SeedValue = evt.SeedValue
}

// 生成扇区Commit信息失败事件
type SectorComputeProofFailed struct{ error }

func (evt SectorComputeProofFailed) FormatError(xerrors.Printer) (next error) { return evt.error }
func (evt SectorComputeProofFailed) apply(*SectorInfo)                        {}

type SectorCommitFailed struct{ error }

func (evt SectorCommitFailed) FormatError(xerrors.Printer) (next error) { return evt.error }
func (evt SectorCommitFailed) apply(*SectorInfo)                        {}

type SectorRetrySubmitCommit struct{}

func (evt SectorRetrySubmitCommit) apply(*SectorInfo) {}

type SectorDealsExpired struct{ error }

func (evt SectorDealsExpired) FormatError(xerrors.Printer) (next error) { return evt.error }
func (evt SectorDealsExpired) apply(*SectorInfo)                        {}

type SectorTicketExpired struct{ error }

func (evt SectorTicketExpired) FormatError(xerrors.Printer) (next error) { return evt.error }
func (evt SectorTicketExpired) apply(*SectorInfo)                        {}

// 扇区C2信息生成成功事件
type SectorCommitted struct {
	Proof []byte
}

func (evt SectorCommitted) apply(state *SectorInfo) {
	state.Proof = evt.Proof
}

// 开始批量提交扇区 Commit 信息事件
type SectorSubmitCommitAggregate struct{}

func (evt SectorSubmitCommitAggregate) apply(*SectorInfo) {}

type SectorCommitSubmitted struct {
	Message cid.Cid
}

func (evt SectorCommitSubmitted) apply(state *SectorInfo) {
	state.CommitMessage = &evt.Message
}

type SectorCommitAggregateSent struct {
	Message cid.Cid
}

func (evt SectorCommitAggregateSent) apply(state *SectorInfo) {
	state.CommitMessage = &evt.Message
}

type SectorProving struct{}

func (evt SectorProving) apply(*SectorInfo) {}

type SectorFinalized struct{}

func (evt SectorFinalized) apply(*SectorInfo) {}

type SectorRetryFinalize struct{}

func (evt SectorRetryFinalize) apply(*SectorInfo) {}

type SectorFinalizeFailed struct{ error }

func (evt SectorFinalizeFailed) FormatError(xerrors.Printer) (next error) { return evt.error }
func (evt SectorFinalizeFailed) apply(*SectorInfo)                        {}

// Failed state recovery

type SectorRetrySealPreCommit1 struct{}

func (evt SectorRetrySealPreCommit1) apply(state *SectorInfo) {}

type SectorRetrySealPreCommit2 struct{}

func (evt SectorRetrySealPreCommit2) apply(state *SectorInfo) {}

type SectorRetryPreCommit struct{}

func (evt SectorRetryPreCommit) apply(state *SectorInfo) {}

type SectorRetryWaitSeed struct{}

func (evt SectorRetryWaitSeed) apply(state *SectorInfo) {}

type SectorRetryPreCommitWait struct{}

func (evt SectorRetryPreCommitWait) apply(state *SectorInfo) {}

type SectorRetryComputeProof struct{}

func (evt SectorRetryComputeProof) apply(state *SectorInfo) {
	state.InvalidProofs++
}

type SectorRetryInvalidProof struct{}

func (evt SectorRetryInvalidProof) apply(state *SectorInfo) {
	state.InvalidProofs++
}

type SectorRetryCommitWait struct{}

func (evt SectorRetryCommitWait) apply(state *SectorInfo) {}

type SectorInvalidDealIDs struct {
	Return ReturnState
}

func (evt SectorInvalidDealIDs) apply(state *SectorInfo) {
	state.Return = evt.Return
}

type SectorUpdateDealIDs struct {
	Updates map[int]abi.DealID
}

func (evt SectorUpdateDealIDs) apply(state *SectorInfo) {
	for i, id := range evt.Updates {
		state.Pieces[i].DealInfo.DealID = id
	}
}

// Faults

type SectorFaulty struct{}

func (evt SectorFaulty) apply(state *SectorInfo) {}

type SectorFaultReported struct{ reportMsg cid.Cid }

func (evt SectorFaultReported) apply(state *SectorInfo) {
	state.FaultReportMsg = &evt.reportMsg
}

type SectorFaultedFinal struct{}

// Terminating

type SectorTerminate struct{}

func (evt SectorTerminate) applyGlobal(state *SectorInfo) bool {
	state.State = Terminating
	return true
}

type SectorTerminating struct{ Message *cid.Cid }

func (evt SectorTerminating) apply(state *SectorInfo) {
	state.TerminateMessage = evt.Message
}

type SectorTerminated struct{ TerminatedAt abi.ChainEpoch }

func (evt SectorTerminated) apply(state *SectorInfo) {
	state.TerminatedAt = evt.TerminatedAt
}

type SectorTerminateFailed struct{ error }

func (evt SectorTerminateFailed) FormatError(xerrors.Printer) (next error) { return evt.error }
func (evt SectorTerminateFailed) apply(*SectorInfo)                        {}

// External events

type SectorRemove struct{}

func (evt SectorRemove) applyGlobal(state *SectorInfo) bool {
	state.State = Removing
	return true
}

type SectorRemoved struct{}

func (evt SectorRemoved) apply(state *SectorInfo) {}

type SectorRemoveFailed struct{ error }

func (evt SectorRemoveFailed) FormatError(xerrors.Printer) (next error) { return evt.error }
func (evt SectorRemoveFailed) apply(*SectorInfo)                        {}
