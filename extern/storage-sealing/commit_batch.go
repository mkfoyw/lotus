package sealing

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	miner5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"
	proof5 "github.com/filecoin-project/specs-actors/v5/actors/runtime/proof"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/actors/policy"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/storage-sealing/sealiface"
)

const arp = abi.RegisteredAggregationProof_SnarkPackV1

type CommitBatcherApi interface {
	SendMsg(ctx context.Context, from, to address.Address, method abi.MethodNum, value, maxFee abi.TokenAmount, params []byte) (cid.Cid, error)
	StateMinerInfo(context.Context, address.Address, TipSetToken) (miner.MinerInfo, error)
	ChainHead(ctx context.Context) (TipSetToken, abi.ChainEpoch, error)

	StateSectorPreCommitInfo(ctx context.Context, maddr address.Address, sectorNumber abi.SectorNumber, tok TipSetToken) (*miner.SectorPreCommitOnChainInfo, error)
	StateMinerInitialPledgeCollateral(context.Context, address.Address, miner.SectorPreCommitInfo, TipSetToken) (big.Int, error)
}

type AggregateInput struct {
	spt   abi.RegisteredSealProof
	info  proof5.AggregateSealVerifyInfo
	proof []byte
}

type CommitBatcher struct {
	api       CommitBatcherApi
	maddr     address.Address
	mctx      context.Context
	addrSel   AddrSel
	feeCfg    FeeConfig
	getConfig GetSealingConfigFunc
	prover    ffiwrapper.Prover

	deadlines map[abi.SectorNumber]time.Time
	todo      map[abi.SectorNumber]AggregateInput
	waiting   map[abi.SectorNumber][]chan sealiface.CommitBatchRes

	notify, stop, stopped chan struct{}
	force                 chan chan []sealiface.CommitBatchRes
	lk                    sync.Mutex
}

func NewCommitBatcher(mctx context.Context, maddr address.Address, api CommitBatcherApi, addrSel AddrSel, feeCfg FeeConfig, getConfig GetSealingConfigFunc, prov ffiwrapper.Prover) *CommitBatcher {
	b := &CommitBatcher{
		api:       api,
		maddr:     maddr,
		mctx:      mctx,
		addrSel:   addrSel,
		feeCfg:    feeCfg,
		getConfig: getConfig,
		prover:    prov,

		deadlines: map[abi.SectorNumber]time.Time{},
		todo:      map[abi.SectorNumber]AggregateInput{},
		waiting:   map[abi.SectorNumber][]chan sealiface.CommitBatchRes{},

		notify:  make(chan struct{}, 1),
		force:   make(chan chan []sealiface.CommitBatchRes),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}

	go b.run()

	return b
}

func (b *CommitBatcher) run() {
	var forceRes chan []sealiface.CommitBatchRes
	var lastMsg []sealiface.CommitBatchRes

	cfg, err := b.getConfig()
	if err != nil {
		panic(err)
	}

	for {
		if forceRes != nil {
			forceRes <- lastMsg
			forceRes = nil
		}
		lastMsg = nil

		var sendAboveMax, sendAboveMin bool
		select {
		case <-b.stop:
			close(b.stopped)
			return
		case <-b.notify:
			sendAboveMax = true
		case <-b.batchWait(cfg.CommitBatchWait, cfg.CommitBatchSlack):
			sendAboveMin = true
		case fr := <-b.force: // user triggered
			forceRes = fr
		}

		var err error
		lastMsg, err = b.maybeStartBatch(sendAboveMax, sendAboveMin)
		if err != nil {
			log.Warnw("CommitBatcher processBatch error", "error", err)
		}
	}
}

// batchWait 返回最晚需要打包进行批量提交的时间
func (b *CommitBatcher) batchWait(maxWait, slack time.Duration) <-chan time.Time {
	now := time.Now()

	b.lk.Lock()
	defer b.lk.Unlock()

	if len(b.todo) == 0 {
		return nil
	}

	var deadline time.Time
	for sn := range b.todo {
		sectorDeadline := b.deadlines[sn]
		if deadline.IsZero() || (!sectorDeadline.IsZero() && sectorDeadline.Before(deadline)) {
			deadline = sectorDeadline
		}
	}
	for sn := range b.waiting {
		sectorDeadline := b.deadlines[sn]
		if deadline.IsZero() || (!sectorDeadline.IsZero() && sectorDeadline.Before(deadline)) {
			deadline = sectorDeadline
		}
	}

	if deadline.IsZero() {
		return time.After(maxWait)
	}

	deadline = deadline.Add(-slack)
	if deadline.Before(now) {
		return time.After(time.Nanosecond) // can't return 0
	}

	wait := deadline.Sub(now)
	if wait > maxWait {
		wait = maxWait
	}

	return time.After(wait)
}

func (b *CommitBatcher) maybeStartBatch(notif, after bool) ([]sealiface.CommitBatchRes, error) {
	b.lk.Lock()
	defer b.lk.Unlock()

	total := len(b.todo)
	if total == 0 {
		return nil, nil // nothing to do
	}

	cfg, err := b.getConfig()
	if err != nil {
		return nil, xerrors.Errorf("getting config: %w", err)
	}

	if notif && total < cfg.MaxCommitBatch {
		return nil, nil
	}

	if after && total < cfg.MinCommitBatch {
		return nil, nil
	}

	var res []sealiface.CommitBatchRes

	if total < cfg.MinCommitBatch || total < miner5.MinAggregatedSectors {
		res, err = b.processIndividually()
	} else {
		res, err = b.processBatch(cfg)
	}
	if err != nil && len(res) == 0 {
		return nil, err
	}

	for _, r := range res {
		if err != nil {
			r.Error = err.Error()
		}

		for _, sn := range r.Sectors {
			for _, ch := range b.waiting[sn] {
				ch <- r // buffered
			}

			delete(b.waiting, sn)
			delete(b.todo, sn)
			delete(b.deadlines, sn)
		}
	}

	return res, nil
}

// processBatch 开始打包 commit 消息， 然后进行批量提交， 然后返回批量提交的结果
func (b *CommitBatcher) processBatch(cfg sealiface.Config) ([]sealiface.CommitBatchRes, error) {
	tok, _, err := b.api.ChainHead(b.mctx)
	if err != nil {
		return nil, err
	}

	total := len(b.todo)

	var res sealiface.CommitBatchRes

	params := miner5.ProveCommitAggregateParams{
		SectorNumbers: bitfield.New(),
	}

	proofs := make([][]byte, 0, total)
	infos := make([]proof5.AggregateSealVerifyInfo, 0, total)
	collateral := big.Zero()

	for id, p := range b.todo {
		if len(infos) >= cfg.MaxCommitBatch {
			log.Infow("commit batch full")
			break
		}

		sc, err := b.getSectorCollateral(id, tok)
		if err != nil {
			res.FailedSectors[id] = err.Error()
			continue
		}

		collateral = big.Add(collateral, sc)

		res.Sectors = append(res.Sectors, id)
		params.SectorNumbers.Set(uint64(id))
		infos = append(infos, p.info)
	}

	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Number < infos[j].Number
	})

	for _, info := range infos {
		proofs = append(proofs, b.todo[info.Number].proof)
	}

	mid, err := address.IDFromAddress(b.maddr)
	if err != nil {
		return []sealiface.CommitBatchRes{res}, xerrors.Errorf("getting miner id: %w", err)
	}

	params.AggregateProof, err = b.prover.AggregateSealProofs(proof5.AggregateSealVerifyProofAndInfos{
		Miner:          abi.ActorID(mid),
		SealProof:      b.todo[infos[0].Number].spt,
		AggregateProof: arp,
		Infos:          infos,
	}, proofs)
	if err != nil {
		return []sealiface.CommitBatchRes{res}, xerrors.Errorf("aggregating proofs: %w", err)
	}

	enc := new(bytes.Buffer)
	if err := params.MarshalCBOR(enc); err != nil {
		return []sealiface.CommitBatchRes{res}, xerrors.Errorf("couldn't serialize ProveCommitAggregateParams: %w", err)
	}

	mi, err := b.api.StateMinerInfo(b.mctx, b.maddr, nil)
	if err != nil {
		return []sealiface.CommitBatchRes{res}, xerrors.Errorf("couldn't get miner info: %w", err)
	}

	goodFunds := big.Add(b.feeCfg.MaxCommitGasFee, collateral)

	from, _, err := b.addrSel(b.mctx, mi, api.CommitAddr, goodFunds, collateral)
	if err != nil {
		return []sealiface.CommitBatchRes{res}, xerrors.Errorf("no good address found: %w", err)
	}

	mcid, err := b.api.SendMsg(b.mctx, from, b.maddr, miner.Methods.ProveCommitAggregate, collateral, b.feeCfg.MaxCommitGasFee, enc.Bytes())
	if err != nil {
		return []sealiface.CommitBatchRes{res}, xerrors.Errorf("sending message failed: %w", err)
	}

	res.Msg = &mcid

	log.Infow("Sent ProveCommitAggregate message", "cid", mcid, "from", from, "todo", total, "sectors", len(infos))

	return []sealiface.CommitBatchRes{res}, nil
}

// processIndividually
func (b *CommitBatcher) processIndividually() ([]sealiface.CommitBatchRes, error) {
	mi, err := b.api.StateMinerInfo(b.mctx, b.maddr, nil)
	if err != nil {
		return nil, xerrors.Errorf("couldn't get miner info: %w", err)
	}

	tok, _, err := b.api.ChainHead(b.mctx)
	if err != nil {
		return nil, err
	}

	var res []sealiface.CommitBatchRes

	for sn, info := range b.todo {
		r := sealiface.CommitBatchRes{
			Sectors: []abi.SectorNumber{sn},
		}

		mcid, err := b.processSingle(mi, sn, info, tok)
		if err != nil {
			log.Errorf("process single error: %+v", err) // todo: return to user
			r.FailedSectors[sn] = err.Error()
		} else {
			r.Msg = &mcid
		}

		res = append(res, r)
	}

	return res, nil
}

// processSingle 提交一个扇区的 Commit 信息
func (b *CommitBatcher) processSingle(mi miner.MinerInfo, sn abi.SectorNumber, info AggregateInput, tok TipSetToken) (cid.Cid, error) {
	enc := new(bytes.Buffer)
	params := &miner.ProveCommitSectorParams{
		SectorNumber: sn,
		Proof:        info.proof,
	}

	if err := params.MarshalCBOR(enc); err != nil {
		return cid.Undef, xerrors.Errorf("marshaling commit params: %w", err)
	}

	collateral, err := b.getSectorCollateral(sn, tok)
	if err != nil {
		return cid.Undef, err
	}

	goodFunds := big.Add(collateral, b.feeCfg.MaxCommitGasFee)

	from, _, err := b.addrSel(b.mctx, mi, api.CommitAddr, goodFunds, collateral)
	if err != nil {
		return cid.Undef, xerrors.Errorf("no good address to send commit message from: %w", err)
	}

	mcid, err := b.api.SendMsg(b.mctx, from, b.maddr, miner.Methods.ProveCommitSector, collateral, b.feeCfg.MaxCommitGasFee, enc.Bytes())
	if err != nil {
		return cid.Undef, xerrors.Errorf("pushing message to mpool: %w", err)
	}

	return mcid, nil
}

// register commit, wait for batch message, return message CID
// AddCommit 添加Commit 信息， 并等待其提交， 并返回提交结果。
func (b *CommitBatcher) AddCommit(ctx context.Context, s SectorInfo, in AggregateInput) (res sealiface.CommitBatchRes, err error) {
	_, curEpoch, err := b.api.ChainHead(b.mctx)
	if err != nil {
		log.Errorf("getting chain head: %s", err)
		return sealiface.CommitBatchRes{}, nil
	}

	sn := s.SectorNumber

	b.lk.Lock()
	b.deadlines[sn] = getSectorDeadline(curEpoch, s)
	b.todo[sn] = in

	sent := make(chan sealiface.CommitBatchRes, 1)
	b.waiting[sn] = append(b.waiting[sn], sent)

	select {
	case b.notify <- struct{}{}:
	default: // already have a pending notification, don't need more
	}
	b.lk.Unlock()

	//等待提交的结果
	select {
	case r := <-sent:
		return r, nil
	case <-ctx.Done():
		return sealiface.CommitBatchRes{}, ctx.Err()
	}
}

// Flush 用户手动启用打包 Commit 信息， 并提交， 然后等待提交结果。
func (b *CommitBatcher) Flush(ctx context.Context) ([]sealiface.CommitBatchRes, error) {
	resCh := make(chan []sealiface.CommitBatchRes, 1)
	select {
	case b.force <- resCh:
		select {
		case res := <-resCh:
			return res, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Pending 获取等待被提交的Commit 信息
func (b *CommitBatcher) Pending(ctx context.Context) ([]abi.SectorID, error) {
	b.lk.Lock()
	defer b.lk.Unlock()

	mid, err := address.IDFromAddress(b.maddr)
	if err != nil {
		return nil, err
	}

	res := make([]abi.SectorID, 0)
	for _, s := range b.todo {
		res = append(res, abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: s.info.Number,
		})
	}

	sort.Slice(res, func(i, j int) bool {
		if res[i].Miner != res[j].Miner {
			return res[i].Miner < res[j].Miner
		}

		return res[i].Number < res[j].Number
	})

	return res, nil
}

func (b *CommitBatcher) Stop(ctx context.Context) error {
	close(b.stop)

	select {
	case <-b.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// getSectorDeadline 获取提交 PreCommit 信息的截止日期
func getSectorDeadline(curEpoch abi.ChainEpoch, si SectorInfo) time.Time {
	// 获取票的最大有效期限(31.5个小时)
	deadlineEpoch := si.TicketEpoch + policy.MaxPreCommitRandomnessLookback
	for _, p := range si.Pieces {
		if p.DealInfo == nil {
			continue
		}

		startEpoch := p.DealInfo.DealSchedule.StartEpoch
		if startEpoch < deadlineEpoch {
			deadlineEpoch = startEpoch
		}
	}

	if deadlineEpoch <= curEpoch {
		return time.Now()
	}

	return time.Now().Add(time.Duration(deadlineEpoch-curEpoch) * time.Duration(build.BlockDelaySecs) * time.Second)
}

// getSectorCollateral 获取扇区质押的金额
func (b *CommitBatcher) getSectorCollateral(sn abi.SectorNumber, tok TipSetToken) (abi.TokenAmount, error) {
	pci, err := b.api.StateSectorPreCommitInfo(b.mctx, b.maddr, sn, tok)
	if err != nil {
		return big.Zero(), xerrors.Errorf("getting precommit info: %w", err)
	}
	if pci == nil {
		return big.Zero(), xerrors.Errorf("precommit info not found on chain")
	}

	collateral, err := b.api.StateMinerInitialPledgeCollateral(b.mctx, b.maddr, pci.Info, tok)
	if err != nil {
		return big.Zero(), xerrors.Errorf("getting initial pledge collateral: %w", err)
	}

	collateral = big.Sub(collateral, pci.PreCommitDeposit)
	if collateral.LessThan(big.Zero()) {
		collateral = big.Zero()
	}

	return collateral, nil
}
