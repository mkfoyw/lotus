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
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	miner5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/extern/storage-sealing/sealiface"
)

type PreCommitBatcherApi interface {
	SendMsg(ctx context.Context, from, to address.Address, method abi.MethodNum, value, maxFee abi.TokenAmount, params []byte) (cid.Cid, error)
	StateMinerInfo(context.Context, address.Address, TipSetToken) (miner.MinerInfo, error)
	ChainHead(ctx context.Context) (TipSetToken, abi.ChainEpoch, error)
}

type preCommitEntry struct {
	deposit abi.TokenAmount
	pci     *miner0.SectorPreCommitInfo
}

type PreCommitBatcher struct {
	api PreCommitBatcherApi
	// 矿工地址
	maddr     address.Address
	mctx      context.Context
	addrSel   AddrSel
	feeCfg    FeeConfig
	getConfig GetSealingConfigFunc

	// 每个扇区的最晚提交信息
	deadlines map[abi.SectorNumber]time.Time
	// 等待批量提交的 PreCommit 信息
	todo map[abi.SectorNumber]*preCommitEntry
	// 该扇区等待上链的消息
	waiting map[abi.SectorNumber][]chan sealiface.PreCommitBatchRes

	notify, stop, stopped chan struct{}
	force                 chan chan []sealiface.PreCommitBatchRes
	lk                    sync.Mutex
}

func NewPreCommitBatcher(mctx context.Context, maddr address.Address, api PreCommitBatcherApi, addrSel AddrSel, feeCfg FeeConfig, getConfig GetSealingConfigFunc) *PreCommitBatcher {
	b := &PreCommitBatcher{
		api:       api,
		maddr:     maddr,
		mctx:      mctx,
		addrSel:   addrSel,
		feeCfg:    feeCfg,
		getConfig: getConfig,

		deadlines: map[abi.SectorNumber]time.Time{},
		todo:      map[abi.SectorNumber]*preCommitEntry{},
		waiting:   map[abi.SectorNumber][]chan sealiface.PreCommitBatchRes{},

		notify:  make(chan struct{}, 1),
		force:   make(chan chan []sealiface.PreCommitBatchRes),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}

	go b.run()

	return b
}

func (b *PreCommitBatcher) run() {
	var forceRes chan []sealiface.PreCommitBatchRes
	var lastRes []sealiface.PreCommitBatchRes

	cfg, err := b.getConfig()
	if err != nil {
		panic(err)
	}

	for {
		if forceRes != nil {
			forceRes <- lastRes
			forceRes = nil
		}
		lastRes = nil

		var sendAboveMax, sendAboveMin bool
		select {
		case <-b.stop:
			close(b.stopped)
			return
		case <-b.notify:
			sendAboveMax = true
		case <-b.batchWait(cfg.PreCommitBatchWait, cfg.PreCommitBatchSlack):
			sendAboveMin = true
		case fr := <-b.force: // user triggered
			forceRes = fr
		}

		var err error
		lastRes, err = b.maybeStartBatch(sendAboveMax, sendAboveMin)
		if err != nil {
			log.Warnw("PreCommitBatcher processBatch error", "error", err)
		}
	}
}

func (b *PreCommitBatcher) batchWait(maxWait, slack time.Duration) <-chan time.Time {
	now := time.Now()

	b.lk.Lock()
	defer b.lk.Unlock()

	if len(b.todo) == 0 {
		return nil
	}

	// 最晚必须要批量提交 PreCommit 消息的时间
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

// maybeStartBatch 判断是否开始打包 PreCommit 信息
// 输入参数：
//	   nofif: 当前等待的 PreCommit 信息， 小于我们设置的最多能够批量提交的消息是否不要进行打包
//     after: 当前等待的 PreCommit 数量， 小于我们设置的最少能够批量提交的消息是否不要进行打包
func (b *PreCommitBatcher) maybeStartBatch(notif, after bool) ([]sealiface.PreCommitBatchRes, error) {
	b.lk.Lock()
	defer b.lk.Unlock()

	//获取当前等待打包的 PreCommit 信息
	total := len(b.todo)
	if total == 0 {
		return nil, nil // nothing to do
	}

	cfg, err := b.getConfig()
	if err != nil {
		return nil, xerrors.Errorf("getting config: %w", err)
	}

	// 当前等待打包的数量小于我们期望提交的 PreCommit 数量
	if notif && total < cfg.MaxPreCommitBatch {
		return nil, nil
	}

	// 当前等待打包的PreCommit 信息小于我们想要批量提交的PreCommit 最小数量是否不要进行打包。
	if after && total < cfg.MinPreCommitBatch {
		return nil, nil
	}

	// todo support multiple batches
	// 开始处理批量提交 PreCommit 消息
	res, err := b.processBatch(cfg)
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

// processBatch 打包 PreCommit 消息，然后进行批量提交 PreCommit 信息
func (b *PreCommitBatcher) processBatch(cfg sealiface.Config) ([]sealiface.PreCommitBatchRes, error) {
	// 开始构造批量提交 PreCommit 消息
	params := miner5.PreCommitSectorBatchParams{}
	// 这批扇区应该预先只要的钱
	deposit := big.Zero()
	// 扇区提交的结果
	var res sealiface.PreCommitBatchRes

	for _, p := range b.todo {
		if len(params.Sectors) >= cfg.MaxPreCommitBatch {
			log.Infow("precommit batch full")
			break
		}

		res.Sectors = append(res.Sectors, p.pci.SectorNumber)
		params.Sectors = append(params.Sectors, *p.pci)
		deposit = big.Add(deposit, p.deposit)
	}

	// 序列化消息参数
	enc := new(bytes.Buffer)
	if err := params.MarshalCBOR(enc); err != nil {
		return []sealiface.PreCommitBatchRes{res}, xerrors.Errorf("couldn't serialize PreCommitSectorBatchParams: %w", err)
	}

	// 获取miner 相关信息
	mi, err := b.api.StateMinerInfo(b.mctx, b.maddr, nil)
	if err != nil {
		return []sealiface.PreCommitBatchRes{res}, xerrors.Errorf("couldn't get miner info: %w", err)
	}

	// 这是我们提交这条消息最少需要的代币
	goodFunds := big.Add(deposit, b.feeCfg.MaxPreCommitGasFee)
	// 找到一个可以提交这条消息的地址
	from, _, err := b.addrSel(b.mctx, mi, api.PreCommitAddr, goodFunds, deposit)
	if err != nil {
		return []sealiface.PreCommitBatchRes{res}, xerrors.Errorf("no good address found: %w", err)
	}

	mcid, err := b.api.SendMsg(b.mctx, from, b.maddr, miner.Methods.PreCommitSectorBatch, deposit, b.feeCfg.MaxPreCommitGasFee, enc.Bytes())
	if err != nil {
		return []sealiface.PreCommitBatchRes{res}, xerrors.Errorf("sending message failed: %w", err)
	}

	res.Msg = &mcid

	log.Infow("Sent ProveCommitAggregate message", "cid", mcid, "from", from, "sectors", len(b.todo))

	return []sealiface.PreCommitBatchRes{res}, nil
}

// register PreCommit, wait for batch message, return message CID
func (b *PreCommitBatcher) AddPreCommit(ctx context.Context, s SectorInfo, deposit abi.TokenAmount, in *miner0.SectorPreCommitInfo) (res sealiface.PreCommitBatchRes, err error) {
	_, curEpoch, err := b.api.ChainHead(b.mctx)
	if err != nil {
		log.Errorf("getting chain head: %s", err)
		return sealiface.PreCommitBatchRes{}, err
	}

	sn := s.SectorNumber

	b.lk.Lock()
	//设置该扇区的最晚提交时间
	b.deadlines[sn] = getSectorDeadline(curEpoch, s)
	b.todo[sn] = &preCommitEntry{
		deposit: deposit,
		pci:     in,
	}

	sent := make(chan sealiface.PreCommitBatchRes, 1)
	b.waiting[sn] = append(b.waiting[sn], sent)

	select {
	case b.notify <- struct{}{}:
	default: // already have a pending notification, don't need more
	}
	b.lk.Unlock()

	select {
	case c := <-sent:
		return c, nil
	case <-ctx.Done():
		return sealiface.PreCommitBatchRes{}, ctx.Err()
	}
}

func (b *PreCommitBatcher) Flush(ctx context.Context) ([]sealiface.PreCommitBatchRes, error) {
	resCh := make(chan []sealiface.PreCommitBatchRes, 1)
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

// Pending 返回正在等待批量提交的扇区Actor
func (b *PreCommitBatcher) Pending(ctx context.Context) ([]abi.SectorID, error) {
	b.lk.Lock()
	defer b.lk.Unlock()

	// 获取 miner 的ID
	mid, err := address.IDFromAddress(b.maddr)
	if err != nil {
		return nil, err
	}

	// 表示等待被批量提交的Sector
	res := make([]abi.SectorID, 0)
	for _, s := range b.todo {
		res = append(res, abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: s.pci.SectorNumber,
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

func (b *PreCommitBatcher) Stop(ctx context.Context) error {
	close(b.stop)

	select {
	case <-b.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
