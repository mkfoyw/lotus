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

	// 计算每个扇区的最晚提交时间
	deadlines map[abi.SectorNumber]time.Time
	// 为每一个扇区生成一个批量提交信息， 以便后面批量提交
	todo map[abi.SectorNumber]*preCommitEntry
	// 为每一个扇区设置一个channel， 来接收提交结果。
	waiting map[abi.SectorNumber][]chan sealiface.PreCommitBatchRes

	notify, stop, stopped chan struct{}

	// 用来接收用户手动强制打包正在等待的 PreCommit 的请求， 并通过该channel 把提交的结果发送会给用户。
	force chan chan []sealiface.PreCommitBatchRes
	lk    sync.Mutex
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
	// 当用户手动批量的提交 PreCommit 消息时， 通过该管道把提交的结果发送给给用户。
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

// batchWait 返回我们下次提交需要等待的时间
func (b *PreCommitBatcher) batchWait(maxWait, slack time.Duration) <-chan time.Time {
	now := time.Now()

	b.lk.Lock()
	defer b.lk.Unlock()

	// 没有扇区 需要去提交
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
//	   nofify：当前有 PreCommit 可以进行打包
//     after: 当前已经达到某些 PreCommit 最长等待时间， 此时必须去打包。
//返回参数：
//	  批量提交扇区的结果， 以及 error
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
	// 开始处理批量提交 PreCommit 消息， 并等待返回提交结果
	res, err := b.processBatch(cfg)
	if err != nil && len(res) == 0 {
		return nil, err
	}

	// 消息已经提交成功
	for _, r := range res {
		if err != nil {
			r.Error = err.Error()
		}

		for _, sn := range r.Sectors {
			//给每个扇区发送提交结果
			for _, ch := range b.waiting[sn] {
				ch <- r // buffered
			}

			//清除一些缓存
			delete(b.waiting, sn)
			delete(b.todo, sn)
			delete(b.deadlines, sn)
		}
	}

	return res, nil
}

// processBatch 打包 PreCommit 消息，然后进行提交， 并返回提交结果
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
		// 添加本次批量提交的扇区
		res.Sectors = append(res.Sectors, p.pci.SectorNumber)
		// 添加本次批量提交的扇区信息
		params.Sectors = append(params.Sectors, *p.pci)
		// 计算总的需要质押金额
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
// AddPreCommit 添加一个扇区去准备批量提交， 并等待该扇区的提交结果。
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
	// 正在等待的扇区
	b.todo[sn] = &preCommitEntry{
		deposit: deposit,
		pci:     in,
	}

	// 创建该扇区用于接收提交结果的channel
	sent := make(chan sealiface.PreCommitBatchRes, 1)
	// 设置该扇区用于接收提交结果的channel
	b.waiting[sn] = append(b.waiting[sn], sent)

	select {
	// 发送一个信号， 表示以及有消息正在等待， 请注意提交
	case b.notify <- struct{}{}:
	default: // already have a pending notification, don't need more
	}
	b.lk.Unlock()

	// 用于等待该扇区的提交结果
	select {
	case c := <-sent:
		return c, nil
	case <-ctx.Done():
		return sealiface.PreCommitBatchRes{}, ctx.Err()
	}
}

// Flush 强制提交正在等待的 PreCommit 消息， 并返回提交结果。
func (b *PreCommitBatcher) Flush(ctx context.Context) ([]sealiface.PreCommitBatchRes, error) {
	// 用户接收执行结果
	resCh := make(chan []sealiface.PreCommitBatchRes, 1)
	select {
	// 发送提交请求到系统
	case b.force <- resCh:
		select {
		//等待提交结果
		case res := <-resCh:
			return res, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Pending 返回正在等待批量提交的扇区
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
