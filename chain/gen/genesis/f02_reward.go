package genesis

import (
	"context"

	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/reward"

	"github.com/filecoin-project/go-state-types/big"

	cbor "github.com/ipfs/go-ipld-cbor"

	bstore "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/types"
)

// SetupRewardActor 创建 RewardActor
func SetupRewardActor(ctx context.Context, bs bstore.Blockstore, qaPower big.Int, av actors.Version) (*types.Actor, error) {
	cst := cbor.NewCborStore(bs)

	// 创建 RewardActor 的 State
	rst, err := reward.MakeState(adt.WrapStore(ctx, cst), av, qaPower)
	if err != nil {
		return nil, err
	}

	// 持久化 State， 并返回cid
	statecid, err := cst.Put(ctx, rst.GetState())
	if err != nil {
		return nil, err
	}

	// 获取 RewardActor 的 CodeID
	actcid, err := reward.GetActorCodeID(av)
	if err != nil {
		return nil, err
	}

	// 创建Actor
	act := &types.Actor{
		Code:    actcid,
		Balance: types.BigInt{Int: build.InitialRewardBalance},
		Head:    statecid,
	}

	return act, nil
}
