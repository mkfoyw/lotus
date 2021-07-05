package genesis

import (
	"context"

	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/cron"

	cbor "github.com/ipfs/go-ipld-cbor"

	bstore "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/types"
)

// SetupCronActor 创建 CronActor
func SetupCronActor(ctx context.Context, bs bstore.Blockstore, av actors.Version) (*types.Actor, error) {
	cst := cbor.NewCborStore(bs)

	// 创建 CronActor的State
	st, err := cron.MakeState(adt.WrapStore(ctx, cbor.NewCborStore(bs)), av)
	if err != nil {
		return nil, err
	}

	// 存储 State， 并返回cid
	statecid, err := cst.Put(ctx, st.GetState())
	if err != nil {
		return nil, err
	}

	// 获取 CrodActor 的 codeid
	actcid, err := cron.GetActorCodeID(av)
	if err != nil {
		return nil, err
	}

	// 创建 types.Actor
	act := &types.Actor{
		Code: actcid,
		Head: statecid,
	}

	return act, nil
}
