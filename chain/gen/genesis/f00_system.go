package genesis

import (
	"context"

	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/system"

	cbor "github.com/ipfs/go-ipld-cbor"

	bstore "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/types"
)

// SetupSystemActor 创建系统actor
func SetupSystemActor(ctx context.Context, bs bstore.Blockstore, av actors.Version) (*types.Actor, error) {

	cst := cbor.NewCborStore(bs)

	//创建 SystemACtor 的State
	st, err := system.MakeState(adt.WrapStore(ctx, cst), av)
	if err != nil {
		return nil, err
	}

	// 存储SystemActor的 State 得到 cid
	statecid, err := cst.Put(ctx, st.GetState())
	if err != nil {
		return nil, err
	}

	// 获取 SystemActor 的Code  cid
	actcid, err := system.GetActorCodeID(av)
	if err != nil {
		return nil, err
	}

	//创建系统Actor
	act := &types.Actor{
		Code: actcid,
		Head: statecid,
	}

	return act, nil
}
