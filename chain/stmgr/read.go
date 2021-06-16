package stmgr

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/chain/state"
	"github.com/filecoin-project/lotus/chain/types"
)

// ParentStateTsk 从tipsetKey 获取该tipset 的父tipset的状态树
func (sm *StateManager) ParentStateTsk(tsk types.TipSetKey) (*state.StateTree, error) {
	ts, err := sm.cs.GetTipSetFromKey(tsk)
	if err != nil {
		return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
	}
	return sm.ParentState(ts)
}

// ParentState 返回 tipset 的父状态树的树
func (sm *StateManager) ParentState(ts *types.TipSet) (*state.StateTree, error) {

	cst := cbor.NewCborStore(sm.cs.StateBlockstore())
	state, err := state.LoadStateTree(cst, sm.parentState(ts))
	if err != nil {
		return nil, xerrors.Errorf("load state tree: %w", err)
	}

	return state, nil
}

func (sm *StateManager) StateTree(st cid.Cid) (*state.StateTree, error) {
	cst := cbor.NewCborStore(sm.cs.StateBlockstore())
	state, err := state.LoadStateTree(cst, st)
	if err != nil {
		return nil, xerrors.Errorf("load state tree: %w", err)
	}

	return state, nil
}

// LoadActor 根据地址加载一个 Actor
func (sm *StateManager) LoadActor(_ context.Context, addr address.Address, ts *types.TipSet) (*types.Actor, error) {
	state, err := sm.ParentState(ts)
	if err != nil {
		return nil, err
	}
	return state.GetActor(addr)
}

// LoadActorTsk 通过tipsetKey 获取状态树， 然后根据提供的地址从状态树获取type.Actor
func (sm *StateManager) LoadActorTsk(_ context.Context, addr address.Address, tsk types.TipSetKey) (*types.Actor, error) {
	//加载状态树
	state, err := sm.ParentStateTsk(tsk)
	if err != nil {
		return nil, err
	}
	return state.GetActor(addr)
}

func (sm *StateManager) LoadActorRaw(_ context.Context, addr address.Address, st cid.Cid) (*types.Actor, error) {
	state, err := sm.StateTree(st)
	if err != nil {
		return nil, err
	}
	return state.GetActor(addr)
}
