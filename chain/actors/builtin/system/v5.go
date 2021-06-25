package system

import (
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/lotus/chain/actors/adt"

	system5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/system"
)

var _ State = (*state5)(nil)

// load5 通过 cid 加载 v5 SystemActor
func load5(store adt.Store, root cid.Cid) (State, error) {
	out := state5{store: store}
	err := store.Get(store.Context(), root, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// make5 创建一个空的 V5 SystemActor
func make5(store adt.Store) (State, error) {
	out := state5{store: store}
	out.State = system5.State{}
	return &out, nil
}

// 进一步封装具体的actor 实现
type state5 struct {
	system5.State
	store adt.Store
}

// GetState 返回 SystemActor 的 State
func (s *state5) GetState() interface{} {
	return &s.State
}
