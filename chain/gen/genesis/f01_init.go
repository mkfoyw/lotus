package genesis

import (
	"context"
	"encoding/json"
	"fmt"

	init_ "github.com/filecoin-project/lotus/chain/actors/builtin/init"

	"github.com/filecoin-project/lotus/chain/actors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/specs-actors/actors/util/adt"

	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"

	bstore "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/genesis"
)

// SetupInitActor 创建 InitActor
func SetupInitActor(ctx context.Context, bs bstore.Blockstore, netname string, initialActors []genesis.Actor, rootVerifier genesis.Actor, remainder genesis.Actor, av actors.Version) (int64, *types.Actor, map[address.Address]address.Address, error) {
	if len(initialActors) > MaxAccounts {
		return 0, nil, nil, xerrors.New("too many initial actors")
	}

	cst := cbor.NewCborStore(bs)
	//创建 initActor 的 State
	ist, err := init_.MakeState(adt.WrapStore(ctx, cst), av, netname)
	if err != nil {
		return 0, nil, nil, err
	}

	// 改变 initActor.NexitID 值
	if err = ist.SetNextID(MinerStart); err != nil {
		return 0, nil, nil, err
	}

	// 加载 AddressMap
	amap, err := ist.AddressMap()
	if err != nil {
		return 0, nil, nil, err
	}

	keyToId := map[address.Address]address.Address{}
	counter := int64(AccountStart)

	// 添加初始Actor
	for _, a := range initialActors {

		// 添加 Multisig Account 中所有的 地址
		if a.Type == genesis.TMultisig {
			var ainfo genesis.MultisigMeta
			if err := json.Unmarshal(a.Meta, &ainfo); err != nil {
				return 0, nil, nil, xerrors.Errorf("unmarshaling account meta: %w", err)
			}
			for _, e := range ainfo.Signers {

				if _, ok := keyToId[e]; ok {
					continue
				}

				fmt.Printf("init set %s t0%d\n", e, counter)

				value := cbg.CborInt(counter)
				if err := amap.Put(abi.AddrKey(e), &value); err != nil {
					return 0, nil, nil, err
				}
				counter = counter + 1
				var err error
				keyToId[e], err = address.NewIDAddress(uint64(value))
				if err != nil {
					return 0, nil, nil, err
				}

			}
			// Need to add actors for all multisigs too
			continue
		}

		if a.Type != genesis.TAccount {
			return 0, nil, nil, xerrors.Errorf("unsupported account type: %s", a.Type)
		}

		//添加 普通的Account
		var ainfo genesis.AccountMeta
		if err := json.Unmarshal(a.Meta, &ainfo); err != nil {
			return 0, nil, nil, xerrors.Errorf("unmarshaling account meta: %w", err)
		}

		fmt.Printf("init set %s t0%d\n", ainfo.Owner, counter)

		value := cbg.CborInt(counter)
		if err := amap.Put(abi.AddrKey(ainfo.Owner), &value); err != nil {
			return 0, nil, nil, err
		}
		counter = counter + 1

		var err error
		keyToId[ainfo.Owner], err = address.NewIDAddress(uint64(value))
		if err != nil {
			return 0, nil, nil, err
		}
	}

	setupMsig := func(meta json.RawMessage) error {
		var ainfo genesis.MultisigMeta
		if err := json.Unmarshal(meta, &ainfo); err != nil {
			return xerrors.Errorf("unmarshaling account meta: %w", err)
		}
		for _, e := range ainfo.Signers {
			if _, ok := keyToId[e]; ok {
				continue
			}
			fmt.Printf("init set %s t0%d\n", e, counter)

			value := cbg.CborInt(counter)
			if err := amap.Put(abi.AddrKey(e), &value); err != nil {
				return err
			}
			counter = counter + 1
			var err error
			keyToId[e], err = address.NewIDAddress(uint64(value))
			if err != nil {
				return err
			}

		}

		return nil
	}

	if rootVerifier.Type == genesis.TAccount {
		var ainfo genesis.AccountMeta
		if err := json.Unmarshal(rootVerifier.Meta, &ainfo); err != nil {
			return 0, nil, nil, xerrors.Errorf("unmarshaling account meta: %w", err)
		}
		value := cbg.CborInt(80)
		if err := amap.Put(abi.AddrKey(ainfo.Owner), &value); err != nil {
			return 0, nil, nil, err
		}
	} else if rootVerifier.Type == genesis.TMultisig {
		err := setupMsig(rootVerifier.Meta)
		if err != nil {
			return 0, nil, nil, xerrors.Errorf("setting up root verifier msig: %w", err)
		}
	}

	if remainder.Type == genesis.TAccount {
		var ainfo genesis.AccountMeta
		if err := json.Unmarshal(remainder.Meta, &ainfo); err != nil {
			return 0, nil, nil, xerrors.Errorf("unmarshaling account meta: %w", err)
		}

		// TODO: Use builtin.ReserveAddress...
		value := cbg.CborInt(90)
		if err := amap.Put(abi.AddrKey(ainfo.Owner), &value); err != nil {
			return 0, nil, nil, err
		}
	} else if remainder.Type == genesis.TMultisig {
		err := setupMsig(remainder.Meta)
		if err != nil {
			return 0, nil, nil, xerrors.Errorf("setting up remainder msig: %w", err)
		}
	}

	// 获取地址映射的root 的 CID
	amapaddr, err := amap.Root()
	if err != nil {
		return 0, nil, nil, err
	}

	// 保存 AddressMap 的 CId 到 InitActor.AddressMap
	if err = ist.SetAddressMap(amapaddr); err != nil {
		return 0, nil, nil, err
	}

	// 持久化 InitActor 的 State 到数据库中， 并返回CID
	statecid, err := cst.Put(ctx, ist.GetState())
	if err != nil {
		return 0, nil, nil, err
	}

	// 获取 InitActor 的
	actcid, err := init_.GetActorCodeID(av)
	if err != nil {
		return 0, nil, nil, err
	}

	// 返回 types.Actor
	act := &types.Actor{
		Code:    actcid,
		Head:    statecid,
		Balance: big.Zero(),
	}

	return counter, act, keyToId, nil
}
