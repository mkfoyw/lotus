package main

import (
	"errors"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper/basicfs"
	sealing "github.com/filecoin-project/lotus/extern/storage-sealing"
	"github.com/filecoin-project/lotus/lib/lotuslog"
	"github.com/filecoin-project/specs-storage/storage"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("lotus-sector-recovery")

func main() {
	lotuslog.SetupLogLevels()
	log.Info("Starting lotus-sector-recovery")

	app := &cli.App{
		Name:    "lotus-sector-recovery",
		Usage:   "repair sector faults",
		Version: build.UserVersion(),
		Commands: []*cli.Command{
			SectorRecoveryCmd,
		},
	}
	app.Setup()
	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

var SectorRecoveryCmd = &cli.Command{
	Name:  "sector",
	Usage: "",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "local-path",
			Usage: "speific file storage path",
			Value: "",
		},
		&cli.IntFlag{
			Name:  "push-path",
			Usage: "speific parrell_count",
			Value: 2,
		},
		&cli.StringFlag{
			Name:  "sectors",
			Usage: "error sectors number file",
		},
		&cli.BoolFlag{
			Name:  "is_push",
			Usage: "push : true  , not push : false ",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		//本地worker工作路径， 即密封过程中数据的存放目录
		localPath := cctx.String("local-path")
		if len(localPath) == 0 {
			return errors.New("need local path")
		}

		//存储节点路径
		pushPath := cctx.String("push-path")
		if len(pushPath) == 0 {
			return errors.New("need push path")
		}

		//保存需要恢复扇区编号的文件路径
		sectorPath := cctx.String("sector-path")
		if len(sectorPath) == 0 {
			return errors.New("need sector path")
		}

		// 这是要恢复的扇区编号
		secrorNumber := []uint64{}
		data, err := ioutil.ReadFile(sectorPath)
		sectorNumberStr := strings.Split(string(data), "\n")
		if err != nil {
			return err
		}
		for _, number := range sectorNumberStr {
			sn := strings.TrimSpace(number)
			if len(sn) == 0 {
				continue
			}
			log.Info("number : ", number)
			num, err := strconv.ParseUint(number, 10, 64)
			if err != nil {
				log.Error("sector number parser error :", num, err.Error())
				continue
			}
			secrorNumber = append(secrorNumber, num)
		}

		// 获取 miner API
		minerApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		//获取 actor
		log.Infof("getting miner actor")
		maddr, err := minerApi.ActorAddress(ctx)
		if err != nil {
			return err
		}
		mid, err := address.IDFromAddress(maddr)

		//创建 Sealer
		sealer, err := ffiwrapper.New(&basicfs.Provider{
			Root: localPath,
		})
		if err != nil {
			return err
		}

		//这儿就是完成addpiece， p1 和 p2 的过程
		sealProcess := func(status api.SectorInfo) {
			sid := storage.SectorRef{
				ID: abi.SectorID{
					Miner:  abi.ActorID(mid),
					Number: status.SectorID,
				},
				ProofType: status.SealProof,
			}

			sectorSize, err := status.SealProof.ProofSize()
			if err != nil {
				log.Error("sectorSize error : ", sectorSize)
				return
			}

			log.Info("ProofType : ", status.SealProof, "sectorSize : ", sectorSize)
			size := abi.PaddedPieceSize(sectorSize).Unpadded()

			// addPiece
			pieceInfo, err := sealer.AddPiece(ctx, sid, []abi.UnpaddedPieceSize{}, size, sealing.NewNullReader(size))
			if err != nil {
				log.Error("AddPiece error : ", err.Error())
				return
			}

			// p1 操作
			p1Out, err := sealer.SealPreCommit1(ctx, sid, status.Ticket.Value, []abi.PieceInfo{pieceInfo})
			if err != nil {
				log.Error("SealPreCommit1 error : ", err.Error())
				return
			}

			// p2 操作
			_, err = sealer.SealPreCommit2(ctx, sid, p1Out)
			if err != nil {
				log.Error("SealPreCommit2 error : ", err.Error())
				return
			}

			// finalize 操作
			err = sealer.FinalizeSector(ctx, sid, nil)
			if err != nil {
				log.Error("FinalizeSector error : ", err.Error())

				return
			}

			//开始传送文件(这需要自己写)
			// localpath/cache/sid
			// localpath/sealed/sid
			// err := PushCache(sid, from, to)
			// if err != nil{

			// }

		}
		for _, number := range secrorNumber {
			status, err := minerApi.SectorsStatus(ctx, abi.SectorNumber(number), true)
			if err != nil {
				return err
			}
			go sealProcess(status)
		}

		return nil
	},
}
