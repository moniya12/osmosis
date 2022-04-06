package cmd

// DONTCOVER

import (
	"github.com/spf13/cobra"

	"fmt"
	"strconv"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	tmstore "github.com/tendermint/tendermint/store"
	tmdb "github.com/tendermint/tm-db"
)

// get cmd to convert any bech32 address to an osmo prefix.
func forceprune() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "forceprune [path string] [full_height] [min_height]",
		Short: "forceprune",
		Long: `forceprune
Example:
	osmosisd forceprune "/home/ec2-user/.osmosisd/data/" 188000 1000
	`,
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println(args[0])

			full_height, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				panic(err)
			}
			min_height, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				panic(err)
			}

			o := opt.Options{
				DisableSeeksCompaction: true,
			}

			db_bs, err := tmdb.NewGoLevelDBWithOpts("blockstore", args[0], &o)
			if err != nil {
				panic(err)
			}

			bs := tmstore.NewBlockStore(db_bs)
			start_height := bs.Base()
			current_height := bs.Height()

			fmt.Println("Pruning Block Store ...")
			bs.PruneBlocks(current_height - int64(full_height))
			fmt.Println("Compacting Block Store ...")
			db_bs.Close()

			db, err := leveldb.OpenFile(args[0]+"blockstore.db", &o)
			if err != nil {
				panic(err)
			}
			if err = db.CompactRange(*util.BytesPrefix([]byte{})); err != nil {
				panic(err)
			}

			db, err = leveldb.OpenFile(args[0]+"state.db", &o)
			if err != nil {
				panic(err)
			}
			a := []string{"validatorsKey:", "consensusParamsKey:", "abciResponsesKey:"}
			fmt.Println("Pruning State Store ...")
			for i, s := range a {
				fmt.Println(i, s)

				retain_height := int64(0)
				if s == "abciResponsesKey:" {
					retain_height = current_height - int64(min_height)
				} else {
					retain_height = current_height - int64(full_height)
				}

				batch := new(leveldb.Batch)
				pruned := uint64(0)

				fmt.Println(start_height, current_height, retain_height)
				for c := start_height; c < retain_height; c++ {
					batch.Delete([]byte(s + strconv.FormatInt(c, 10)))
					pruned++

					if pruned%1000 == 0 && pruned > 0 {
						err := db.Write(batch, nil)
						if err != nil {
							panic(err)
						}
						batch.Reset()
						batch = new(leveldb.Batch)
					}
				}

				err := db.Write(batch, nil)
				if err != nil {
					panic(err)
				}
				batch.Reset()
			}
			fmt.Println("Compacting State Store ...")
			if err = db.CompactRange(*util.BytesPrefix([]byte{})); err != nil {
				panic(err)
			}
			fmt.Println("Done ...")

			return nil
		},
	}

	return cmd
}
