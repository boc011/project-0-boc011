package surfstore

import (
	context "context"
	"errors"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	mutex    sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	block, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		return nil, errors.New("GetBlock wrong")
	}

	return &Block{
		BlockData: block.GetBlockData(),
		BlockSize: block.GetBlockSize(),
	}, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	key := GetBlockHashString(block.BlockData)

	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	bs.BlockMap[key] = block

	return &Success{
		Flag: true,
	}, nil
}

// MissingBlocks Given a list of hashes “in”, returns a list containing the
// hashes that are not stored in the key-value store
func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	missingBlockHashes := &BlockHashes{
		Hashes: make([]string, 0),
	}

	for _, hash := range blockHashesIn.Hashes {
		bs.mutex.Lock()
		if _, ok := bs.BlockMap[hash]; ok {
			missingBlockHashes.Hashes = append(missingBlockHashes.Hashes, hash)
		}
		bs.mutex.Unlock()
	}

	return missingBlockHashes, nil
}

// GetBlockHashes Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	blockHashes := &BlockHashes{
		Hashes: make([]string, 0),
	}

	for hash := range bs.BlockMap {
		blockHashes.Hashes = append(blockHashes.Hashes, hash)
	}

	return blockHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
