package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	mutex              sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	filename := fileMetaData.Filename
	version := fileMetaData.Version

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.FileMetaMap[filename]; ok {
		if version-1 == m.FileMetaMap[filename].Version {
			m.FileMetaMap[filename] = fileMetaData
		} else {
			version = -1
		}
	} else {
		m.FileMetaMap[filename] = fileMetaData
	}
	return &Version{Version: version}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	c := m.ConsistentHashRing
	blockStoreMap := make(map[string]*BlockHashes)
	for _, hash := range blockHashesIn.Hashes {
		ResponsibleServer := c.GetResponsibleServer(hash)
		if _, ok := blockStoreMap[ResponsibleServer]; !ok {
			blockStoreMap[ResponsibleServer] = &BlockHashes{Hashes: []string{hash}}
		}
		blockStoreMap[ResponsibleServer].Hashes = append(blockStoreMap[ResponsibleServer].Hashes, hash)
	}
	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{
		BlockStoreAddrs: m.BlockStoreAddrs,
	}, nil
}

// Ensure that all methods required by the MetaStore interface are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
