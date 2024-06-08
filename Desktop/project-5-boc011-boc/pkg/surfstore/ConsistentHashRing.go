package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	var hashes []string
	for h := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)

	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			return c.ServerMap[hashes[i]]
		}
	}

	return c.ServerMap[hashes[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	cHash := &ConsistentHashRing{
		ServerMap: make(map[string]string),
	}

	for _, addr := range serverAddrs {
		cHash.ServerMap[cHash.Hash("blockstore"+addr)] = addr
	}

	return cHash
}
