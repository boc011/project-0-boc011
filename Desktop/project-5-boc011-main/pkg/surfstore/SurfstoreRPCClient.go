package surfstore

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)

	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		return err
	}

	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	return nil
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)

	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := c.PutBlock(ctx, block)
	if err != nil {
		return err
	}

	*succ = res.Flag

	return nil
}

func (surfClient *RPCClient) MissingBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)

	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := c.MissingBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		return err
	}

	*blockHashesOut = res.Hashes

	return nil
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)

	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	*blockHashes = res.Hashes

	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}

		c := NewMetaStoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		res, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			if strings.Contains(err.Error(), ErrServerCrashed.Error()) {
				continue
			} else if strings.Contains(err.Error(), ErrServerCrashedUnreachable.Error()) {
				continue
			} else if strings.Contains(err.Error(), ErrNotLeader.Error()) {
				continue
			} else {
				return conn.Close()
			}
		}

		*serverFileInfoMap = res.FileInfoMap
		return conn.Close()
	}

	return errors.New("all servers are down")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}

		c := NewMetaStoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		res, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			if strings.Contains(err.Error(), ErrServerCrashed.Error()) {
				continue
			} else if strings.Contains(err.Error(), ErrServerCrashedUnreachable.Error()) {
				continue
			} else if strings.Contains(err.Error(), ErrNotLeader.Error()) {
				continue
			} else {
				return conn.Close()
			}
		}
		*latestVersion = res.Version
		return conn.Close()
	}

	return errors.New("all servers are down")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}

		c := NewMetaStoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		res, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil {
			if strings.Contains(err.Error(), ErrServerCrashed.Error()) {
				continue
			} else if strings.Contains(err.Error(), ErrServerCrashedUnreachable.Error()) {
				continue
			} else if strings.Contains(err.Error(), ErrNotLeader.Error()) {
				continue
			} else {
				return conn.Close()
			}
		}

		*blockStoreMap = make(map[string][]string)
		for k, v := range res.BlockStoreMap {
			(*blockStoreMap)[k] = v.Hashes
		}

		return conn.Close()
	}

	return errors.New("all servers are down")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}

		c := NewMetaStoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		res, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			if strings.Contains(err.Error(), ErrServerCrashed.Error()) {
				continue
			} else if strings.Contains(err.Error(), ErrServerCrashedUnreachable.Error()) {
				continue
			} else if strings.Contains(err.Error(), ErrNotLeader.Error()) {
				continue
			} else {
				return conn.Close()
			}
		}

		*blockStoreAddrs = res.BlockStoreAddrs

		return conn.Close()
	}

	return errors.New("all servers are down")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// NewSurfstoreRPCClient Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
