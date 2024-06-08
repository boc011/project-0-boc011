package surfstore

import (
	"errors"
	"sort"

	"io/fs"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

func ClientSync(client RPCClient) {
	localMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Printf("load meta file failed: %v\n", err)

	}

	updateMetaFile := make(map[string]*FileMetaData)
	err = filepath.Walk(client.BaseDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
			return err
		}
		if !info.IsDir() && info.Name() != DEFAULT_META_FILENAME {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}

			if len(data) == 0 {
				updateMetaFile[info.Name()] = &FileMetaData{
					Filename:      info.Name(),
					BlockHashList: []string{EMPTYFILE_HASHVALUE},
				}
			} else {
				handleFile(client, updateMetaFile, localMetaMap, info, data)
			}
		}
		return nil
	})

	if err != nil {
		log.Printf("error walking the path %q: %v\n", client.BaseDir, err)
		return
	}

	var remoteMetaMap map[string]*FileMetaData
	err = client.GetFileInfoMap(&remoteMetaMap)
	if err != nil {
		log.Printf("get remoteMetaMap failed: %v\n", err)
		return
	}

	for name, metaData := range localMetaMap {
		if remoteMetaData, ok := remoteMetaMap[name]; ok {
			if metaData.Version > remoteMetaData.Version {
				err = fileUploader(client, metaData)
				if err != nil {
					if strings.Contains(err.Error(), "version conflict") {
						err = handleConflict(client, metaData, remoteMetaData)
						if err != nil {
							log.Printf("handleConflict failed: %v\n", err)
							continue
						}
					} else {
						log.Printf("failed to uploadFile %s: %v\n", name, err)
						continue
					}
				}
			}
		} else {
			err = fileUploader(client, metaData)
			if err != nil {
				log.Printf("failed to uploadFile %s: %v\n", name, err)
				continue
			}
		}
	}

	for name, remoteMetaData := range remoteMetaMap {
		if localMetaData, ok := localMetaMap[name]; ok {
			if localMetaData.Version < remoteMetaData.Version {
				err = fileDownloader(client, remoteMetaData, localMetaData)
				if err != nil {
					log.Printf("fileDownloader failed: %v\n", err)
					return
				}
				localMetaMap[name] = remoteMetaData
			} else if remoteMetaData.Version == localMetaData.Version &&
				!reflect.DeepEqual(remoteMetaData.BlockHashList, localMetaData.BlockHashList) {
				err = fileUploader(client, localMetaData)
				if err != nil {
					if strings.Contains(err.Error(), "version conflict") {
						err = handleConflict(client, localMetaData, remoteMetaData)
						if err != nil {
							log.Printf("handleConflict failed: %v\n", err)
							continue
						}
					} else {
						log.Printf("uploadFile failed: %v\n", err)
						continue
					}
				}
			}

		} else {
			localMetaMap[name] = &FileMetaData{}
			err = fileDownloader(client, remoteMetaData, localMetaMap[name])
			if err != nil {
				log.Printf("fileDownloader failed: %v\n", err)
				return
			}
		}
	}

	err = WriteMetaFile(localMetaMap, client.BaseDir)
	if err != nil {
		log.Printf("write meta file failed: %v\n", err)
		return
	}

	for name, localMetaData := range localMetaMap {
		if localMetaData.BlockHashList[0] == "0" {
			path := ConcatPath(client.BaseDir, name)
			if _, err := os.Stat(path); err == nil {
				err = os.Remove(path)
				if err != nil {
					log.Printf("remove file %s failed: %v\n", path, err)
				}
			} else {
				log.Printf("file %s does not exist\n", path)
			}
		}
	}
}

// handleFile Handles the case where the file exists on the local machine
func handleFile(client RPCClient, updateMetaFile map[string]*FileMetaData, localMetaMap map[string]*FileMetaData, info fs.FileInfo, data []byte) {
	blockNum := (len(data) + client.BlockSize - 1) / client.BlockSize
	HashList := make([]string, blockNum)
	for i := 0; i < blockNum; i++ {
		endI := min((1+i)*client.BlockSize, len(data))
		blockHash := GetBlockHashString(data[i*client.BlockSize : endI])
		HashList[i] = blockHash
	}

	updateMetaFile[info.Name()] = &FileMetaData{
		Filename:      info.Name(),
		BlockHashList: HashList,
	}

	if localMetaData, ok := localMetaMap[info.Name()]; ok {
		if !reflect.DeepEqual(localMetaData.BlockHashList, updateMetaFile[info.Name()].BlockHashList) {
			localMetaMap[info.Name()].BlockHashList = updateMetaFile[info.Name()].BlockHashList
			localMetaMap[info.Name()].Version += 1
		}
	} else {
		localMetaMap[info.Name()] = &FileMetaData{
			Filename:      info.Name(),
			Version:       1,
			BlockHashList: updateMetaFile[info.Name()].BlockHashList,
		}
	}
}

// handleConflict handles the case where there is a conflict between the local and cloud file
func handleConflict(client RPCClient, localMetaData *FileMetaData, remoteMetaData *FileMetaData) error {
	if remoteMetaData.BlockHashList[0] != "0" {
		err := fileDownloader(client, remoteMetaData, localMetaData)
		if err != nil {
			return err
		}
		localMetaData.BlockHashList = remoteMetaData.BlockHashList
	}

	return nil
}

// fileDownloader downloads a file from the server
func fileDownloader(c RPCClient, fileMetaData *FileMetaData, localmf *FileMetaData) error {
	var blockAddrMap map[string][]string
	var BlockSAS []string
	err := c.GetBlockStoreAddrs(&BlockSAS)
	if err != nil {
		return err
	}

	err = c.GetBlockStoreMap(fileMetaData.BlockHashList, &blockAddrMap)
	if err != nil {
		return err
	}

	if len(fileMetaData.BlockHashList) == 1 && fileMetaData.BlockHashList[0] == EMPTYFILE_HASHVALUE {
		path := filepath.Join(c.BaseDir, fileMetaData.Filename)
		err = os.WriteFile(path, []byte{}, 0777)
		if err != nil {
			return err
		}
	} else {
		var fileData []byte
		for _, blockHash := range fileMetaData.BlockHashList {
			var block Block
			for serverAddr, blockHashList := range blockAddrMap {
				if isContain(blockHashList, blockHash) {
					err := c.GetBlock(blockHash, serverAddr, &block)
					if err != nil {
						return err
					}
					break
				}
			}

			fileData = append(fileData, block.BlockData...)
		}

		path := filepath.Join(c.BaseDir, fileMetaData.Filename)
		err = os.WriteFile(path, fileData, 0777)
		if err != nil {
			return err
		}
	}

	*localmf = *fileMetaData

	return nil

}

// fileUploader uploads a file to the server
func fileUploader(client RPCClient, metaData *FileMetaData) error {
	filePath := filepath.Join(client.BaseDir, metaData.Filename)
	latestVersion := metaData.Version

	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		return handleFileNotExist(client, metaData, &latestVersion)
	}

	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	if len(fileData) == 0 {
		metaData.BlockHashList = []string{EMPTYFILE_HASHVALUE}
	} else {
		err = handleFileData(client, metaData, fileData)
		if err != nil {
			return err
		}
	}

	err = client.UpdateFile(metaData, &latestVersion)
	if err != nil {
		return err
	}
	if latestVersion == -1 {
		return errors.New("version conflict: server version is newer")
	}

	metaData.Version = latestVersion

	return nil
}

// handleFileNotExist handles the case where the file does not exist on the server
func handleFileNotExist(client RPCClient, metaData *FileMetaData, latestVersion *int32) error {
	err := client.UpdateFile(metaData, latestVersion)
	if err != nil {
		log.Printf("Failed to update file %s: %v\n", metaData.Filename, err)
	}
	metaData.Version = *latestVersion
	return err
}

// handleFileData handles the case where the file exists on the server
func handleFileData(client RPCClient, metaData *FileMetaData, fileData []byte) error {
	var blockStoreAddrs []string
	err := client.GetBlockStoreAddrs(&blockStoreAddrs)
	if err != nil {
		return err
	}

	consistentHashRing := NewConsistentHashRing(blockStoreAddrs)
	blockHashList, err := createBlockHashList(client, consistentHashRing, fileData)
	if err != nil {
		return err
	}

	var cloudIndex map[string]*FileMetaData
	err = client.GetFileInfoMap(&cloudIndex)
	if err != nil {
		log.Fatalf("Failed to get cloudIndex: %v\n", err)
	}

	metaData.BlockHashList = blockHashList

	return nil
}

// createBlockHashList creates a list of block hashes for a file
func createBlockHashList(client RPCClient, consistentHashRing *ConsistentHashRing, fileData []byte) ([]string, error) {
	blockHashList := make([]string, 0)
	for i := 0; i < len(fileData); i += client.BlockSize {
		endI := i + client.BlockSize
		if endI > len(fileData) {
			endI = len(fileData)
		}
		blockData := fileData[i:endI]
		blockHash := GetBlockHashString(blockData)
		block := &Block{
			BlockData: blockData,
			BlockSize: int32(len(blockData)),
		}

		blockStoreAddr := consistentHashRing.GetResponsibleServer(blockHash)
		succ := false
		err := client.PutBlock(block, blockStoreAddr, &succ)
		if err != nil {
			return nil, err
		}
		blockHashList = append(blockHashList, blockHash)
	}
	return blockHashList, nil
}

// isContain checks if a string is in a sorted string slice
func isContain(strs []string, str string) bool {
	sort.Strings(strs)
	i := sort.SearchStrings(strs, str)
	return i < len(strs) && strs[i] == str
}
