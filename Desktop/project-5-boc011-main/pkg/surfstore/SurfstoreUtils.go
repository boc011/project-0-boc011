package surfstore

import (
	"errors"
	"log"
	"math"
	"os"
	"reflect"
	"strings"
)

func ClientSync(client RPCClient) {
	files, err := os.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("Error when reading basedir")
		return
	}

	localMetaDatas, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("Could not load meta from meta file")
		return
	}

	PrintMetaMap(localMetaDatas)

	hashMap := make(map[string][]string)
	for _, file := range files {
		if isInvalidFile(file) {
			continue
		}

		hashset, err := getHashset(client, file)
		if err != nil {
			log.Println("Error reading file in basedir: ", err)
			return
		}

		hashMap[file.Name()] = hashset
	}

	updateLocalMetaDatas(localMetaDatas, hashMap)

	remoteMetaDatas := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteMetaDatas); err != nil {
		log.Println("Could not get remote index: ", err)
		return
	}

	PrintMetaMap(remoteMetaDatas)

	handleLocalFiles(client, localMetaDatas, remoteMetaDatas, hashMap)
	handleRemoteFiles(client, localMetaDatas, remoteMetaDatas, hashMap)

	err = WriteMetaFile(localMetaDatas, client.BaseDir)
	if err != nil {
		log.Println("Could not write meta file: ", err)
		return
	}
}

func uploadFileToCloud(client RPCClient, fileMetaData *FileMetaData, blockStoreMap map[string][]string) error {
	filePath := ConcatPath(client.BaseDir, fileMetaData.Filename)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return handleFileNotExist(client, fileMetaData)
	}

	file, err := os.Open(filePath)
	if err != nil {
		log.Println("Error opening file: ", err)
		return err
	}
	defer closeFile(file)

	fileStat, err := os.Stat(filePath)
	if err != nil {
		log.Println("Error getting fileInfo: ", err)
		return err
	}

	numsBlocks := int(math.Ceil(float64(fileStat.Size()) / float64(client.BlockSize)))
	for i := 0; i < numsBlocks; i++ {
		if err := handleBlock(client, file, blockStoreMap); err != nil {
			return err
		}
	}

	return updateFile(client, fileMetaData)
}

func downloadFileFromCloud(client RPCClient, localMetaData *FileMetaData, remoteMetaData *FileMetaData) error {
	filePath := ConcatPath(client.BaseDir, remoteMetaData.Filename)

	file, err := os.Create(filePath)
	if err != nil {
		return errors.New("error creating file")
	}
	defer closeFile(file)

	if _, err = os.Stat(filePath); err != nil {
		return errors.New("error getting fileInfo")
	}

	if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == "0" {
		return handleSingleBlock(client, localMetaData, remoteMetaData, filePath)
	}

	return handleMultipleBlocks(client, localMetaData, remoteMetaData, file)
}

func isInvalidFile(file os.DirEntry) bool {
	return file.Name() == "index.db" || strings.Contains(file.Name(), ",") || strings.Contains(file.Name(), "/")
}

func getHashset(client RPCClient, file os.DirEntry) ([]string, error) {
	fileInfo, _ := file.Info()
	var blocksNum = int(math.Ceil(float64(fileInfo.Size()) / float64(client.BlockSize)))
	fileToRead, err := os.Open(client.BaseDir + "/" + file.Name())
	if err != nil {
		return nil, err
	}
	defer closeFile(fileToRead)

	var hashset []string
	byteSlice := make([]byte, client.BlockSize)
	for i := 0; i < blocksNum; i++ {
		nRead, err := fileToRead.Read(byteSlice)
		if err != nil {
			return nil, err
		}

		hash := GetBlockHashString(byteSlice[:nRead])
		hashset = append(hashset, hash)
	}

	return hashset, nil
}

func updateLocalMetaDatas(localMetaDatas map[string]*FileMetaData, hashMap map[string][]string) {
	for filename, hashset := range hashMap {
		if localMetaDatas[filename] == nil { // check new file, then update it
			localMetaDatas[filename] = &FileMetaData{Filename: filename, Version: int32(1), BlockHashList: hashset}
		} else if !reflect.DeepEqual(localMetaDatas[filename].BlockHashList, hashset) { // check changed file
			localMetaDatas[filename].BlockHashList = hashset
			localMetaDatas[filename].Version = localMetaDatas[filename].Version + 1
		}
	}

	for fileName, fileMetaData := range localMetaDatas {
		if _, ok := hashMap[fileName]; !ok { // update the feature of the deleted file
			if len(fileMetaData.BlockHashList) != 1 || fileMetaData.BlockHashList[0] != "0" {
				fileMetaData.Version++
				fileMetaData.BlockHashList = []string{"0"}
			}
		}
	}
}

func handleLocalFiles(client RPCClient, localMetaDatas map[string]*FileMetaData, remoteMetaDatas map[string]*FileMetaData, hashMap map[string][]string) {
	for filename, localMD := range localMetaDatas {
		m := make(map[string][]string)
		err := client.GetBlockStoreMap(hashMap[filename], &m)
		if err != nil {
			log.Println("Could not get blockStoreAddr: ", err)
			return
		}
		if remoteMetaData, ok := remoteMetaDatas[filename]; ok {
			if localMD.Version > remoteMetaData.Version {
				err := uploadFileToCloud(client, localMD, m)
				if err != nil {
					log.Println("Could not upload file: ", err)
					return
				}
			}
		} else {
			err := uploadFileToCloud(client, localMD, m)
			if err != nil {
				log.Println("Could not upload file: ", err)
				return
			}
		}
	}
}

func handleRemoteFiles(client RPCClient, localMetaDatas map[string]*FileMetaData, remoteMetaDatas map[string]*FileMetaData, hashMap map[string][]string) {
	for filename, remoteMetaData := range remoteMetaDatas {
		m := make(map[string][]string)
		err := client.GetBlockStoreMap(hashMap[filename], &m)
		if err != nil {
			log.Println("Could not get blockStoreAddr: ", err)
			return
		}
		if localMD, ok := localMetaDatas[filename]; !ok {
			localMetaDatas[filename] = &FileMetaData{}
			err := downloadFileFromCloud(client, localMetaDatas[filename], remoteMetaData)
			if err != nil {
				log.Println("Could not download file: ", err)
				return
			}
		} else {
			if remoteMetaData.Version >= localMetaDatas[filename].Version {
				err := downloadFileFromCloud(client, localMD, remoteMetaData)
				if err != nil {
					log.Println("Could not download file: ", err)
					return
				}
			}
		}
	}
}

func closeFile(file *os.File) {
	if err := file.Close(); err != nil {
		log.Println("Error closing file: ", err)
	}
}

func handleSingleBlock(client RPCClient, localMetaData *FileMetaData, remoteMetaData *FileMetaData, filePath string) error {
	if err := os.Remove(filePath); err != nil {
		return errors.New("could not remove file")
	}
	updateMetaData(localMetaData, remoteMetaData)
	return nil
}

func handleMultipleBlocks(client RPCClient, localMetaData *FileMetaData, remoteMetaData *FileMetaData, file *os.File) error {
	m := make(map[string][]string)
	if err := client.GetBlockStoreMap(remoteMetaData.BlockHashList, &m); err != nil {
		return errors.New("could not get blockStoreAddr")
	}

	data, err := retrieveData(client, remoteMetaData, m)
	if err != nil {
		return err
	}

	if _, err := file.WriteString(data); err != nil {
		return errors.New("could not write to file")
	}

	updateMetaData(localMetaData, remoteMetaData)
	return nil
}

func updateMetaData(localMetaData *FileMetaData, remoteMetaData *FileMetaData) {
	*localMetaData = FileMetaData{
		Filename: remoteMetaData.Filename,
		Version:  remoteMetaData.Version,
	}
	copy(localMetaData.BlockHashList, remoteMetaData.BlockHashList)
}

func retrieveData(client RPCClient, remoteMetaData *FileMetaData, m map[string][]string) (string, error) {
	data := ""
	for _, hash := range remoteMetaData.BlockHashList {
		blockData, err := getBlockData(client, hash, m)
		if err != nil {
			return "", err
		}
		data += blockData
	}
	return data, nil
}

func getBlockData(client RPCClient, hash string, m map[string][]string) (string, error) {
	for bsAddr, blockHashes := range m {
		for _, blockHash := range blockHashes {
			if hash == blockHash {
				var block Block
				if err := client.GetBlock(hash, strings.ReplaceAll(bsAddr, "blockstore", ""), &block); err != nil {
					return "", errors.New("could not get block")
				}
				return string(block.BlockData), nil
			}
		}
	}
	return "", nil
}

func handleFileNotExist(client RPCClient, fileMetaData *FileMetaData) error {
	var latestVersion int32
	err := client.UpdateFile(fileMetaData, &latestVersion)
	if err != nil {
		log.Println("Could not update file")
		return err
	}
	fileMetaData.Version = latestVersion
	log.Println("Couldn't open Os")
	return err
}

func handleBlock(client RPCClient, file *os.File, blockStoreMap map[string][]string) error {
	byteSlice := make([]byte, client.BlockSize)
	nRead, err := file.Read(byteSlice)
	if err != nil {
		log.Println("Error reading bytes from file in basedir: ", err)
		return err
	}
	byteSlice = byteSlice[:nRead]

	block := Block{BlockData: byteSlice, BlockSize: int32(nRead)}
	hash := GetBlockHashString(block.BlockData)

	responsibleServer := getResponsibleServer(hash, blockStoreMap)

	var ok bool
	if err := client.PutBlock(&block, responsibleServer, &ok); err != nil {
		log.Println("Could not put block")
		return err
	}

	if !ok {
		log.Println("Could not put block")
		return errors.New("could not put block")
	}

	return nil
}

func getResponsibleServer(hash string, blockStoreMap map[string][]string) string {
	for bsAddr, blockHashes := range blockStoreMap {
		for _, blockHash := range blockHashes {
			if hash == blockHash {
				return bsAddr
			}
		}
	}
	return ""
}

func updateFile(client RPCClient, fileMetaData *FileMetaData) error {
	var latestVersion int32
	if err := client.UpdateFile(fileMetaData, &latestVersion); err != nil {
		log.Println("Could not update file")
		return err
	}

	fileMetaData.Version = latestVersion // update the version of the file
	return nil
}
