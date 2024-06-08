package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

// GetBlockHashBytes /* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

// ConcatPath /* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version ,hashIndex, hashValue) VALUES (?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	defer db.Close()

	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	_, _ = statement.Exec()

	statement, err = db.Prepare(insertTuple)
	if err != nil {
		return err
	}

	for name, data := range fileMetas {
		version := data.Version
		for hashIdx, hashVal := range data.BlockHashList {
			_, err = statement.Exec(name, version, hashIdx, hashVal)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `SELECT DISTINCT fileName FROM indexes;`

const getTuplesByFileName string = `SELECT version, hashIndex, hashValue
										FROM indexes WHERE fileName = ?
										ORDER BY hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Printf("Error When Opening Meta File")
		return fileMetaMap, err
	}
	defer db.Close()

	// first if the index table doesn't exist, create the index table
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Printf("Error in creating the table!")
	}
	statement.Exec()

	distinctFile, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Printf("Error When Querying Meta")
	}

	var fileName string
	for distinctFile.Next() {

		distinctFile.Scan(&fileName)
		res, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			log.Printf("Error When Scanning Meta")
			continue
		}

		var (
			version     int32
			hashIdx     int
			hashVal     string
			blockHashes []string
		)

		for res.Next() {
			err := res.Scan(&version, &hashIdx, &hashVal)
			if err != nil {
				return nil, err
			}
			blockHashes = append(blockHashes, hashVal)
		}

		fileMetaMap[fileName] = &FileMetaData{
			Filename:      fileName,
			Version:       version,
			BlockHashList: blockHashes,
		}
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {
	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
