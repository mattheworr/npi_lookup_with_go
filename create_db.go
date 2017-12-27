package main

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"time"
)

type NPI_Taxonomy struct {
	NPI int
	Taxonomy string
}

func main() {
	//start := -1
	//loop := &start

	fmt.Println("Connecting to database...")
	st := time.Now()
	db, err := setupDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("Opening file...")
	file, err := os.Open("all_taxonomies.csv")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	fmt.Println("Reading file...")
	data := csv.NewReader(file)
	taxMap := make(map[string][]NPI_Taxonomy)
	for {
		record, err := data.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if i, err := strconv.Atoi(record[0]); err == nil {
			mapTaxonomy(i, record[1], taxMap)
			//*loop++
			//fmt.Printf("Mapped entry %v\n", *loop)
		}
	}
	//*loop = -1
	for k := range taxMap {
		err = addNPI(db, k, taxMap[k])
		if err != nil {
			log.Fatal(err)
		}
		//*loop++
		//fmt.Printf("Processed entry %v\n", *loop)
	}
	fmt.Println("Success!\nExecution time:", time.Since(st))
}

func setupDB() (*bolt.DB, error) {
	db, err := bolt.Open("npi.db", 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("could not open db, %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucketIfNotExists([]byte("DB"))
		if err != nil {
			return fmt.Errorf("could not create root bucket: %v", err)
		}
		_, err = root.CreateBucketIfNotExists([]byte("NPI"))
		if err != nil {
			return fmt.Errorf("could not create NPI bucket: %v", err)
		}
		return nil
	})
	fmt.Println("DB Setup Done")
	return db, nil
}

func mapTaxonomy(npi int, taxonomy string, taxMap map[string][]NPI_Taxonomy) {
	entry := NPI_Taxonomy{NPI: npi, Taxonomy: taxonomy}
	taxMap[taxonomy] = append(taxMap[taxonomy], entry)
}

func addNPI(db *bolt.DB, taxonomy string, entry []NPI_Taxonomy) error {
	encoded, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("could not marshal entry json: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("DB")).Bucket([]byte("NPI"))
		b.Put([]byte(taxonomy), encoded)
		if err != nil {
			return fmt.Errorf("could not insert entry: %v", err)
		}
		return nil
	})
	return err
}