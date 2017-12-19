package main

import (
	"time"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"encoding/csv"
	"io"
	"os"
	"strconv"
)

type NPI_Taxonomy struct {
	NPI int
	Taxonomy string
}


func main() {
	db, err := setupDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	file, err := os.Open("all_taxonomies.csv")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()
	data := csv.NewReader(file)

	for {
		record, err := data.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if i, err := strconv.Atoi(record[0]); err == nil {
			err = addNPI(db, i, record[1], time.Now())
			if err != nil {
				log.Fatal(err)
		}
		}
	}
}

func setupDB() (*bolt.DB, error) {
	db, err := bolt.Open("npi.db", 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("Could not open db, %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucketIfNotExists([]byte("DB"))
		if err != nil {
			return fmt.Errorf("Could not create root bucket: %v", err)
		}
		_, err = root.CreateBucketIfNotExists([]byte("NPI"))
		if err != nil {
			return fmt.Errorf("Could not create NPI bucket: %v", err)
		}
		return nil
	})
	fmt.Println("DB Setup Done")
	return db, nil
}

func addNPI(db *bolt.DB, npi int, taxonomy string, date time.Time) error {
	entry := NPI_Taxonomy{NPI: npi, Taxonomy: taxonomy}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("Could not marshal entry json: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket([]byte("DB")).Bucket([]byte("NPI")).Put([]byte(date.Format(time.RFC3339)), encoded)
		if err != nil {
			return fmt.Errorf("Could not insert entry: %v", err)
		}
		return nil
		
	})
	fmt.Println("Added NPI Entry")
	return err
}