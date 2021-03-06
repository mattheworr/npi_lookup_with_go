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
	"sync"
)

type NPI_Taxonomy struct {
	NPI int
	Taxonomy string
}

func main() {
	start := -1
	loop := &start

	var wg sync.WaitGroup

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
	for {
		record, err := data.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if i, err := strconv.Atoi(record[0]); err == nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err = addNPI(db, i, record[1])
				if err != nil {
					log.Fatal(err)
				}
			}()
		*loop++
		fmt.Printf("Processed batch %v\n", *loop)
		}
	}
	wg.Wait()
	fmt.Printf("Success!\nStart time: %v\nEnd time: %v\n", st.Local(), time.Now().Local())
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

func addNPI(db *bolt.DB, npi int, taxonomy string) error {
	entry := NPI_Taxonomy{NPI: npi, Taxonomy: taxonomy}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("could not marshal entry json: %v", err)
	}
	err = db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("DB")).Bucket([]byte("NPI"))
		id, _ := b.NextSequence()
		b.Put([]byte(strconv.FormatInt(int64(id), 32)), encoded)
		if err != nil {
			return fmt.Errorf("could not insert entry: %v", err)
		}
		return nil
	})
	return err
}