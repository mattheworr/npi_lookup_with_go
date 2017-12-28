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
	npiMap := make(map[string][]NPI_Taxonomy)
	taxMap := make(map[string][]string)
	for {
		record, err := data.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if i, err := strconv.Atoi(record[0]); err == nil {
			makeMaps(i, record[1], npiMap, taxMap)
		}
	}

	for k := range npiMap {
		err = addNPI(db, k, npiMap[k])
		if err != nil {
			log.Fatal(err)
		}
	}

	for t := range taxMap {
		err = addTax(db, t, taxMap[t])
		if err != nil {
			log.Fatal(err)
		}
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
		_, err = root.CreateBucketIfNotExists([]byte("Taxonomy"))
		if err != nil {
			return fmt.Errorf("could not create taxonomy ID bucket: %v", err)
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


func makeMaps(npi int, taxonomy string, npiMap map[string][]NPI_Taxonomy, taxMap map[string][]string) {
	mapNPI(npi, taxonomy, npiMap)
	mapTaxonomy(taxonomy, taxMap)
}

func mapNPI(npi int, taxonomy string, npiMap map[string][]NPI_Taxonomy) {
	entry := NPI_Taxonomy{NPI: npi, Taxonomy: taxonomy}
	npiMap[taxonomy] = append(npiMap[taxonomy], entry)
}

func mapTaxonomy(taxonomy string, taxMap map[string][]string) {
	for _, j := range []int{2, 3, 4, 5, 6, 7, 8, 9} {
		if _, ok := taxMap[taxonomy[:j]]; ok {
		} else {
			taxMap[taxonomy[:j]] = append(taxMap[taxonomy[:j]], taxonomy)
		}
	}
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


func addTax(db *bolt.DB, taxKey string, taxList []string) error {
	encoded, err := json.Marshal(taxList)
	if err != nil {
		return fmt.Errorf("could not marshal entry json: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("DB")).Bucket([]byte("Taxonomy"))
		c.Put([]byte(taxKey), encoded)
		if err != nil {
			return fmt.Errorf("could not insert entry: %v", err)
		}
		return nil
	})
	return err
}

