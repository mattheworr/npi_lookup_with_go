package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"log"
)

type NPI_Taxonomy struct {
	NPI int
	Taxonomy string
}


func main() {
	db, err := bolt.Open("nbi.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("DB")).Bucket([]byte("NPI")).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Println(string(v))
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}