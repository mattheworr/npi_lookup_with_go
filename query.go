package main

import (
	"github.com/boltdb/bolt"
	"log"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
	"fmt"
	"bytes"
	"sync"
)

type NPI_Taxonomy struct {
	NPI int
	Taxonomy string
}

func main() {
	db, err := bolt.Open("npi.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	http.HandleFunc("/taxonomy", func(w http.ResponseWriter, r *http.Request) {
		st := time.Now()
		querystring := r.URL.Query()
		prefixes := querystring["prefix"]
		res := make(map[string][]NPI_Taxonomy)

		var wg sync.WaitGroup

		for _, prefix := range prefixes {
			res[prefix] = []NPI_Taxonomy{}
			prefixByte := []byte(prefix)
			wg.Add(1)
			go func() {
				defer wg.Done()
				err = db.Batch(func(tx *bolt.Tx) error {
					c := tx.Bucket([]byte("DB")).Bucket([]byte("NPI")).Cursor()
					for k, v := c.Seek(prefixByte); k != nil && bytes.HasPrefix(k, prefixByte); k, v = c.Next() {
						n := decodeV(string(v))
							res[prefix] = append(res[prefix], n...)
					}
					return nil
				})
			}()
		}
		wg.Wait()

		b, err := json.Marshal(res)
		if err != nil {
			log.Fatal(err)
		}

		w.Write(b)
		fmt.Println("Success!\nExecution time:", time.Since(st))
	})

	log.Fatal(http.ListenAndServe(":3535", nil))
	fmt.Println("Running http://localhost:3535/")
}

func decodeV(jsonStream string) []NPI_Taxonomy {
	dec := json.NewDecoder(strings.NewReader(string(jsonStream)))
	var n []NPI_Taxonomy
	for {
		if err := dec.Decode(&n); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		return n
	}
	return n
}