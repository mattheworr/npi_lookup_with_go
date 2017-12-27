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

		for _, prefix := range prefixes {
			res[prefix] = []NPI_Taxonomy{}
			err = db.View(func(tx *bolt.Tx) error {
				if len(prefix) == 10 {
					b := tx.Bucket([]byte("DB")).Bucket([]byte("NPI"))
					v := b.Get([]byte(prefix))
					n := decodeV(v)
					res[prefix] = append(res[prefix], n...)
				} else {
					b := tx.Bucket([]byte("DB")).Bucket([]byte("Taxonomy"))
					taxList := decodeTax(b.Get([]byte(prefix)))
					for _, tax := range taxList{
						fmt.Println(tax)
						c := tx.Bucket([]byte("DB")).Bucket([]byte("NPI"))
						v := c.Get([]byte(tax))
						n := decodeV(v)
						res[prefix] = append(res[prefix], n...)
					}
				}
				return nil
			})
		}

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

func decodeTax(jsonStream []byte) []string {
	dec := json.NewDecoder(strings.NewReader(string(jsonStream)))
	var n []string
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

func decodeV(jsonStream []byte) []NPI_Taxonomy {
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