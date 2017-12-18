package main

import (
	"github.com/boltdb/bolt"
	"log"
	"encoding/json"
	"io"
	"net/http"
	"strings"
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
		querystring := r.URL.Query()
		prefixes := querystring["prefix"]
		res := make(map[string][]NPI_Taxonomy)
		for _, prefix := range prefixes {
			err = db.View(func(tx *bolt.Tx) error {
				c := tx.Bucket([]byte("DB")).Bucket([]byte("NPI")).Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					n := decodeV(string(v))
					if strings.HasPrefix(n.Taxonomy, prefix) != false {
						res[prefix] = append(res[prefix], n)
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
	})

	log.Fatal(http.ListenAndServe(":3535", nil))
}

func decodeV(jsonStream string) NPI_Taxonomy {
	dec := json.NewDecoder(strings.NewReader(string(jsonStream)))
	var n NPI_Taxonomy
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