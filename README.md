# npi_lookup_with_go
The specialties for medical service providers are indicated by taxonomy codes1, and each provider has one or more taxonomies.
Given a list of taxonomy code prefixes, this app can return the NPI numbers of all the providers who have at least one taxonomy that matches the prefix.

## Input
Once the data is loaded, the service responds to an HTTP GET request of the form `http://localhost:3535/taxonomy?prefix=PREFIX1&prefix=PREFIX2`. It supports queries with multiple prefixes at a time, and no restriction on the length of the prefix.

For example:
```
http://localhost:3535/taxonomy?prefix=207P&prefix=207ND0900X&prefix=2084P08&prefix=12345
```

## Output
The service returns a JSON object in which each key is one of the requested prefixes, and the value is a list of objects where each object contains a matched NPI number and the matching taxonomy for that prefix.

If a prefix does not match any providers, the value for that key will be an empty list. If a single NPI number has multiple taxonomies that match a prefix, each of these results will be in a separate object in the list, as in the first entry in the below example.

This example output is meant to indicate the format given the above example query, but the results when run on actual data will be different.

```    
  {
"207P": [
         { "NPI": 1037402843, "Taxonomy": "207P259200" },
         { "NPI": 1037402843, "Taxonomy": "207P25000X" },
         { "NPI": 4928502831, "Taxonomy": "207P12400X" }
     ],
     "207ND0900X": [ { "NPI": 9306820482, "Taxonomy": "207ND0900X" } ],
     "2084P08": [
         { "NPI": 2145025305, "Taxonomy": "2084P0800" },
         { "NPI": 2950383053, "Taxonomy": "2084P084X" }
     ],
"12345": [] }
```

## Data sources and libraries:
*Source Dataset*

https://storage.googleapis.com/cv-client-export/all_taxonomies.csv.gz2

The ZIP file downloaded from this website contains a CSV with all the data needed for the project. You can assume that the format of the file and the number of columns will not change. The specific columns of interest here are the npi column and `the healthcare_provider_taxonomy_code`.

*BoltDB*

https://github.com/boltdb/bolt3
