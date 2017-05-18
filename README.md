# Mr.T [![GoDoc](https://godoc.org/github.com/itsmontoya/mrT?status.svg)](https://godoc.org/github.com/itsmontoya/mrT) ![Status](https://img.shields.io/badge/status-alpha-red.svg) [![Go Report Card](https://goreportcard.com/badge/github.com/itsmontoya/mrT)](https://goreportcard.com/report/github.com/itsmontoya/mrT)
Mr.T (Mr. Transaction) is a database persistence layer. The goal of this library is to provide a simple and elegant way for a database to manage it's data file persistence. Some of the core features:
- Key/Value support*
- Thread-safe transactions
- ACID-compliant safety for database actions

**To support multi-level maps, please use concatinated strings to represent buckets*

## Usage
For usage examples, please see the examples directory OR see direct links below:
- [MapDB](https://github.com/itsmontoya/mrT/tree/master/examples/mapDB)