module github.com/ankur-anand/unisondb/notifiers/zeromq

go 1.24.0

require (
	github.com/ankur-anand/unisondb v0.0.0
	github.com/pebbe/zmq4 v1.4.0
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/flatbuffers v25.9.23+incompatible // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/ankur-anand/unisondb => ./../../..
