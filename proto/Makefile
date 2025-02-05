protofiles = kvalchemy/*/*/*.proto

gen-dir:
	@mkdir -p gen/go

## compile all proto file
gen-grpc: gen-dir
	@for proto in $(protofiles) ; do \
  		protoc $$proto -I=. \
  		--go_out=gen/go --go_opt=paths=source_relative \
  		--go-grpc_out=gen/go --go-grpc_opt=paths=source_relative; \
    done

deps:
	go get -u google.golang.org/protobuf/cmd/protoc-gen-go
	go get -u google.golang.org/grpc/cmd/protoc-gen-go-grpc
