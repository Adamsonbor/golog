PROTO_DIR=./protos
PROTO_GEN_DIR=./gen
PROTO_FILES=$(shell find $(PROTO_DIR) -name "*.proto")

CONFIG_PATH=$(HOME)/.golog/

init:
	mkdir -p $(CONFIG_PATH)

proto-gen:
	protoc -I$(PROTO_DIR) --go_out=$(PROTO_GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

gencert: init $(CONFIG_PATH)/model.conf $(CONFIG_PATH)/policy.csv
	cfssl gencert \
		-initca test/ca-csr.json | cfssljson -bare ca
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		test/client-csr.json | cfssljson -bare client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="root" \
		test/client-csr.json | cfssljson -bare root-client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobody" \
		test/client-csr.json | cfssljson -bare nobody-client

	mv *.pem *.csr $(CONFIG_PATH)

$(CONFIG_PATH)/model.conf:
	cp test/model.conf $(CONFIG_PATH)

$(CONFIG_PATH)/policy.csv:
	cp test/policy.csv $(CONFIG_PATH)

.PHONY: init proto-gen gencert
