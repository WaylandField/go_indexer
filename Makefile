.PHONY: dev prod test testbench build hardhat-deploy deploy-contract build-abi emit-logs

dev:
	go run -tags=dev ./cmd/indexer

prod:
	go run ./cmd/indexer

test:
	go test ./...

testbench:
	go test -bench ./...

build:
	go build -o bin/indexer ./cmd/indexer

hardhat-deploy:
	yarn deploy

deploy-contract: hardhat-deploy

hardhat-node:
	yarn hardhat node

build-abi:
	yarn hardhat compile

emit-logs:
	CONTRACT_ADDRESS=0x5fbdb2315678afecb367f032d93f642f64180aa3 \
	yarn hardhat run scripts/emit_logs.ts --network localhost

emit-logs-loop:
	CONTRACT_ADDRESS=0x5fbdb2315678afecb367f032d93f642f64180aa3 \
	EMIT_INTERVAL_MS=500 \
	EMIT_COUNT=0 \
	EMIT_AMOUNT=1 \
	EMIT_DATA="hello logs" \
	yarn hardhat run scripts/emit_loop.ts --network localhost
