.PHONY: dev prod test testbench build ordering compute hardhat-deploy deploy-contract build-abi emit-logs fetch-logs fetch-hardhat-logs

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

ordering:
	go run -tags=dev ./cmd/ordering

compute:
	go run -tags=dev ./cmd/indexer

hardhat-deploy:
	yarn deploy

deploy-contract: hardhat-deploy

hardhat-node:
	yarn hardhat node

build-abi:
	yarn hardhat compile

emit-logs:
	CONTRACT_ADDRESS=0x5FbDB2315678afecb367f032d93F642f64180aa3 \
	yarn hardhat run scripts/emit_logs.ts --network localhost

emit-logs-loop:
	CONTRACT_ADDRESS=0x5fbdb2315678afecb367f032d93f642f64180aa3 \
	EMIT_INTERVAL_MS=500 \
	EMIT_COUNT=0 \
	EMIT_AMOUNT=1 \
	EMIT_DATA="hello logs" \
	yarn hardhat run scripts/emit_loop.ts --network localhost

fetch-logs:
	curl "http://127.0.0.1:8080/logs?limit=50"

fetch-hardhat-logs:
	curl -s -X POST http://127.0.0.1:8545 \
	-H "Content-Type: application/json" \
	-d '{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":"0x0","toBlock":"latest","address":"0x5fbdb2315678afecb367f032d93f642f64180aa3"}]}'
