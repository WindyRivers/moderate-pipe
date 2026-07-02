# Developer entrypoints. The Go toolchain in this environment lives under
# ~/goroot; export the paths before invoking make, or run the commands directly.

.PHONY: proto build test vet up down logs scale loadtest chaos-kill chaos-degrade tidy

# Regenerate gRPC stubs from proto/user.proto (needs protoc + the go plugins).
proto:
	protoc --proto_path=proto \
		--go_out=proto/gen --go_opt=paths=source_relative \
		--go-grpc_out=proto/gen --go-grpc_opt=paths=source_relative \
		proto/user.proto

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

tidy:
	go mod tidy

# Bring the whole stack up (2 review replicas by default).
up:
	docker compose up --build -d

# Scale the review consumer group to 3 instances.
scale:
	docker compose up --build -d --scale review-service=3

down:
	docker compose down -v

logs:
	docker compose logs -f

# Load test: sustained concurrent posting against the content service.
loadtest:
	./scripts/loadtest.sh

# Chaos: kill one review replica and watch the group rebalance.
chaos-kill:
	./scripts/chaos_kill_review.sh

# Chaos: take the user service down and watch degradation kick in.
chaos-degrade:
	./scripts/chaos_degrade_user.sh
