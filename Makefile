.PHONY: test lint fmt build

test:
	cd rust && cargo test --workspace
	cd js && pnpm test

lint:
	cd rust && cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings
	cd js && pnpm run lint && pnpm run check

fmt:
	cd rust && cargo fmt --all
	cd js && pnpm run format

build:
	cd rust && cargo build --workspace
	cd js && pnpm run build
