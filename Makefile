.PHONY: test lint fmt build

test:
	cd rust && cargo test --workspace
	cd js && npm test

lint:
	cd rust && cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings
	cd js && npm run lint && npm run check

fmt:
	cd rust && cargo fmt --all
	cd js && npm run format

build:
	cd rust && cargo build --workspace
	cd js && npm run build
