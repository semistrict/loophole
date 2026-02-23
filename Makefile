.PHONY: check fmt clippy test doc python-lint e2e

check: fmt clippy test doc python-lint

fmt:
	cargo fmt --check

clippy:
	cargo clippy --all-targets -- -D warnings

test:
	cargo nextest run

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps

python-lint:
	cd tests && uv run ruff check .
	cd tests && uv run ruff format --check .

e2e:
	docker compose build
	docker compose up -d
	docker compose exec -T e2e bash -c 'umount /mnt/ext4* /mnt/loophole* 2>/dev/null; losetup -D 2>/dev/null; rm -r /tmp/loophole-cache* 2>/dev/null; true'
	docker compose exec -T -w /tests e2e uv run pytest -xvs -n auto $(TEST)
