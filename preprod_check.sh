#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "[preprod] repo: $ROOT_DIR"
echo

if ! command -v cargo >/dev/null 2>&1; then
  echo "[preprod] ERROR: cargo not found in PATH" >&2
  exit 1
fi

echo "[preprod] 0) Tooling"
rustup component add clippy >/dev/null
echo "OK"
echo

echo "[preprod] 1) Clippy (workspace, all targets, deny warnings)"
cargo clippy --workspace --all-targets -- -D warnings
echo "OK"
echo

echo "[preprod] 2) Unit/Integration tests (workspace)"
cargo test --workspace --tests -- --nocapture
echo "OK"
echo

echo "[preprod] 3) Doc tests (workspace)"
cargo test --workspace --doc -- --nocapture
echo "OK"
echo

echo "[preprod] 4) Preflight (DB connectivity + DDL/DML + streaming semantics)"
export MYSQL_URL="${MYSQL_URL:-mysql://root:Kephi520!@127.0.0.1:3306/Salary}"
export PGSQL_URL="${PGSQL_URL:-postgresql://root:Kephi520!@localhost:5432/Salary}"
export MSSQL_URL="${MSSQL_URL:-server=tcp:localhost,1433;user=sa;password=Kephi520!;database=Salary;Encrypt=DANGER_PLAINTEXT;TrustServerCertificate=true}"
export DB_PREFLIGHT_TIMEOUT_SECS="${DB_PREFLIGHT_TIMEOUT_SECS:-5}"
export DB_PREFLIGHT_ITERS="${DB_PREFLIGHT_ITERS:-50}"
cargo test --test preflight -- --ignored --nocapture
echo "OK"
echo

echo "[preprod] 5) Stress/bench (ignored tests in workspace)"
echo "[preprod] Tip: this may take time and will hit your databases."
cargo test --workspace -- --ignored --nocapture
echo "OK"
echo

echo "[preprod] DONE"
