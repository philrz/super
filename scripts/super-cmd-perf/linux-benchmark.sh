#!/bin/bash
set -euo pipefail

sudo apt update
sudo apt -y upgrade
sudo apt -y install make unzip cargo hyperfine

# Prepare local SSD for best I/O performance
sudo fdisk -l /dev/nvme1n1
sudo mkfs.ext4 -E discard -F /dev/nvme1n1
sudo mount /dev/nvme1n1 /mnt
sudo chown ubuntu:ubuntu /mnt
sudo chmod 777 /mnt
echo 'export TMPDIR="/mnt/tmpdir"' >> "$HOME"/.profile
mkdir /mnt/tmpdir

# Install ClickHouse
if ! command -v clickhouse-client > /dev/null 2>&1; then
  sudo apt-get install -y apt-transport-https ca-certificates curl gnupg
  curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | sudo gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg
  echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | sudo tee \
      /etc/apt/sources.list.d/clickhouse.list
  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y clickhouse-client
fi

# Install DuckDB
if ! command -v duckdb > /dev/null 2>&1; then
  wget https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip
  unzip duckdb_cli-linux-amd64.zip
  sudo mv duckdb /usr/local/bin
fi

# Install Rust
if ! command -v cargo > /dev/null 2>&1; then
  wget https://static.rust-lang.org/dist/rust-1.82.0-x86_64-unknown-linux-gnu.tar.xz
  tar xf rust-1.82.0-x86_64-unknown-linux-gnu.tar.xz
  sudo rust-1.82.0-x86_64-unknown-linux-gnu/install.sh
  # shellcheck disable=SC2016
  echo 'export PATH="$PATH:$HOME/.cargo/bin"' >> "$HOME"/.profile
fi

# Install DataFusion CLI
if ! command -v datafusion-cli > /dev/null 2>&1; then
  cargo install datafusion-cli
fi

# Install Go
if ! command -v go > /dev/null 2>&1; then
  wget https://go.dev/dl/go1.23.3.linux-amd64.tar.gz
  rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.23.3.linux-amd64.tar.gz
  # shellcheck disable=SC2016
  echo 'export PATH="$PATH:/usr/local/go/bin:$HOME/go/bin"' >> "$HOME"/.profile
  source "$HOME"/.profile
fi

# Install SuperDB
if ! command -v super > /dev/null 2>&1; then
  git clone -b super-cmd-perf https://github.com/brimdata/super.git
  cd super
  make install
fi

cd scripts/super-cmd-perf
rundir="$(date +%F_%T)"
mkdir "$rundir"

# Prepare the test data
./prep-data.sh "$rundir" | tee "$rundir/runlog.txt" 2>&1

# Run the queries and generate the summary report
./run-queries.sh "$rundir" | tee -a "$rundir/runlog.txt" 2>&1
