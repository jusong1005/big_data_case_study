#!/usr/bin/env bash
set -euo pipefail

NODE_VERSION="v20.19.5"
NODE_DIR="/home/hadoop/export/servers/node-${NODE_VERSION}-linux-x64"
NODE_ARCHIVE="/home/hadoop/export/softerware/node-${NODE_VERSION}-linux-x64.tar.xz"

mkdir -p /home/hadoop/export/softerware /home/hadoop/export/servers

if [ ! -x "${NODE_DIR}/bin/node" ]; then
  if [ ! -f "${NODE_ARCHIVE}" ]; then
    curl -L --fail --retry 3 -o "${NODE_ARCHIVE}" "https://nodejs.org/dist/${NODE_VERSION}/node-${NODE_VERSION}-linux-x64.tar.xz"
  fi
  tar -xf "${NODE_ARCHIVE}" -C /home/hadoop/export/servers
fi

export PATH="${NODE_DIR}/bin:${PATH}"

cd "$(dirname "${BASH_SOURCE[0]}")"
node -v
npm -v
npm install
npm run build
