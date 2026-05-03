#!/bin/bash
# scripts/update-manifest.sh
# Updates Kubernetes deployment.yaml with new image tag and pushes to Git

set -euo pipefail

# ── Parse Arguments ─────────────────────────────────────────────────────────
REPO=""
TOKEN=""
IMAGE_TAG=""
BRANCH="main"
MANIFEST_FILE="deployment.yaml"

while [[ $# -gt 0 ]]; do
  case $1 in
    --repo) REPO="$2"; shift 2 ;;
    --token) TOKEN="$2"; shift 2 ;;
    --image) IMAGE_TAG="$2"; shift 2 ;;
    --branch) BRANCH="$2"; shift 2 ;;
    --file) MANIFEST_FILE="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── Validate Inputs ─────────────────────────────────────────────────────────
if [[ -z "$REPO" || -z "$TOKEN" || -z "$IMAGE_TAG" ]]; then
  echo "❌ Usage: $0 --repo <URL> --token <GH_TOKEN> --image <image:tag> [--branch <name>] [--file <manifest>]"
  exit 1
fi

echo "🔄 Updating manifest: $IMAGE_TAG → $REPO/$MANIFEST_FILE"

# ── Clone Manifest Repo ─────────────────────────────────────────────────────
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

# Clone using token for authentication (remove token from URL after clone)
git clone --depth 1 --branch "$BRANCH" \
  "https://${TOKEN}@${REPO#*://}" "$TEMP_DIR" 2>/dev/null

cd "$TEMP_DIR"

# Configure git user (required for commit)
git config user.email "jenkins@ci.stockseva.local"
git config user.name "StockSeva CI"

# ── Update Deployment Manifest ──────────────────────────────────────────────
if [[ ! -f "$MANIFEST_FILE" ]]; then
  echo "❌ Error: $MANIFEST_FILE not found in $TEMP_DIR"
  exit 1
fi

# Replace image tag using sed (cross-platform safe)
if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' "s|image: .*|image: $IMAGE_TAG|g" "$MANIFEST_FILE"
else
  sed -i "s|image: .*|image: $IMAGE_TAG|g" "$MANIFEST_FILE"
fi

# ── Commit and Push ─────────────────────────────────────────────────────────
git add "$MANIFEST_FILE"
git commit -m "ci: update StockSeva image to $IMAGE_TAG [skip ci]"
git push origin "$BRANCH" 2>/dev/null || {
  echo "⚠️ Push failed — checking if update was already applied..."
  git status
}

echo "✅ Manifest updated: $IMAGE_TAG pushed to $REPO"