#!/usr/bin/env sh
# slipway installer - https://github.com/GerhardOfRivia/slipway
# Usage: curl -fsSL https://raw.githubusercontent.com/GerhardOfRivia/slipway/refs/heads/main/install.sh | sh

set -e

PACKAGE="slipway"
REPO="GerhardOfRivia/${PACKAGE}"
INSTALL_DIR="${SLIPWAY_INSTALL_DIR:-$HOME/.local/bin}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info() {
    printf "${GREEN}[INFO]${NC} %s\n" "$1"
}

warn() {
    printf "${YELLOW}[WARN]${NC} %s\n" "$1"
}

error() {
    printf "${RED}[ERROR]${NC} %s\n" "$1"
    exit 1
}

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Linux*)  OS="linux";;
        *)       error "Unsupported operating system: $(uname -s)";;
    esac
}

# Detect architecture
detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64)  ARCH="x86_64";;
        arm64|aarch64) ARCH="aarch64";;
        *)             error "Unsupported architecture: $(uname -m)";;
    esac
}

# Get latest release version
# Primary: parse the 302 redirect on /releases/latest (no API call, no rate limit).
# Fallback: the GitHub REST API (subject to 60 req/hour anonymous limit).
get_latest_version() {
    # Try the web redirect first — does not count against the API rate limit.
    VERSION=$(curl -sI "https://github.com/${REPO}/releases/latest" \
        | grep -i '^location:' \
        | sed -E 's|.*/tag/([^[:space:]]+).*|\1|' \
        | tr -d '\r')

    # Fallback to the REST API if the redirect didn't yield a tag.
    if [ -z "$VERSION" ]; then
        warn "Redirect lookup failed, falling back to GitHub API..."
        VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
            | grep '"tag_name":' \
            | sed -E 's/.*"([^"]+)".*/\1/')
    fi

    if [ -z "$VERSION" ]; then
        error "Failed to get latest version (GitHub API may be rate-limited; set SLIPWAY_VERSION=vX.Y.Z to pin)"
    fi
}

# Build target triple
get_target() {
    case "$OS" in
        linux)
            case "$ARCH" in
                x86_64)  TARGET="linux-amd64";;
                aarch64) TARGET="linux-arm64";;
            esac
            ;;
    esac
}

# Download and install
install() {
    info "Detected: $OS $ARCH"
    info "Target: $TARGET"
    info "Version: $VERSION"

    CHECKSUMS_URL="https://github.com/${REPO}/releases/download/${VERSION}/checksums.txt"
    TEMP_DIR=$(mktemp -d)
    CHECKSUMS="${TEMP_DIR}/checksums.txt"

    if [ "${SLIPWAY_SKIP_CHECKSUM:-0}" = "1" ]; then
        warn "SLIPWAY_SKIP_CHECKSUM=1 set — SKIPPING checksum verification (NOT RECOMMENDED)"
    else
        info "Downloading checksums..."
        if ! curl -fsSL "$CHECKSUMS_URL" -o "$CHECKSUMS"; then
            error "Failed to download checksums.txt — refusing to install unverified binary (set SLIPWAY_SKIP_CHECKSUM=1 to bypass at your own risk)"
        fi
    fi

    ASSET_NAME="${PACKAGE}-${TARGET}.tar"
    DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET_NAME}"
    ARCHIVE="${TEMP_DIR}/${ASSET_NAME}"

    info "Downloading from: $DOWNLOAD_URL"
    if ! curl -fsSL "$DOWNLOAD_URL" -o "$ARCHIVE"; then
        error "Failed to download ${PACKAGE}"
    fi

    if [ "${SLIPWAY_SKIP_CHECKSUM:-0}" != "1" ]; then
        info "Verifying SHA-256 checksum for ${PACKAGE}..."
        EXPECTED=$(awk -v asset="$ASSET_NAME" '$2 == asset || $2 == "release/" asset { print $1; exit }' "$CHECKSUMS")
        if [ -z "$EXPECTED" ]; then
            error "checksum for ${ASSET_NAME} not found in checksums.txt — refusing to install"
        fi
        # Prefer sha256sum, with shasum as a portable fallback.
        if command -v sha256sum >/dev/null 2>&1; then
            ACTUAL=$(sha256sum "$ARCHIVE" | awk '{print $1}')
        elif command -v shasum >/dev/null 2>&1; then
            ACTUAL=$(shasum -a 256 "$ARCHIVE" | awk '{print $1}')
        else
            error "Neither sha256sum nor shasum available — cannot verify checksum"
        fi
        if [ "$EXPECTED" != "$ACTUAL" ]; then
            error "checksum mismatch for ${ASSET_NAME}! expected=${EXPECTED} actual=${ACTUAL} — refusing to install"
        fi
        info "Checksum verified for ${PACKAGE}."
    fi

    tar -xvf $ARCHIVE -C $TEMP_DIR

    mkdir -p "$INSTALL_DIR"
    for BINARY_NAME in slipwayd slipway; do
        mv "${TEMP_DIR}/${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"
        chmod +x "${INSTALL_DIR}/${BINARY_NAME}"
        info "Successfully installed ${BINARY_NAME} to ${INSTALL_DIR}/${BINARY_NAME}"
    done

    # Cleanup
    rm -rf "$TEMP_DIR"
}

# Verify installation
verify() {
    MISSING_FROM_PATH=0
    for BINARY_NAME in slipwayd slipway; do
        INSTALLED_BIN="${INSTALL_DIR}/${BINARY_NAME}"
        if [ -x "$INSTALLED_BIN" ]; then
            info "Verification: $("$INSTALLED_BIN" --version)"
        else
            error "Binary not found at expected location: $INSTALLED_BIN"
        fi
        if ! command -v "$BINARY_NAME" >/dev/null 2>&1; then
            MISSING_FROM_PATH=1
        fi
    done
    if [ "$MISSING_FROM_PATH" -eq 1 ]; then
        warn "Binaries installed but not all are in PATH. Add to your shell profile:"
        warn "  export PATH=\"\$HOME/.local/bin:\$PATH\""
    fi
}

main() {
    info "Installing slipway and slipwayd..."

    detect_os
    detect_arch
    get_target
    if [ -n "$SLIPWAY_VERSION" ]; then
        VERSION="$SLIPWAY_VERSION"
        info "Using pinned version from SLIPWAY_VERSION: $VERSION"
    else
        get_latest_version
    fi
    install
    verify

    echo ""
    info "Installation complete! Run 'slipway --help' to get started."
}

main
