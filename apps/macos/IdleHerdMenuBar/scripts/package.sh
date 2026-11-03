#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if ! xcodebuild -version >/dev/null 2>&1; then
    echo "error: Full Xcode is required to build this SwiftUI app; Command Line Tools alone do not include the SwiftUI macro plugin." >&2
    echo "Install Xcode, then select it with: sudo xcode-select --switch /Applications/Xcode.app/Contents/Developer" >&2
    exit 1
fi
swift build -c release
app="${1:-build/Idle-Herd.app}"
mkdir -p "$app/Contents/MacOS"
cp .build/release/IdleHerdMenuBar "$app/Contents/MacOS/IdleHerdMenuBar"
cat > "$app/Contents/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleName</key><string>Idle-Herd</string>
<key>CFBundleDisplayName</key><string>Idle-Herd</string>
<key>CFBundleExecutable</key><string>IdleHerdMenuBar</string>
<key>CFBundleIdentifier</key><string>org.idle-herd.menubar</string>
<key>CFBundlePackageType</key><string>APPL</string>
<key>CFBundleShortVersionString</key><string>0.1.0</string>
<key>CFBundleVersion</key><string>1</string>
<key>LSMinimumSystemVersion</key><string>13.0</string>
<key>LSUIElement</key><true/>
<key>NSAppTransportSecurity</key><dict><key>NSAllowsLocalNetworking</key><true/></dict>
</dict></plist>
PLIST
printf 'Created %s\n' "$app"
