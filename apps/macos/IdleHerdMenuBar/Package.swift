// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "IdleHerdMenuBar", platforms: [.macOS(.v13)],
    products: [.library(name: "IdleHerdCore", targets: ["IdleHerdCore"]), .executable(name: "IdleHerdMenuBar", targets: ["IdleHerdMenuBar"]), .executable(name: "ClientSmoke", targets: ["ClientSmoke"])],
    targets: [
        .target(name: "IdleHerdCore"),
        .executableTarget(name: "IdleHerdMenuBar", dependencies: ["IdleHerdCore"]),
        .executableTarget(name: "ClientSmoke", dependencies: ["IdleHerdCore"]),
        .testTarget(name: "IdleHerdCoreTests", dependencies: ["IdleHerdCore"]),
    ]
)
