// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "WebSocketClient",
    platforms: [
        .iOS(.v17),
        .macOS(.v14),
        .tvOS(.v17),
        .watchOS(.v10),
        .visionOS(.v1),
    ],
    products: [
        .library(name: "WebSocketClient", targets: ["WebSocketClient"]),
    ],
    targets: [
        .target(name: "WebSocketClient"),
        .testTarget(name: "WebSocketClientTests", dependencies: ["WebSocketClient"]),
    ]
)
