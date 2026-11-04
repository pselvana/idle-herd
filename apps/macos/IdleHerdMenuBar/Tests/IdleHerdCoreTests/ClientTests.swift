import XCTest
@testable import IdleHerdCore

final class MockProtocol: URLProtocol {
    static var handler: ((URLRequest) throws -> (Int, Data))!
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        do {
            let (status, data) = try Self.handler(request)
            client?.urlProtocol(self, didReceive: HTTPURLResponse(url: request.url!, statusCode: status, httpVersion: nil, headerFields: nil)!, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch { client?.urlProtocol(self, didFailWithError: error) }
    }
    override func stopLoading() {}
}

final class ClientTests: XCTestCase {
    func testRejectedCredentialDestinationsNeverReadTheSecret() {
        for address in ["https://localhost:11435", "http://192.168.1.2:11435",
                        "http://localhost.example:11435", "http://user:pass@localhost:11435",
                        "http://127.0.0.1:11435/?next=remote", "http://localhost:11435/#fragment"] {
            var readSecret = false
            let unsafe = NodeClient(baseURL: URL(string: address)!, token: {
                readSecret = true
                return "secret"
            })
            XCTAssertThrowsError(try unsafe.request("v1/node/status"), address)
            XCTAssertFalse(readSecret, address)
        }
    }

    func testCredentialFileBoundsAndCharacterSet() throws {
        func data(_ token: String, version: Int = 1) throws -> Data {
            try JSONSerialization.data(withJSONObject: ["version": version, "control_token": token])
        }
        for count in [32, 256] {
            let token = String(repeating: "a", count: count - 2) + "_-"
            XCTAssertEqual(try CredentialStore.validateFileData(data(token)), token)
        }
        for token in [String(repeating: "a", count: 31), String(repeating: "a", count: 257),
                      String(repeating: "a", count: 32) + "\n", String(repeating: "é", count: 32)] {
            XCTAssertThrowsError(try CredentialStore.validateFileData(data(token)))
        }
        XCTAssertThrowsError(try CredentialStore.validateFileData(data(String(repeating: "a", count: 32), version: 2)))
        XCTAssertThrowsError(try CredentialStore.validateFileData(Data("{}".utf8)))
    }

    func testRetrySendsCurrentSourceAndOriginalManifestWithExplicitConsent() async throws {
        let digest = String(repeating: "a", count: 64)
        let manifest = String(repeating: "b", count: 64)
        MockProtocol.handler = { request in
            XCTAssertEqual(request.url?.path, "/v1/models/transfers/\(digest)/retry")
            XCTAssertEqual(request.httpMethod, "POST")
            XCTAssertEqual(request.value(forHTTPHeaderField: "Authorization"), "Bearer test-token")
            let json = try XCTUnwrap(JSONSerialization.jsonObject(with: self.body(request)) as? [String: Any])
            XCTAssertEqual(json["digest"] as? String, digest)
            XCTAssertEqual(json["manifest_digest"] as? String, manifest)
            XCTAssertEqual(json["source_worker_id"] as? String, "alternate-peer")
            XCTAssertEqual(json["permission"] as? Bool, true)
            XCTAssertEqual(json["license_accepted"] as? Bool, true)
            return (200, Data("{}".utf8))
        }
        try await client().startTransfer(digest: digest, manifestDigest: manifest,
                                         sourceWorkerID: "alternate-peer", licenseAccepted: true, retry: true)
    }

    func testInvalidEventsCannotReplaceLastValidStatus() throws {
        for text in ["{}", "{\"data\":[]}", "{\"data\":{\"version\":2}}",
                     "{\"data\":{\"version\":1,\"mode\":{\"mode\":\"auto\"}}}"] {
            XCTAssertThrowsError(try NodeClient.eventSnapshot(Data(text.utf8)))
        }
        let text = """
        {"type":"status","future_field":true,"data":{"version":1,"mode":{"mode":"offline"},
        "slots":{"running":0,"occupied":0,"total":1},"idle":{"eligible":false},"new_field":42}}
        """
        let event = try NodeClient.eventSnapshot(Data(text.utf8))
        XCTAssertEqual(try NodeSnapshot.decode(event).mode.mode, "offline")
        XCTAssertEqual(event["new_field"] as? Int, 42)
    }

    func testPackageOffersDecodeWithoutLocalPaths() throws {
        let object: [String: Any] = [
            "model_id": "mlx-model:latest", "digest": String(repeating: "a", count: 64),
            "manifest_digest": String(repeating: "b", count: 64), "format": "mlx",
            "backend": "ollama", "local": false, "source_worker_id": "peer",
            "files": [["name": "config.json", "size_bytes": 42, "role": "file"],
                      ["name": "model.safetensors", "size_bytes": 5_000_000_000, "role": "file"]],
        ]
        let data = try JSONSerialization.data(withJSONObject: object)
        let item = try JSONDecoder().decode(ModelInventoryItem.self, from: data)
        XCTAssertEqual(item.formatLabel, "MLX / safetensors")
        XCTAssertEqual(item.files?.map(\.name), ["config.json", "model.safetensors"])
        XCTAssertEqual(item.files?.last?.size_bytes, 5_000_000_000)
        XCTAssertTrue(item.canCopy(from: "peer"))
    }

    func body(_ request: URLRequest) throws -> Data {
        if let data = request.httpBody { return data }
        let stream = try XCTUnwrap(request.httpBodyStream)
        stream.open(); defer { stream.close() }
        var buffer = [UInt8](repeating: 0, count: 4096); var data = Data()
        while stream.hasBytesAvailable {
            let count = stream.read(&buffer, maxLength: buffer.count)
            if count <= 0 { break }
            data.append(contentsOf: buffer.prefix(count))
        }
        return data
    }
    func client() -> NodeClient {
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [MockProtocol.self]
        return NodeClient(session: URLSession(configuration: config), token: { "test-token" })
    }
    func testModeSendsAuthenticatedDuration() async throws {
        MockProtocol.handler = { request in
            XCTAssertEqual(request.httpMethod, "PUT")
            XCTAssertEqual(request.value(forHTTPHeaderField: "Authorization"), "Bearer test-token")
            let body = try self.body(request)
            let json = try XCTUnwrap(JSONSerialization.jsonObject(with: body) as? [String: Any])
            XCTAssertEqual(json["mode"] as? String, "offline")
            XCTAssertEqual(json["duration_seconds"] as? Int, 3600)
            return (200, Data("{}".utf8))
        }
        try await client().setMode("offline", duration: 3600)
    }
    func testReconnectSnapshotAfterFailure() async throws {
        var calls = 0
        MockProtocol.handler = { _ in
            calls += 1
            return calls == 1 ? (503, Data()) : (200, Data("{\"mode\":{\"mode\":\"auto\"}}".utf8))
        }
        do { _ = try await client().fetch("v1/node/status"); XCTFail("expected disconnect") } catch {}
        let snapshot = try await client().fetch("v1/node/status")
        XCTAssertEqual((snapshot["mode"] as? [String: String])?["mode"], "auto")
    }
    func testRejectsNonLoopbackCredentialDestination() throws {
        let remote = NodeClient(baseURL: URL(string: "http://example.com")!, token: { "secret" })
        XCTAssertThrowsError(try remote.request("v1/node/status"))
    }
    func testTransferConsentAndAuthenticatedActions() async throws {
        let digest = String(repeating: "a", count: 64)
        var requests: [URLRequest] = []
        MockProtocol.handler = { request in requests.append(request); return (200, Data("{}".utf8)) }
        do { try await client().startTransfer(digest: digest, manifestDigest: digest, sourceWorkerID: "peer", licenseAccepted: false); XCTFail("consent required") } catch {}
        do { try await client().transferAction(digest: "../bad", action: "retry"); XCTFail("invalid digest") } catch {}
        XCTAssertTrue(requests.isEmpty)
        try await client().startTransfer(digest: digest, manifestDigest: digest, sourceWorkerID: "peer", licenseAccepted: true)
        try await client().transferAction(digest: digest, action: "pause")
        XCTAssertEqual(requests.count, 2)
        XCTAssertEqual(requests[0].value(forHTTPHeaderField: "Authorization"), "Bearer test-token")
        let body = try body(requests[0])
        let json = try XCTUnwrap(JSONSerialization.jsonObject(with: body) as? [String: Any])
        XCTAssertEqual(json["permission"] as? Bool, true)
        XCTAssertEqual(json["license_accepted"] as? Bool, true)
        XCTAssertEqual(json["source_worker_id"] as? String, "peer")
        XCTAssertEqual(json["manifest_digest"] as? String, digest)
        XCTAssertEqual(requests[1].url?.path, "/v1/models/transfers/\(digest)/pause")
    }
    func testSharingSnapshotsDecode() async throws {
        MockProtocol.handler = { request in
            if request.url!.path.hasSuffix("inventory") {
                return (200, Data("{\"models\":[{\"digest\":\"abc\",\"model_id\":\"m\",\"total_bytes\":100}]}".utf8))
            }
            return (200, Data("{\"transfers\":[{\"digest\":\"abc\",\"model_id\":\"m\",\"state\":\"failed\",\"completed_bytes\":20,\"total_bytes\":100,\"reason\":\"disk full\"}]}".utf8))
        }
        let inventory = try await client().inventory()
        let transfers = try await client().transfers()
        XCTAssertEqual(inventory.first?.total_bytes, 100)
        XCTAssertEqual(transfers.first?.reason, "disk full")
    }

    func testSelectedPeerCatalogUsesAuthenticatedEncodedQuery() async throws {
        MockProtocol.handler = { request in
            XCTAssertEqual(request.url?.host, "127.0.0.1")
            XCTAssertEqual(request.url?.path, "/v1/models/inventory")
            XCTAssertEqual(request.value(forHTTPHeaderField: "Authorization"), "Bearer test-token")
            let components = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)!
            XCTAssertEqual(components.queryItems, [URLQueryItem(name: "source_worker_id", value: "peer & /?")])
            return (200, Data("{\"models\":[]}".utf8))
        }
        let items = try await client().inventory(sourceWorkerID: "peer & /?")
        XCTAssertTrue(items.isEmpty)
        MockProtocol.handler = { _ in (503, Data()) }
        do { _ = try await client().inventory(sourceWorkerID: "peer"); XCTFail("expected catalog failure") } catch {}
    }

    func testCopySelectionExcludesLocalOtherPeerAndChangedRetryManifest() throws {
        let digest = String(repeating: "a", count: 64)
        let manifest = String(repeating: "b", count: 64)
        func item(source: String = "peer", local: Bool = false, revision: String? = nil) throws -> ModelInventoryItem {
            let object: [String: Any] = ["digest": digest, "model_id": "model", "source_worker_id": source,
                                         "local": local, "manifest_digest": revision ?? manifest]
            return try JSONDecoder().decode(ModelInventoryItem.self, from: JSONSerialization.data(withJSONObject: object))
        }
        let transferObject: [String: Any] = ["digest": digest, "manifest_digest": manifest,
                                            "state": "paused", "completed_bytes": 10, "total_bytes": 100]
        let transfer = try JSONDecoder().decode(ModelTransfer.self, from: JSONSerialization.data(withJSONObject: transferObject))
        XCTAssertTrue(try item().canCopy(from: "peer"))
        XCTAssertFalse(try item(local: true).canCopy(from: "peer"))
        XCTAssertFalse(try item(source: "other").canCopy(from: "peer"))
        XCTAssertFalse(try item(revision: "invalid").canCopy(from: "peer"))
        XCTAssertTrue(try item().canCopy(from: "peer", retrying: transfer))
        XCTAssertFalse(try item(revision: String(repeating: "c", count: 64)).canCopy(from: "peer", retrying: transfer))
        XCTAssertTrue(try item(source: "alternative").canCopy(from: "alternative", retrying: transfer))
    }

}
