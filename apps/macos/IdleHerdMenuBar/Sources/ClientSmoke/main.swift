import Foundation
import IdleHerdCore

final class Mock: URLProtocol {
    static var calls = 0
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        if request.url?.path.hasPrefix("/v1/models/transfers") == true {
            precondition(request.httpMethod == "POST")
            precondition(request.value(forHTTPHeaderField: "Authorization") == "Bearer mock")
            if request.url?.path == "/v1/models/transfers" || request.url?.path.hasSuffix("/retry") == true {
                let data: Data
                if let direct = request.httpBody { data = direct }
                else if let stream = request.httpBodyStream {
                    stream.open(); defer { stream.close() }
                    var bytes = [UInt8](repeating: 0, count: 4096); var collected = Data()
                    while stream.hasBytesAvailable { let count = stream.read(&bytes, maxLength: bytes.count); if count <= 0 { break }; collected.append(contentsOf: bytes.prefix(count)) }
                    data = collected
                } else { fatalError("missing request body") }
                let body = try! JSONSerialization.jsonObject(with: data) as! [String: Any]
                precondition(body["permission"] as? Bool == true && body["license_accepted"] as? Bool == true)
                precondition(body["manifest_digest"] as? String == String(repeating: "a", count: 64))
            }
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: Data("{}".utf8))
            client?.urlProtocolDidFinishLoading(self)
            return
        }
        if request.url?.path == "/v1/node/jobs/job-123/cancel" {
            precondition(request.httpMethod == "POST")
            precondition(request.value(forHTTPHeaderField: "Authorization") == "Bearer mock")
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: Data("{}".utf8))
            client?.urlProtocolDidFinishLoading(self)
            return
        }
        Self.calls += 1
        precondition(request.value(forHTTPHeaderField: "Authorization") == "Bearer mock")
        let status = Self.calls == 2 ? 503 : 200
        let response = HTTPURLResponse(url: request.url!, statusCode: status, httpVersion: nil, headerFields: nil)!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: Data("{\"version\":1,\"mode\":{\"mode\":\"auto\"},\"slots\":{\"running\":2,\"occupied\":2,\"total\":4},\"idle\":{\"eligible\":true,\"reason\":null}}".utf8))
        client?.urlProtocolDidFinishLoading(self)
    }
    override func stopLoading() {}
}

@main struct Smoke {
    static func main() async throws {
        let token = String(repeating: "a", count: 32)
        let valid = Data("{\"version\":1,\"control_token\":\"\(token)\"}".utf8)
        guard try CredentialStore.validateFileData(valid) == token else { fatalError("valid credential rejected") }
        for text in ["{\"version\":2,\"control_token\":\"\(token)\"}", "{\"version\":1,\"control_token\":\"short\"}", "{\"version\":1,\"control_token\":\"\(token)!\"}"] {
            do { _ = try CredentialStore.validateFileData(Data(text.utf8)); fatalError("invalid credential accepted") } catch {}
        }
        for value in ["http://example.com", "http://user@localhost:11435", "http://localhost:11435/?x=1", "http://localhost:11435/#x"] {
            let unsafe = NodeClient(baseURL: URL(string: value)!, token: { "mock" })
            do { _ = try unsafe.request("v1/node/status"); fatalError("unsafe URL accepted") } catch {}
        }
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [Mock.self]
        let client = NodeClient(session: URLSession(configuration: config), token: { "mock" })
        let first = try NodeSnapshot.decode(try await client.fetch("v1/node/status"))
        guard first.slots.running == 2 else { fatalError("initial snapshot invalid") }
        do { _ = try await client.fetch("v1/node/status"); fatalError("failure not detected") } catch {}
        let snapshot = try await client.fetch("v1/node/status")
        guard try NodeSnapshot.decode(snapshot).mode.mode == "auto" else { fatalError("snapshot not recovered") }
        try await client.cancelJob("job-123")
        do { try await client.cancelJob("../unsafe"); fatalError("unsafe job id accepted") } catch {}
        let digest = String(repeating: "a", count: 64)
        do { try await client.startTransfer(digest: digest, manifestDigest: digest, sourceWorkerID: "peer", licenseAccepted: false); fatalError("missing license accepted") } catch {}
        do { try await client.transferAction(digest: "../invalid", action: "pause"); fatalError("unsafe digest accepted") } catch {}
        try await client.startTransfer(digest: digest, manifestDigest: digest, sourceWorkerID: "peer", licenseAccepted: true)
        try await client.transferAction(digest: digest, action: "pause")
        try await client.startTransfer(digest: digest, manifestDigest: digest, sourceWorkerID: "peer", licenseAccepted: true, retry: true)
        let event = Data("{\"type\":\"transfer\",\"data\":{\"version\":1,\"mode\":{\"mode\":\"auto\"},\"slots\":{\"running\":0,\"occupied\":0,\"total\":1},\"idle\":{\"eligible\":true,\"reason\":null},\"transfers\":[{\"digest\":\"d\",\"model_id\":\"m\",\"state\":\"transferring\",\"completed_bytes\":42,\"total_bytes\":100}]}}".utf8)
        let live = try NodeSnapshot.decode(NodeClient.eventSnapshot(event))
        guard live.transfers?.first?.completed_bytes == 42 else { fatalError("transfer event lost") }
        print("Mock transfer events, retry consent, authenticated actions, credential validation, URL restrictions, snapshot recovery, and authenticated cancellation passed")
    }
}
