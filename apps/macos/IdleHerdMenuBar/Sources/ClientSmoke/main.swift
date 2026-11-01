import Foundation
import IdleHerdCore

final class Mock: URLProtocol {
    static var calls = 0
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
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
        print("Mock credential validation, URL restrictions, snapshot recovery, and authenticated cancellation passed")
    }
}
