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
    func client() -> NodeClient {
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [MockProtocol.self]
        return NodeClient(session: URLSession(configuration: config), token: { "test-token" })
    }
    func testModeSendsAuthenticatedDuration() async throws {
        MockProtocol.handler = { request in
            XCTAssertEqual(request.httpMethod, "PUT")
            XCTAssertEqual(request.value(forHTTPHeaderField: "Authorization"), "Bearer test-token")
            let body = try XCTUnwrap(request.httpBody)
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
}
