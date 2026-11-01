#if DEBUG
import Foundation

/// In-process mock is compiled into debug builds only and never contacts a daemon.
final class UITestProtocol: URLProtocol {
    private static let lock = NSLock()
    private static var statusRequests = 0
    private static var mode = "auto"
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        Self.lock.lock()
        defer { Self.lock.unlock() }
        let path = request.url?.path ?? ""
        var code = 200
        var body = "{}"
        if path.hasSuffix("/status") {
            Self.statusRequests += 1
            if (2...4).contains(Self.statusRequests) { code = 503 }
            body = """
            {"version":1,"mode":{"mode":"\(Self.mode)"},"slots":{"total":4,"occupied":2,"running":2},"idle":{"eligible":true,"reason":null}}
            """
        } else if path.hasSuffix("/mode"), request.httpMethod == "PUT" {
            if let data = request.httpBody, let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any], let mode = json["mode"] as? String { Self.mode = mode }
        } else if path.hasSuffix("/jobs") {
            body = "{\"jobs\":[],\"worker_leases\":[]}"
        } else if path.hasSuffix("/events") {
            code = 503
        }
        let response = HTTPURLResponse(url: request.url!, statusCode: code, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: Data(body.utf8))
        client?.urlProtocolDidFinishLoading(self)
    }
    override func stopLoading() {}
}
#endif
