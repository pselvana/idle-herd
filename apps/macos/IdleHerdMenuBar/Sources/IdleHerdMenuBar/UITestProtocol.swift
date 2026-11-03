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
            let telemetry = ProcessInfo.processInfo.environment["IDLE_HERD_UI_SCENARIO"] == "telemetry"
            if !telemetry && (2...4).contains(Self.statusRequests) { code = 503 }
            body = """
            {"version":1,"mode":{"mode":"\(Self.mode)"},"slots":{"total":4,"occupied":2,"running":2,"reserved":0,"quarantined":0},"idle":{"eligible":false,"reason":"owner_active"},"resources":{"cpu_percent":11.44067798499703,"memory_available_gb":22.748971008,"memory_total_gb":64,"on_ac":true,"battery_percent":100,"thermal_state":"unknown","source":"macos_iokit","observed_at":\(Date().timeIntervalSince1970)},"backend":{"state":"offline","models_loaded":[]},"peers":{"known":0,"reachable":0,"stale":0,"observed_available_slots":0},"jobs":{"queued":0},"waiters":0,"worker_leases":[],"job_history":[],"transfers":[],"model_inventory":[],"peer_details":[{"worker_id":"this-mac","local":true,"reachable":true,"backend":"ollama","platform":"macOS arm64","models":[]}]}
            """
        } else if path.hasSuffix("/mode"), request.httpMethod == "PUT" {
            if let data = bodyData(), let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any], let mode = json["mode"] as? String { Self.mode = mode }
        } else if path.hasSuffix("/jobs") {
            body = "{\"jobs\":[],\"worker_leases\":[]}"
        } else if path.hasSuffix("/inventory") {
            body = "{\"models\":[]}"
        } else if path.hasSuffix("/transfers") {
            body = "{\"transfers\":[]}"
        } else if path.hasSuffix("/events") {
            code = 503
        }
        let response = HTTPURLResponse(url: request.url!, statusCode: code, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: Data(body.utf8))
        client?.urlProtocolDidFinishLoading(self)
    }
    private func bodyData() -> Data? {
        if let body = request.httpBody { return body }
        guard let stream = request.httpBodyStream else { return nil }
        stream.open()
        defer { stream.close() }
        var data = Data()
        var buffer = [UInt8](repeating: 0, count: 4096)
        while stream.hasBytesAvailable {
            let count = stream.read(&buffer, maxLength: buffer.count)
            if count <= 0 { break }
            data.append(contentsOf: buffer.prefix(count))
        }
        return data
    }
    override func stopLoading() {}
}
#endif
