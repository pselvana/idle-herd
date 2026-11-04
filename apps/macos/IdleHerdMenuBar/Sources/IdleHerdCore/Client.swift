import Foundation
import Security
import Darwin

public enum ClientError: LocalizedError {
    case credential, response(Int), invalidResponse
    public var errorDescription: String? {
        switch self {
        case .credential: return "Control credential unavailable. Import the daemon credential in Preferences."
        case .response(let code): return "Daemon returned HTTP \(code)."
        case .invalidResponse: return "The daemon returned an invalid response."
        }
    }
}

public enum CredentialStore {
    private static let service = "org.idle-herd.control"
    public static func read() throws -> String {
        let query: [String: Any] = [kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service, kSecAttrAccount as String: NSUserName(),
            kSecReturnData as String: true, kSecMatchLimit as String: kSecMatchLimitOne]
        var result: CFTypeRef?
        guard SecItemCopyMatching(query as CFDictionary, &result) == errSecSuccess,
              let data = result as? Data, let value = String(data: data, encoding: .utf8),
              !value.isEmpty else { throw ClientError.credential }
        return value
    }
    public static func save(_ token: String) throws {
        let query: [String: Any] = [kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service, kSecAttrAccount as String: NSUserName()]
        let data = Data(token.utf8)
        let status = SecItemUpdate(query as CFDictionary, [kSecValueData as String: data] as CFDictionary)
        if status == errSecItemNotFound {
            var item = query
            item[kSecValueData as String] = data
            item[kSecAttrAccessible as String] = kSecAttrAccessibleWhenUnlockedThisDeviceOnly
            guard SecItemAdd(item as CFDictionary, nil) == errSecSuccess else { throw ClientError.credential }
        } else if status != errSecSuccess { throw ClientError.credential }
    }
    private struct CredentialFile: Decodable { let version: Int; let control_token: String }
    public static func validateFileData(_ data: Data) throws -> String {
        let file = try JSONDecoder().decode(CredentialFile.self, from: data)
        let allowed = CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
        guard file.version == 1, (32...256).contains(file.control_token.utf8.count),
              file.control_token.unicodeScalars.allSatisfy({ allowed.contains($0) }) else { throw ClientError.credential }
        return file.control_token
    }
    // Refuse links, other owners and any group/other permissions before importing.
    public static func importFile(_ url: URL) throws {
        let fd = open(url.path, O_RDONLY | O_NOFOLLOW)
        guard fd >= 0 else { throw ClientError.credential }
        defer { close(fd) }
        var info = stat()
        guard fstat(fd, &info) == 0, (info.st_mode & S_IFMT) == S_IFREG,
              info.st_uid == getuid(), (info.st_mode & 0o077) == 0,
              info.st_size > 0, info.st_size <= 16384 else { throw ClientError.credential }
        let handle = FileHandle(fileDescriptor: fd, closeOnDealloc: false)
        let data = try handle.readToEnd() ?? Data()
        try save(validateFileData(data))
    }
}

private final class NoRedirect: NSObject, URLSessionTaskDelegate {
    func urlSession(_ session: URLSession, task: URLSessionTask,
                    willPerformHTTPRedirection response: HTTPURLResponse, newRequest request: URLRequest,
                    completionHandler: @escaping (URLRequest?) -> Void) {
        completionHandler(nil)
    }
}

public struct NodeClient {
    private static let noRedirect = NoRedirect()
    private static func makeSession() -> URLSession {
        let config = URLSessionConfiguration.ephemeral
        config.httpCookieStorage = nil
        config.httpShouldSetCookies = false
        config.urlCredentialStorage = nil
        return URLSession(configuration: config)
    }
    public let baseURL: URL
    public let session: URLSession
    public let token: () throws -> String
    public init(baseURL: URL = URL(string: "http://127.0.0.1:11435")!, session: URLSession? = nil,
                token: @escaping () throws -> String = CredentialStore.read) {
        self.baseURL = baseURL; self.session = session ?? Self.makeSession(); self.token = token
    }
    public func request(_ path: String, method: String = "GET", body: Data? = nil) throws -> URLRequest {
        guard baseURL.scheme == "http", ["127.0.0.1", "localhost", "::1"].contains(baseURL.host ?? ""),
              baseURL.user == nil, baseURL.password == nil, baseURL.query == nil, baseURL.fragment == nil else { throw ClientError.invalidResponse }
        var request = URLRequest(url: baseURL.appendingPathComponent(path))
        request.cachePolicy = .reloadIgnoringLocalCacheData
        request.httpMethod = method; request.httpBody = body
        request.setValue("Bearer \(try token())", forHTTPHeaderField: "Authorization")
        if body != nil { request.setValue("application/json", forHTTPHeaderField: "Content-Type") }
        request.timeoutInterval = 10
        return request
    }
    public func fetch(_ path: String) async throws -> [String: Any] {
        let (data, response) = try await session.data(for: request(path), delegate: Self.noRedirect)
        try check(response)
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else { throw ClientError.invalidResponse }
        return object
    }
    public func setMode(_ mode: String, duration: Int? = nil) async throws {
        var payload: [String: Any] = ["mode": mode]
        if let duration { payload["duration_seconds"] = duration }
        let body = try JSONSerialization.data(withJSONObject: payload)
        let (_, response) = try await session.data(for: request("v1/node/mode", method: "PUT", body: body), delegate: Self.noRedirect)
        try check(response)
    }
    public func updatePreferences(version: Int, values: [String: Any]) async throws {
        let payload: [String: Any] = ["expected_version": version, "values": values]
        let body = try JSONSerialization.data(withJSONObject: payload)
        let (_, response) = try await session.data(for: request("v1/node/preferences", method: "PATCH", body: body), delegate: Self.noRedirect)
        try check(response)
    }
    public func cancelJob(_ id: String) async throws {
        let allowed = CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
        guard !id.isEmpty, id.unicodeScalars.allSatisfy({ allowed.contains($0) }) else { throw ClientError.invalidResponse }
        let (_, response) = try await session.data(for: request("v1/node/jobs/\(id)/cancel", method: "POST"), delegate: Self.noRedirect)
        try check(response)
    }
    public func setService(enabled: Bool) async throws {
        let body = try JSONSerialization.data(withJSONObject: ["enabled": enabled])
        let (_, response) = try await session.data(for: request("v1/node/service", method: "PUT", body: body), delegate: Self.noRedirect)
        try check(response)
    }
    public func inventory(sourceWorkerID: String? = nil) async throws -> [ModelInventoryItem] {
        var catalogRequest = try request("v1/models/inventory")
        if let sourceWorkerID {
            var components = URLComponents(url: catalogRequest.url!, resolvingAgainstBaseURL: false)!
            components.queryItems = [URLQueryItem(name: "source_worker_id", value: sourceWorkerID)]
            catalogRequest.url = components.url
        }
        let (data, response) = try await session.data(for: catalogRequest, delegate: Self.noRedirect)
        try check(response)
        return try JSONDecoder().decode(ModelInventoryResponse.self, from: data).models
    }
    public func transfers() async throws -> [ModelTransfer] {
        let (data, response) = try await session.data(for: request("v1/models/transfers"), delegate: Self.noRedirect)
        try check(response)
        return try JSONDecoder().decode(ModelTransferResponse.self, from: data).transfers
    }
    public func startTransfer(digest: String, manifestDigest: String, sourceWorkerID: String, licenseAccepted: Bool, retry: Bool = false) async throws {
        guard Self.validDigest(digest), Self.validDigest(manifestDigest), !sourceWorkerID.isEmpty, licenseAccepted else { throw ClientError.invalidResponse }
        let body = try JSONSerialization.data(withJSONObject: ["digest": digest, "manifest_digest": manifestDigest, "source_worker_id": sourceWorkerID, "permission": true, "license_accepted": true])
        let (_, response) = try await session.data(for: request(retry ? "v1/models/transfers/\(digest)/retry" : "v1/models/transfers", method: "POST", body: body), delegate: Self.noRedirect)
        try check(response)
    }
    public func transferAction(digest: String, action: String) async throws {
        guard Self.validDigest(digest), action == "pause" else { throw ClientError.invalidResponse }
        let (_, response) = try await session.data(for: request("v1/models/transfers/\(digest)/\(action)", method: "POST"), delegate: Self.noRedirect)
        try check(response)
    }
    private static func validDigest(_ digest: String) -> Bool {
        return digest.utf8.count == 64 && digest.unicodeScalars.allSatisfy { CharacterSet(charactersIn: "0123456789abcdef").contains($0) }
    }
    public func events(onEvent: @escaping ([String: Any]) async -> Void) async throws {
        var req = try request("v1/node/events")
        req.timeoutInterval = 60
        req.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        let (bytes, response) = try await session.bytes(for: req, delegate: Self.noRedirect)
        try check(response)
        var dataLines: [String] = []
        for try await line in bytes.lines {
            if Task.isCancelled { return }
            if line.hasPrefix("data:") { dataLines.append(String(line.dropFirst(5)).trimmingCharacters(in: .whitespaces)) }
            if line.isEmpty && !dataLines.isEmpty {
                let data = Data(dataLines.joined(separator: "\n").utf8); dataLines = []
                if let snapshot = try? Self.eventSnapshot(data) { await onEvent(snapshot) }
            }
        }
    }
    public static func eventSnapshot(_ data: Data) throws -> [String: Any] {
        guard let event = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let snapshot = event["data"] as? [String: Any] else { throw ClientError.invalidResponse }
        _ = try NodeSnapshot.decode(snapshot)
        return snapshot
    }
    private func check(_ response: URLResponse) throws {
        guard let http = response as? HTTPURLResponse else { throw ClientError.invalidResponse }
        guard (200..<300).contains(http.statusCode) else { throw ClientError.response(http.statusCode) }
    }
}

/// Fields used to make live status and control decisions. Unknown fields remain forward compatible.
public struct NodeSnapshot: Decodable {
    public struct Mode: Decodable { public let mode: String; public let expires_at: Double? }
    public struct Slots: Decodable {
        public let running: Int; public let total: Int; public let occupied: Int
        public let reserved: Int?; public let quarantined: Int?
    }
    public struct Idle: Decodable {
        public let eligible: Bool; public let reason: String?; public let policy: String?
    }
    public struct Resources: Decodable {
        public let cpu_percent: Double?; public let gpu_percent: Double?
        public let memory_available_gb: Double?; public let memory_total_gb: Double?
        public let on_ac: Bool?; public let battery_percent: Double?
        public let thermal_state: String?; public let source: String?; public let observed_at: Double?
    }
    public struct Backend: Decodable {
        public let state: String; public let models_loaded: [String]; public let models_available: [String]?
    }
    public struct Peers: Decodable {
        public let known: Int; public let reachable: Int; public let stale: Int
        public let observed_available_slots: Int?
    }
    public let version: Int
    public let worker_id: String?
    public let outbound_running: Int?
    public let mode: Mode
    public let slots: Slots
    public let idle: Idle
    public let resources: Resources?
    public let backend: Backend?
    public let peers: Peers?
    public let transfers: [ModelTransfer]?
    public let model_inventory: [ModelInventoryItem]?
    public let peer_details: [ModelPeer]?
    public static func decode(_ object: [String: Any]) throws -> NodeSnapshot {
        let data = try JSONSerialization.data(withJSONObject: object)
        let snapshot = try JSONDecoder().decode(NodeSnapshot.self, from: data)
        guard snapshot.version == 1, ["auto", "draining", "offline", "paused"].contains(snapshot.mode.mode),
              snapshot.slots.running >= 0, snapshot.slots.total >= 0,
              snapshot.slots.occupied >= snapshot.slots.running,
              (snapshot.outbound_running ?? 0) >= 0 else { throw ClientError.invalidResponse }
        return snapshot
    }
}

public struct ModelInventoryResponse: Decodable { public let models: [ModelInventoryItem] }
public struct ModelPeer: Decodable, Identifiable {
    public var id: String { worker_id }
    public let worker_id: String
    public let hostname: String?
    public let local: Bool
    public let reachable: Bool
    public var displayName: String { hostname ?? worker_id }
}
public struct ModelInventoryItem: Decodable, Identifiable {
    public struct PackageFile: Decodable, Identifiable {
        public var id: String { name }
        public let name: String
        public let size_bytes: Int64
        public let role: String
    }
    public var id: String { "\(source_worker_id ?? "local"):\(digest)" }
    public let digest: String
    public let model_id: String
    public let manifest_digest: String?
    public let license: String?
    public let provenance: String?
    public let size_bytes: Int64?
    public let total_bytes: Int64?
    public let format: String?
    public let backend: String?
    public let quantization: String?
    public let source_worker_id: String?
    public let local: Bool?
    public let files: [PackageFile]?
    public var formatLabel: String? {
        switch format {
        case "mlx": return "MLX / safetensors"
        case "gguf-package": return "GGUF package"
        default: return format?.uppercased()
        }
    }

    public func canCopy(from source: String, retrying transfer: ModelTransfer? = nil) -> Bool {
        guard local == false, source_worker_id == source,
              digest.range(of: "^[0-9a-f]{64}$", options: .regularExpression) != nil,
              manifest_digest?.range(of: "^[0-9a-f]{64}$", options: .regularExpression) != nil else { return false }
        guard let transfer else { return true }
        return digest == transfer.digest && manifest_digest == transfer.manifest_digest
    }
}
public struct ModelTransferResponse: Decodable { public let transfers: [ModelTransfer] }
public struct ModelTransfer: Decodable, Identifiable {
    public var id: String { transfer_id ?? digest }
    public let transfer_id: String?
    public let manifest_digest: String?
    public let source_worker_id: String?
    public let digest: String
    public let model_id: String?
    public let state: String
    public let completed_bytes: Int64
    public let total_bytes: Int64
    public let reason: String?
}
