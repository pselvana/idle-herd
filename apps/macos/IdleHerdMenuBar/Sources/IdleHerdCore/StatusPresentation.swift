import Foundation

/// Execution location and fresh peer reachability drive the menu bar artwork.
public struct MenuBarActivity: Equatable {
    public enum RightLlama: Int, CaseIterable {
        case hidden, outline, incoming, outgoing
    }

    public let localExecuting: Bool
    public let peerExecuting: Bool
    public let remoteExecuting: Bool
    public let hasConnectedPeers: Bool

    public init(workerLeases: [[String: Any]], originJobs: [[String: Any]] = [],
                localWorkerID: String? = nil, connectedRemoteHosts: Int = 0,
                outboundRunning: Int? = nil, current: Bool,
                workerLeasesCurrent: Bool = true, originJobsCurrent: Bool = true) {
        let running = current && workerLeasesCurrent ? workerLeases.filter { $0["state"] as? String == "running" } : []
        localExecuting = running.contains { ($0["peer_origin"] as? Bool) == false }
        peerExecuting = running.contains { ($0["peer_origin"] as? Bool) == true }
        hasConnectedPeers = current && connectedRemoteHosts > 0
        if let outboundRunning {
            remoteExecuting = current && outboundRunning > 0
        } else {
            // Older daemons only expose journaled origin jobs. Require explicit
            // worker identities; a missing location cannot imply remote execution.
            remoteExecuting = current && originJobsCurrent && originJobs.contains { job in
                guard let localWorkerID, !localWorkerID.isEmpty,
                      job["state"] as? String == "running",
                      job["origin_worker_id"] as? String == localWorkerID,
                      let worker = job["worker_id"] as? String, !worker.isEmpty else { return false }
                return worker != localWorkerID
            }
        }
    }

    public var rightLlama: RightLlama {
        guard hasConnectedPeers else { return .hidden }
        if peerExecuting { return .incoming }
        return remoteExecuting ? .outgoing : .outline
    }

    public var accessibilityDescription: String {
        var descriptions: [String] = []
        if localExecuting { descriptions.append("Local work executing on this Mac") }
        if peerExecuting { descriptions.append("Peer work executing on this Mac") }
        if remoteExecuting { descriptions.append("Work from this Mac executing on a remote host") }
        if descriptions.isEmpty { descriptions.append("No confirmed execution") }
        if !hasConnectedPeers { descriptions.append("No remote hosts connected") }
        return descriptions.joined(separator: ", ")
    }
}

/// Presentation keeps unavailable observations distinct from measured zeroes.
public struct StatusPresentation {
    private let snapshot: NodeSnapshot?
    public init(_ snapshot: NodeSnapshot?) { self.snapshot = snapshot }

    public static func label(_ value: String?, fallback: String = "Not reported") -> String {
        guard let value, !value.isEmpty, value != "unknown" else { return fallback }
        return value.replacingOccurrences(of: "_", with: " ").capitalized
    }

    public static func percent(_ value: Double?) -> String {
        guard let value, value.isFinite, (0...100).contains(value) else { return "Not reported" }
        return value.formatted(.number.precision(.fractionLength(1))) + "%"
    }

    public static func jobOrigin(_ job: [String: Any]) -> String {
        if let host = job["origin_host"] as? String, !host.isEmpty { return "From \(host)" }
        if (job["peer_origin"] as? Bool) == true {
            if let worker = job["origin_worker_id"] as? String, !worker.isEmpty {
                return "From peer \(worker.prefix(8))"
            }
            return "From unknown peer"
        }
        return "From this Mac"
    }

    public static func quarantineRecovery(_ job: [String: Any], now: Date = Date()) -> String? {
        guard job["state"] as? String == "quarantined" else { return nil }
        if job["quarantine_recovery"] as? String == "awaiting_backend",
           let deadline = job["quarantine_recovery_deadline"] as? Double,
           deadline.isFinite, deadline > now.timeIntervalSince1970 {
            return "Waiting for backend completion · up to \(Int(ceil(deadline - now.timeIntervalSince1970)))s"
        }
        return "Backend outcome unknown · stopped-backend recovery required"
    }

    public var mode: String {
        switch snapshot?.mode.mode {
        case "auto": return "Automatic"
        case "draining": return "Draining"
        case "offline": return "Offline"
        case "paused": return "Paused"
        default: return "Waiting for status"
        }
    }

    public var eligibility: String {
        guard let idle = snapshot?.idle else { return "Waiting for status" }
        if idle.eligible { return "Ready for peer work" }
        switch idle.reason {
        case "owner_active": return "Waiting until you’re idle"
        case "activity_unknown": return "Waiting for activity sensor"
        case "utilization_unknown": return "Waiting for resource measurements"
        case "sensor_stale": return "Waiting for a fresh sensor sample"
        case "policy_never": return "Peer work disabled in policy"
        case "manual_approval_required": return "Peer work needs manual approval"
        case "memory_pressure": return "Waiting for available memory"
        case "thermal_limit": return "Waiting for this Mac to cool down"
        case "ac_required": return "Connect power to accept peer work"
        case "battery_low": return "Battery below the contribution limit"
        case "cpu_busy", "gpu_busy": return "Waiting for lower system usage"
        case "cpu_recovery", "idle_hysteresis": return "Waiting for usage to settle"
        case "quiet_hours": return "Peer work blocked during quiet hours"
        case "peer_quota": return "Peer contribution limit reached"
        case "manual_offline": return "Peer work disabled while offline"
        case "manual_paused": return "New inference paused"
        case "manual_draining": return "Finishing existing work"
        default: return Self.label(idle.reason, fallback: "Peer work unavailable")
        }
    }

    public var cpu: String { Self.percent(snapshot?.resources?.cpu_percent) }
    public var gpu: String { Self.percent(snapshot?.resources?.gpu_percent) }
    public var memory: String {
        let resources = snapshot?.resources
        let available = resources?.memory_available_gb.flatMap { $0.isFinite && $0 >= 0 ? $0 : nil }
        let total = resources?.memory_total_gb.flatMap { $0.isFinite && $0 > 0 ? $0 : nil }
        let format: (Double) -> String = { $0.formatted(.number.precision(.fractionLength(1))) }
        switch (available, total) {
        case let (available?, total?): return "\(format(available)) of \(format(total)) GB available"
        case let (available?, nil): return "\(format(available)) GB available"
        case let (nil, total?): return "\(format(total)) GB total · Available not reported"
        default: return "Not reported"
        }
    }
    public var power: String {
        switch snapshot?.resources?.on_ac {
        case true: return "Power adapter"
        case false: return "Battery power"
        default: return "Not reported"
        }
    }
    public var battery: String {
        guard let value = snapshot?.resources?.battery_percent,
              value.isFinite, (0...100).contains(value) else { return "Not reported" }
        return value.formatted(.number.precision(.fractionLength(0))) + "%"
    }
    public var thermal: String { Self.label(snapshot?.resources?.thermal_state) }
    public var backend: String {
        guard let backend = snapshot?.backend else { return "Waiting for status" }
        return "Ollama · " + Self.label(backend.state, fallback: "Status not reported")
    }
    public var loadedModels: String {
        guard let backend = snapshot?.backend else { return "Waiting for status" }
        if backend.state == "offline" { return "Unavailable while Ollama is offline" }
        guard backend.state == "online" else { return "Backend status not reported" }
        return backend.models_loaded.isEmpty ? "None loaded" : backend.models_loaded.joined(separator: ", ")
    }
    public var remoteSlots: String {
        guard let peers = snapshot?.peers else { return "Waiting for status" }
        if let slots = peers.observed_available_slots { return String(slots) }
        return peers.reachable == 0 ? "0" : "Awaiting peer status"
    }
    public var sensorSource: String {
        switch snapshot?.resources?.source {
        case "macos_iokit": return "macOS host sensor"
        case "linux_logind": return "Linux host sensor"
        case "windows_session": return "Windows session sensor"
        case "native_helper": return "Native sensor helper"
        case "sensor_failure": return "Host sensor unavailable"
        default: return Self.label(snapshot?.resources?.source)
        }
    }
}
