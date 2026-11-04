import Foundation

/// Editable owner preferences. Only changed fields are persisted, so opening a
/// dialog does not freeze effective defaults supplied by the daemon environment.
public struct PreferencesDraft: Equatable {
    public var backgroundPolicy = "idle_only"
    public var idleSeconds = ""
    public var batteryFloor = ""
    public var requireAC = false
    public var quietHours = false
    public var quietStart = "22:00"
    public var quietEnd = "07:00"
    public var peerMax = "128"
    public var notificationsEnabled = false
    public var hideJobCounts = false

    public init() {}

    public init(values: [String: Any]) throws {
        self.init()
        guard let policy = values["background_policy"] as? String,
              let idle = values["idle_seconds"] as? Double,
              let floor = values["battery_floor_percent"] as? Double,
              let ac = values["require_ac"] as? Bool else { throw ClientError.invalidResponse }
        backgroundPolicy = policy
        idleSeconds = String(idle)
        batteryFloor = String(floor)
        requireAC = ac
        quietHours = values["quiet_hours_enabled"] as? Bool ?? false
        quietStart = try Self.clockTime(values["quiet_hours_start_minute"] as? Int ?? 1320)
        quietEnd = try Self.clockTime(values["quiet_hours_end_minute"] as? Int ?? 420)
        peerMax = String(values["peer_max_concurrent"] as? Int ?? 128)
        notificationsEnabled = values["notifications_enabled"] as? Bool ?? false
        hideJobCounts = values["hide_job_counts"] as? Bool ?? false
        _ = try validatedValues()
    }

    public enum ValidationError: LocalizedError {
        case field(String)
        public var errorDescription: String? {
            switch self { case .field(let message): return message }
        }
    }

    public static func clockTime(_ minute: Int) throws -> String {
        guard (0...1439).contains(minute) else { throw ClientError.invalidResponse }
        return String(format: "%02d:%02d", minute / 60, minute % 60)
    }

    public static func minuteOfDay(_ text: String) throws -> Int {
        let parts = text.trimmingCharacters(in: .whitespacesAndNewlines).split(separator: ":", omittingEmptySubsequences: false)
        guard parts.count == 2, parts.allSatisfy({ $0.count == 2 && $0.utf8.allSatisfy { (48...57).contains($0) } }),
              let hour = Int(parts[0]), let minute = Int(parts[1]),
              (0...23).contains(hour), (0...59).contains(minute) else {
            throw ValidationError.field("Enter quiet hours in 24-hour HH:mm format, from 00:00 to 23:59.")
        }
        return hour * 60 + minute
    }

    public func validatedValues() throws -> [String: Any] {
        guard ["always", "idle_only", "utilization_only", "manual", "never"].contains(backgroundPolicy) else {
            throw ValidationError.field("Choose a supported background policy.")
        }
        guard let idle = Double(idleSeconds), idle.isFinite, (0...86400).contains(idle) else {
            throw ValidationError.field("Idle time must be between 0 and 86,400 seconds.")
        }
        guard let floor = Double(batteryFloor), floor.isFinite, (0...100).contains(floor) else {
            throw ValidationError.field("Battery floor must be between 0 and 100 percent.")
        }
        guard let quota = Int(peerMax), (0...128).contains(quota) else {
            throw ValidationError.field("The per-peer limit must be a whole number from 0 to 128.")
        }
        return [
            "background_policy": backgroundPolicy, "idle_seconds": idle,
            "battery_floor_percent": floor, "require_ac": requireAC,
            "quiet_hours_enabled": quietHours,
            "quiet_hours_start_minute": try Self.minuteOfDay(quietStart),
            "quiet_hours_end_minute": try Self.minuteOfDay(quietEnd),
            "peer_max_concurrent": quota, "notifications_enabled": notificationsEnabled,
            "hide_job_counts": hideJobCounts,
        ]
    }

    public func changes(from saved: PreferencesDraft) throws -> [String: Any] {
        let previous = try saved.validatedValues()
        return try validatedValues().filter { key, value in
            (value as? AnyHashable) != (previous[key] as? AnyHashable)
        }
    }
}
