import XCTest
@testable import IdleHerdCore

final class StatusPresentationTests: XCTestCase {
    func testMenuBarLlamasTrackExecutionOriginIndependently() {
        let local: [String: Any] = ["state": "running", "peer_origin": false]
        let peer: [String: Any] = ["state": "running", "peer_origin": true]
        for (leases, left, right) in [([], false, false), ([local], true, false),
                                     ([peer], false, true), ([local, peer], true, true)] {
            let activity = MenuBarActivity(workerLeases: leases, current: true)
            XCTAssertEqual(activity.localExecuting, left)
            XCTAssertEqual(activity.peerExecuting, right)
        }
    }

    func testMenuBarDoesNotLightForUncertainNonrunningOrStaleWork() {
        let leases: [[String: Any]] = [
            ["state": "reserved", "peer_origin": false],
            ["state": "quarantined", "peer_origin": true],
            ["state": "completed", "peer_origin": false],
            ["state": "queued", "peer_origin": true],
            ["state": "running"], // Unknown provenance cannot choose a llama.
        ]
        let idle = MenuBarActivity(workerLeases: [], current: true)
        XCTAssertEqual(MenuBarActivity(workerLeases: leases, current: true), idle)
        XCTAssertEqual(MenuBarActivity(workerLeases: [
            ["state": "running", "peer_origin": false],
            ["state": "running", "peer_origin": true],
        ], current: false), idle)
    }

    func testJobOriginsUseHostnamesAndHonestOlderDaemonFallbacks() {
        XCTAssertEqual(StatusPresentation.jobOrigin(["peer_origin": true, "origin_host": "pstudio.local"]), "From pstudio.local")
        XCTAssertEqual(StatusPresentation.jobOrigin(["peer_origin": false, "origin_host": "my-mac.local"]), "From my-mac.local")
        XCTAssertEqual(StatusPresentation.jobOrigin(["peer_origin": true, "origin_worker_id": "abcdef123456"]), "From peer abcdef12")
        XCTAssertEqual(StatusPresentation.jobOrigin(["peer_origin": true]), "From unknown peer")
        XCTAssertEqual(StatusPresentation.jobOrigin([:]), "From this Mac")
    }

    func testQuarantineTextDistinguishesConfirmationWindowFromUnknownOutcome() {
        let now = Date(timeIntervalSince1970: 100)
        XCTAssertEqual(StatusPresentation.quarantineRecovery([
            "state": "quarantined", "quarantine_recovery": "awaiting_backend",
            "quarantine_recovery_deadline": 115.0,
        ], now: now), "Waiting for backend completion · up to 15s")
        XCTAssertEqual(StatusPresentation.quarantineRecovery([
            "state": "quarantined", "quarantine_recovery": "awaiting_backend",
            "quarantine_recovery_deadline": 99.0,
        ], now: now), "Backend outcome unknown · stopped-backend recovery required")
        XCTAssertNil(StatusPresentation.quarantineRecovery(["state": "running"]))
    }
    func snapshot(_ fields: [String: Any] = [:]) throws -> NodeSnapshot {
        var data: [String: Any] = [
            "version": 1, "mode": ["mode": "auto"],
            "slots": ["running": 0, "occupied": 0, "total": 1],
            "idle": ["eligible": false, "reason": "owner_active"],
        ]
        data.merge(fields) { _, new in new }
        return try NodeSnapshot.decode(data)
    }

    func testMeasurementsAndPowerUseReadableUnits() throws {
        let view = StatusPresentation(try snapshot([
            "resources": ["cpu_percent": 11.44067798499703, "gpu_percent": 0,
                          "memory_available_gb": 22.748971008, "memory_total_gb": 64,
                          "on_ac": true, "battery_percent": 100, "source": "macos_iokit"],
        ]))
        XCTAssertEqual(view.cpu, "11.4%")
        XCTAssertEqual(view.gpu, "0.0%")
        XCTAssertEqual(view.memory, "22.7 of 64.0 GB available")
        XCTAssertEqual(view.power, "Power adapter")
        XCTAssertEqual(view.battery, "100%")
        XCTAssertEqual(view.thermal, "Not reported")
        XCTAssertEqual(view.sensorSource, "macOS host sensor")
    }

    func testMissingMeasurementsAreNotZeroOrFalse() throws {
        let view = StatusPresentation(try snapshot([
            "resources": ["memory_available_gb": 22.748971008, "on_ac": NSNull(), "gpu_percent": NSNull()],
        ]))
        XCTAssertEqual(view.memory, "22.7 GB available")
        XCTAssertEqual(view.cpu, "Not reported")
        XCTAssertEqual(view.gpu, "Not reported")
        XCTAssertEqual(view.power, "Not reported")
        XCTAssertEqual(view.battery, "Not reported")
        XCTAssertEqual(StatusPresentation.percent(.infinity), "Not reported")
        XCTAssertEqual(StatusPresentation.percent(-1), "Not reported")
        XCTAssertEqual(StatusPresentation.percent(101), "Not reported")
        let battery = StatusPresentation(try snapshot(["resources": ["on_ac": false]]))
        XCTAssertEqual(battery.power, "Battery power")
    }

    func testOlderSnapshotsAndUnreportedBackendStayHonest() throws {
        let view = StatusPresentation(try snapshot())
        XCTAssertEqual(view.mode, "Automatic")
        XCTAssertEqual(view.eligibility, "Waiting until you’re idle")
        XCTAssertEqual(view.cpu, "Not reported")
        XCTAssertEqual(view.backend, "Waiting for status")
        XCTAssertEqual(view.loadedModels, "Waiting for status")
        let unknown = StatusPresentation(try snapshot(["backend": ["state": "unknown", "models_loaded": []]]))
        XCTAssertEqual(unknown.loadedModels, "Backend status not reported")
    }

    func testUnavailableModelsAreDistinctFromEmptyLoadedModels() throws {
        let offline = StatusPresentation(try snapshot(["backend": ["state": "offline", "models_loaded": ["old:model"]]]))
        XCTAssertEqual(offline.backend, "Ollama · Offline")
        XCTAssertEqual(offline.loadedModels, "Unavailable while Ollama is offline")
        let online = StatusPresentation(try snapshot(["backend": ["state": "online", "models_loaded": []]]))
        XCTAssertEqual(online.loadedModels, "None loaded")
    }

    func testCapacitySeparatesMissingObservationsFromZeroPeers() throws {
        let none = StatusPresentation(try snapshot(["peers": ["known": 0, "reachable": 0, "stale": 0]]))
        XCTAssertEqual(none.remoteSlots, "0")
        let pending = StatusPresentation(try snapshot(["peers": ["known": 1, "reachable": 1, "stale": 0]]))
        XCTAssertEqual(pending.remoteSlots, "Awaiting peer status")
        let measured = StatusPresentation(try snapshot([
            "peers": ["known": 1, "reachable": 1, "stale": 0, "observed_available_slots": 3],
        ]))
        XCTAssertEqual(measured.remoteSlots, "3")
    }
}
