import XCTest
@testable import IdleHerdCore

final class PreferencesDraftTests: XCTestCase {
    func draft() throws -> PreferencesDraft {
        try PreferencesDraft(values: [
            "background_policy": "always", "idle_seconds": 300.0,
            "battery_floor_percent": 25.0, "require_ac": true,
            "quiet_hours_enabled": true, "quiet_hours_start_minute": 1320,
            "quiet_hours_end_minute": 420, "peer_max_concurrent": 2,
        ])
    }

    func testClockTimesRoundTripAcrossMidnightAndAllDayWindow() throws {
        for minute in [0, 1, 419, 420, 1320, 1439] {
            XCTAssertEqual(try PreferencesDraft.minuteOfDay(PreferencesDraft.clockTime(minute)), minute)
        }
        var view = try draft()
        XCTAssertEqual(view.quietStart, "22:00")
        XCTAssertEqual(view.quietEnd, "07:00")
        view.quietEnd = view.quietStart
        let values = try view.validatedValues()
        XCTAssertEqual(values["quiet_hours_start_minute"] as? Int, values["quiet_hours_end_minute"] as? Int)
        XCTAssertEqual(values["quiet_hours_enabled"] as? Bool, true)
    }

    func testRejectsMalformedAndOutOfRangeQuietTimes() {
        for text in ["24:00", "12:60", "7:00", "12:3", "-1:30", "22", "22:00:01", ":", "ab:cd"] {
            XCTAssertThrowsError(try PreferencesDraft.minuteOfDay(text), text)
        }
    }

    func testSavesOnlyChangedValuesWithoutFreezingOtherDefaults() throws {
        let saved = try draft()
        var edited = saved
        edited.idleSeconds = "300"
        XCTAssertTrue(try edited.changes(from: saved).isEmpty)
        edited.quietStart = "23:30"
        edited.requireAC = false
        let changes = try edited.changes(from: saved)
        XCTAssertEqual(changes.count, 2)
        XCTAssertEqual(changes["quiet_hours_start_minute"] as? Int, 1410)
        XCTAssertEqual(changes["require_ac"] as? Bool, false)
        XCTAssertNil(changes["interactive_policy"])
        XCTAssertNil(changes["background_policy"])
        XCTAssertNil(changes["hide_job_counts"])
    }

    func testRejectsInvalidBudgetsBeforeSubmitting() throws {
        let saved = try draft()
        for field in ["-1", "86401", "nan", "inf", ""] {
            var edited = saved; edited.idleSeconds = field
            XCTAssertThrowsError(try edited.validatedValues())
        }
        for field in ["-1", "101", "nan"] {
            var edited = saved; edited.batteryFloor = field
            XCTAssertThrowsError(try edited.validatedValues())
        }
        for field in ["-1", "129", "1.5", ""] {
            var edited = saved; edited.peerMax = field
            XCTAssertThrowsError(try edited.validatedValues())
        }
        var edited = saved; edited.peerMax = "0"
        XCTAssertEqual(try edited.validatedValues()["peer_max_concurrent"] as? Int, 0)
    }

    func testInvalidDaemonPreferencesDoNotBecomeEditableDefaults() {
        XCTAssertThrowsError(try PreferencesDraft(values: [:]))
        XCTAssertThrowsError(try PreferencesDraft(values: [
            "background_policy": "new-unknown-policy", "idle_seconds": 300.0,
            "battery_floor_percent": 25.0, "require_ac": true,
        ]))
    }
}
