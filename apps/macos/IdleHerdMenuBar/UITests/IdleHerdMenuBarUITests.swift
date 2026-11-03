import XCTest

/// Add this file to a macOS UI-testing target whose host is the packaged app.
final class IdleHerdMenuBarUITests: XCTestCase {
    func testOfflineAndReconnect() throws {
        let app = XCUIApplication()
        app.launchArguments = ["-hideJobCount", "NO"]
        app.launchEnvironment["IDLE_HERD_UI_TESTING"] = "1"
        app.launch()
        let menu = app.menuBars.statusItems.matching(identifier: "Idle-Herd, 2 local jobs").firstMatch
        XCTAssertTrue(menu.waitForExistence(timeout: 10))
        menu.click()
        XCTAssertTrue(app.staticTexts["Connected"].waitForExistence(timeout: 10))
        XCTAssertTrue(app.staticTexts["Disconnected · stale"].waitForExistence(timeout: 10))
        XCTAssertTrue(app.staticTexts["Connected"].waitForExistence(timeout: 10))
        app.buttons["Go offline"].click()
        XCTAssertTrue(app.alerts["Go offline?"].waitForExistence(timeout: 3))
        app.alerts.buttons["Go offline"].click()
        XCTAssertTrue(app.staticTexts["Mode: Offline"].waitForExistence(timeout: 10))
        app.menuButtons["More actions"].click()
        app.menuItems["Quit UI"].click()
        XCTAssertTrue(app.wait(for: .notRunning, timeout: 5))
        // The mock is in-process. Real daemon continuity requires integration testing.
    }

    func testTelemetryAndEmptyStates() throws {
        let app = XCUIApplication()
        app.launchArguments = ["-hideJobCount", "NO"]
        app.launchEnvironment["IDLE_HERD_UI_TESTING"] = "1"
        app.launchEnvironment["IDLE_HERD_UI_SCENARIO"] = "telemetry"
        app.launch()
        let menu = app.menuBars.statusItems.matching(identifier: "Idle-Herd, 2 local jobs").firstMatch
        XCTAssertTrue(menu.waitForExistence(timeout: 10))
        menu.click()
        XCTAssertTrue(app.staticTexts["Waiting until you’re idle"].waitForExistence(timeout: 5))
        XCTAssertEqual(app.staticTexts["cpuValue"].label, "11.4%")
        XCTAssertEqual(app.staticTexts["memoryValue"].label, "22.7 of 64.0 GB available")
        XCTAssertEqual(app.staticTexts["powerValue"].label, "Power adapter")
        XCTAssertEqual(app.staticTexts["backendValue"].label, "Ollama · Offline")
        let screenshot = XCTAttachment(screenshot: app.screenshot())
        screenshot.name = "Menu bar telemetry"
        screenshot.lifetime = .keepAlways
        add(screenshot)
        app.scrollViews.firstMatch.swipeUp()
        XCTAssertTrue(app.staticTexts["No other peers discovered"].exists)
        XCTAssertEqual(app.staticTexts["remoteSlotsValue"].label, "0")
        XCTAssertTrue(app.buttons["Preferences…"].isHittable)
        app.terminate()
    }
}
