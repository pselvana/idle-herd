import XCTest

/// Add this file to a macOS UI-testing target whose host is the packaged app.
final class IdleHerdMenuBarUITests: XCTestCase {
    func testOfflineAndReconnect() throws {
        let app = XCUIApplication()
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
        XCTAssertTrue(app.staticTexts.matching(NSPredicate(format: "label CONTAINS 'Mode: offline'")).firstMatch.waitForExistence(timeout: 10))
        app.buttons["Quit UI"].click()
        XCTAssertTrue(app.wait(for: .notRunning, timeout: 5))
        // The mock is in-process. Real daemon continuity requires integration testing.
    }
}
