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
        XCTAssertTrue(app.menuButtons["Mode: Offline"].waitForExistence(timeout: 10))
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
        app.disclosureTriangles["Resources & backend"].click()
        app.scrollViews.firstMatch.swipeUp()
        XCTAssertEqual(app.staticTexts["cpuValue"].label, "11.4%")
        XCTAssertEqual(app.staticTexts["memoryValue"].label, "22.7 of 64.0 GB available")
        XCTAssertEqual(app.staticTexts["powerValue"].label, "Power adapter")
        XCTAssertEqual(app.staticTexts["backendValue"].label, "Ollama · Offline")
        let screenshot = XCTAttachment(screenshot: app.screenshot())
        screenshot.name = "Menu bar telemetry"
        screenshot.lifetime = .keepAlways
        add(screenshot)
        app.scrollViews.firstMatch.swipeDown()
        XCTAssertTrue(app.staticTexts["No other peers discovered"].exists)
        XCTAssertEqual(app.staticTexts["remoteSlotsValue"].label, "0")
        XCTAssertTrue(app.buttons["Preferences…"].isHittable)
        app.terminate()
    }

    func testTimedPauseRequiresConfirmationAndShowsExpiry() throws {
        let app = XCUIApplication()
        app.launchArguments = ["-hideJobCount", "NO"]
        app.launchEnvironment["IDLE_HERD_UI_TESTING"] = "1"
        app.launchEnvironment["IDLE_HERD_UI_SCENARIO"] = "telemetry"
        app.launch()
        let menu = app.menuBars.statusItems.matching(identifier: "Idle-Herd, 2 local jobs").firstMatch
        XCTAssertTrue(menu.waitForExistence(timeout: 10))
        menu.click()
        XCTAssertTrue(app.buttons["Pause for 1 hr"].waitForExistence(timeout: 5))
        app.buttons["Pause for 1 hr"].click()
        XCTAssertTrue(app.alerts["Pause new inference?"].waitForExistence(timeout: 3))
        app.alerts.buttons["Cancel"].click()
        XCTAssertTrue(app.menuButtons["Mode: Automatic"].exists)
        app.buttons["Pause for 1 hr"].click()
        app.alerts.buttons["Pause"].click()
        XCTAssertTrue(app.menuButtons["Mode: Paused"].waitForExistence(timeout: 5))
        XCTAssertTrue(app.staticTexts.containing(NSPredicate(format: "label BEGINSWITH %@", "Returns to Automatic at")).firstMatch.exists)
        app.buttons["Auto"].click()
        XCTAssertTrue(app.menuButtons["Mode: Automatic"].waitForExistence(timeout: 5))
        app.terminate()
    }

    func testPreferencesValidationSaveAndClose() throws {
        let app = XCUIApplication()
        app.launchArguments = ["-hideJobCount", "NO"]
        app.launchEnvironment["IDLE_HERD_UI_TESTING"] = "1"
        app.launchEnvironment["IDLE_HERD_UI_SCENARIO"] = "preferences"
        app.launch()
        let menu = app.menuBars.statusItems.matching(identifier: "Idle-Herd, 2 local jobs").firstMatch
        XCTAssertTrue(menu.waitForExistence(timeout: 10))
        menu.click()
        app.buttons["Preferences…"].click()
        let preferences = app.windows["Idle-Herd Preferences"]
        XCTAssertTrue(preferences.waitForExistence(timeout: 5))
        preferences.radioButtons["Daemon Policy"].click()
        let idle = preferences.textFields["idleSecondsField"]
        XCTAssertTrue(idle.waitForExistence(timeout: 5))
        idle.click()
        idle.typeKey("a", modifierFlags: .command)
        idle.typeText("not-a-number")
        preferences.buttons["Save Daemon Policy"].click()
        let message = preferences.staticTexts["preferencesMessage"]
        XCTAssertTrue(message.waitForExistence(timeout: 3))
        XCTAssertTrue(message.label.contains("Idle time must"))
        XCTAssertEqual(idle.value as? String, "not-a-number")
        idle.click()
        idle.typeKey("a", modifierFlags: .command)
        idle.typeText("600")
        preferences.buttons["Save Daemon Policy"].click()
        expectation(for: NSPredicate(format: "label CONTAINS %@", "Daemon policy saved."), evaluatedWith: message)
        waitForExpectations(timeout: 5)
        XCTAssertFalse(preferences.buttons["Save Daemon Policy"].isEnabled)
        preferences.buttons["Close"].click()
        expectation(for: NSPredicate(format: "exists == false"), evaluatedWith: preferences)
        waitForExpectations(timeout: 3)
        XCTAssertTrue(app.state == .runningForeground || app.state == .runningBackground)
        app.terminate()
    }

}
