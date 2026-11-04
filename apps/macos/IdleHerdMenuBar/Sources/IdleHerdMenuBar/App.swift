import SwiftUI
import AppKit
import ServiceManagement
import IdleHerdCore

@MainActor final class NodeModel: ObservableObject {
    @Published var status: [String: Any] = [:]
    @Published var snapshot: NodeSnapshot?
    @Published var jobs: [[String: Any]] = []
    @Published var originJobs: [[String: Any]] = []
    @Published var inventory: [ModelInventoryItem] = []
    @Published var transfers: [ModelTransfer] = []
    @Published var sharingError: String?
    @Published var jobsError: String?
    @Published var connected = false
    @Published var lastUpdate: Date?
    @Published var error: String?
    @Published var actionError: String?
    @Published var busy = false
    let client: NodeClient = {
        #if DEBUG
        if ProcessInfo.processInfo.environment["IDLE_HERD_UI_TESTING"] == "1" {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [UITestProtocol.self]
            return NodeClient(session: URLSession(configuration: configuration), token: { "test-token" })
        }
        #endif
        return NodeClient()
    }()
    private var stream: Task<Void, Never>?
    private var polling: Task<Void, Never>?
    private var freshness: Task<Void, Never>?
    private var refreshing = false

    func start() {
        guard stream == nil else { return }
        stream = Task {
            while !Task.isCancelled {
                await refresh()
                do { try await client.events { [weak self] snapshot in try? self?.apply(snapshot) } }
                catch { self.error = "Live events unavailable; polling status." }
                if !Task.isCancelled { try? await Task.sleep(nanoseconds: 3_000_000_000) }
            }
        }
        // Poll while streaming too: a dead/quiet connection must not make old data look live.
        polling = Task {
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 3_000_000_000)
                if !Task.isCancelled { await refresh() }
            }
        }
        freshness = Task {
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 1_000_000_000)
                if let last = lastUpdate, Date().timeIntervalSince(last) > 5 {
                    connected = false
                    error = "Status is stale; reconnecting."
                }
            }
        }
    }
    func refresh() async {
        guard !refreshing else { return }
        refreshing = true
        defer { refreshing = false }
        do {
            let snapshot = try await client.fetch("v1/node/status")
            try apply(snapshot)
            // Current daemons include jobs in the status snapshot. Retain the
            // jobs endpoint only for older daemons, and never hide fetch errors.
            if snapshot["worker_leases"] == nil || snapshot["job_history"] == nil {
                do {
                    let jobSnapshot = try await client.fetch("v1/node/jobs")
                    jobs = (jobSnapshot["worker_leases"] as? [[String: Any]]) ?? []
                    originJobs = (jobSnapshot["jobs"] as? [[String: Any]]) ?? []
                    jobsError = nil
                } catch { jobsError = "Job details unavailable: \(error.localizedDescription)" }
            }
            do {
                let inventory = try await client.inventory()
                self.inventory = inventory
                if self.snapshot?.transfers == nil { self.transfers = try await client.transfers() }
                sharingError = nil
            } catch { inventory = self.snapshot?.model_inventory ?? []; sharingError = "Model sharing status unavailable: \(error.localizedDescription)" }
        } catch { connected = false; self.error = error.localizedDescription }
    }
    func apply(_ object: [String: Any]) throws {
        let validated = try NodeSnapshot.decode(object)
        snapshot = validated; status = object
        if let leases = object["worker_leases"] as? [[String: Any]] { jobs = leases }
        if let history = object["job_history"] as? [[String: Any]] { originJobs = history }
        if object["worker_leases"] != nil && object["job_history"] != nil { jobsError = nil }
        if let values = validated.transfers { transfers = values }
        if let local = validated.model_inventory {
            let remote = inventory.filter { $0.local == false }
            let knownLocal = inventory.filter { $0.local == true }
            inventory = local.map { item in knownLocal.first { $0.digest == item.digest } ?? item } + remote
        }
        connected = true; lastUpdate = Date(); error = nil
    }
    func transferAction(_ transfer: ModelTransfer, action: String) {
        actionError = nil; busy = true
        Task {
            do { try await client.transferAction(digest: transfer.digest, action: action); await refresh() }
            catch { actionError = error.localizedDescription }
            busy = false
        }
    }
    func startTransfer(digest: String, manifestDigest: String, source: String, licenseAccepted: Bool, retry: Bool) {
        actionError = nil; busy = true
        Task {
            do { try await client.startTransfer(digest: digest, manifestDigest: manifestDigest, sourceWorkerID: source, licenseAccepted: licenseAccepted, retry: retry); await refresh() }
            catch { actionError = error.localizedDescription }
            busy = false
        }
    }
    func cancelJob(_ id: String) {
        actionError = nil
        busy = true
        Task {
            do { try await client.cancelJob(id); await refresh() }
            catch { self.actionError = error.localizedDescription }
            busy = false
        }
    }
    func changeMode(_ mode: String, duration: Int? = nil) {
        actionError = nil
        busy = true
        Task {
            do { try await client.setMode(mode, duration: duration); await refresh() }
            catch { self.actionError = error.localizedDescription }
            busy = false
        }
    }
    func value(_ keys: String...) -> String {
        var current: Any = status
        for key in keys { guard let dict = current as? [String: Any], let next = dict[key] else { return "—" }; current = next }
        if let value = current as? String { return value }
        if let value = current as? NSNumber { return value.stringValue }
        return "—"
    }
    var count: String { connected ? snapshot.map { String($0.slots.running) } ?? "—" : "—" }
    var presentation: StatusPresentation { StatusPresentation(snapshot) }
    var availableSlots: String {
        guard let slots = snapshot?.slots else { return "—" }
        return String(max(0, slots.total - slots.occupied))
    }
    func originLocation(_ job: [String: Any]) -> String {
        if let host = job["execution_host"] as? String { return "worker \(host)" }
        guard let worker = job["worker_id"] as? String else { return "unassigned or unknown" }
        guard let local = status["worker_id"] as? String else { return "location unknown" }
        return worker == local ? "local worker" : "remote worker"
    }
    var telemetryAge: String {
        guard let stamp = snapshot?.resources?.observed_at, stamp.isFinite, stamp > 0 else { return "Sample time not reported" }
        return "\(max(0, Int(Date().timeIntervalSince1970 - stamp)))s ago"
    }
    func jobDescription(_ job: [String: Any]) -> String {
        let name = job["model"] as? String ?? "unknown model"
        let state = job["state"] as? String ?? "unknown"
        let origin = StatusPresentation.jobOrigin(job)
        let elapsed = (job["created_at"] as? Double).map { "\(max(0, Int(Date().timeIntervalSince1970 - $0)))s" } ?? "unknown"
        return "\(name) · \(state) · \(origin) · \(elapsed)"
    }
}

@main struct IdleHerdApp: App {
    @StateObject private var model = NodeModel()
    @AppStorage("hideJobCount") private var hideCount = false
    var body: some Scene {
        MenuBarExtra {
            ContentView(model: model).onAppear { model.start() }
        } label: {
            Label(hideCount || (model.status["hide_job_counts"] as? Bool == true) || !model.connected ? "Idle-Herd" : "Idle-Herd \(model.count)", systemImage: model.connected ? "circle.grid.2x2.fill" : "exclamationmark.circle")
                .onAppear { model.start() }
                .accessibilityLabel(model.connected ? ((hideCount || model.status["hide_job_counts"] as? Bool == true) ? "Idle-Herd, connected" : "Idle-Herd, \(model.count) local jobs") : "Idle-Herd, disconnected")
        }.menuBarExtraStyle(.window)
    }
}

struct ContentView: View {
    @ObservedObject var model: NodeModel
    @State private var confirm: String?
    @State private var cancelJobID: String?
    @State private var showingPreferences = false
    @State private var showingDiagnostics = false
    @State private var showingTransfer = false
    @State private var retryTransfer: ModelTransfer?
    @State private var diagnostics = ""
    var body: some View {
        VStack(spacing: 0) {
            header.padding(14)
            Divider()
            ScrollView {
                VStack(alignment: .leading, spacing: 12) {
                    if let error = model.error {
                        Label(error, systemImage: "exclamationmark.triangle")
                            .font(.caption).foregroundStyle(.orange).textSelection(.enabled)
                    }
                    jobsSection
                    resourcesSection
                    clusterSection
                    sharingSection
                    if let error = model.actionError {
                        HStack {
                            Text(error).foregroundStyle(.red).font(.caption).textSelection(.enabled)
                            Button("Dismiss") { model.actionError = nil }
                        }
                    }
                }.padding(14)
            }
            Divider()
            controlsSection.padding(.horizontal, 14).padding(.vertical, 10)
            Divider()
            footer.padding(12)
        }
        .frame(width: 440, height: min(700, max(360, (NSScreen.main?.visibleFrame.height ?? 800) - 90)))
            .alert(confirm == "offline" ? "Go offline?" : "Pause new inference?", isPresented: Binding(get: { confirm != nil }, set: { if !$0 { confirm = nil } })) {
                Button(confirm == "offline" ? "Go offline" : "Pause") { if let mode = confirm { model.changeMode(mode) }; confirm = nil }
                Button("Cancel", role: .cancel) { confirm = nil }
            } message: { Text(confirm == "offline" ? "Peer work stops; local inference remains available. Running work will finish." : "New local and remote inference will be refused. Running work will finish.") }
            .alert("Cancel this job?", isPresented: Binding(get: { cancelJobID != nil }, set: { if !$0 { cancelJobID = nil } })) {
                Button("Cancel job", role: .destructive) { if let id = cancelJobID { model.cancelJob(id) }; cancelJobID = nil }
                Button("Keep job", role: .cancel) { cancelJobID = nil }
            } message: { Text("Cancellation is best effort. The outcome may remain unknown.") }
            .sheet(isPresented: $showingTransfer) { TransferView(model: model) }
            .sheet(item: $retryTransfer) { transfer in TransferView(model: model, retryTransfer: transfer) }
            .sheet(isPresented: $showingPreferences) { PreferencesView(model: model) }
            .sheet(isPresented: $showingDiagnostics) {
                VStack(alignment: .leading) { Text("Diagnostics").font(.headline); ScrollView { Text(diagnostics).textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading) }; Button("Close") { showingDiagnostics = false } }.padding().frame(width: 480, height: 300)
            }
    }
    private var header: some View {
        VStack(alignment: .leading, spacing: 7) {
            HStack {
                Text("Idle-Herd").font(.headline)
                Spacer()
                Circle().fill(model.connected ? Color.green : Color.orange).frame(width: 7, height: 7)
                Text(model.connected ? "Connected" : (model.lastUpdate == nil ? "Connecting…" : "Disconnected · stale"))
                    .font(.caption).foregroundStyle(.secondary)
            }
            HStack {
                Text("Mode: \(model.presentation.mode)").font(.subheadline.bold())
                Spacer()
                if model.busy { ProgressView().controlSize(.small) }
            }
            Text(model.presentation.eligibility).font(.subheadline).foregroundStyle(.secondary)
                .help("Eligibility for incoming background peer work. Local inference follows its own policy and backend availability.")
            if let expiry = model.snapshot?.mode.expires_at {
                Text("Returns to Automatic at \(Date(timeIntervalSince1970: expiry).formatted(date: .omitted, time: .shortened))")
                    .font(.caption).foregroundStyle(.secondary)
            }
            if let last = model.lastUpdate {
                Text("Updated \(last.formatted(date: .omitted, time: .standard))\(model.connected ? "" : " · Last received values")")
                    .font(.caption2).foregroundStyle(.secondary)
            }
        }.frame(maxWidth: .infinity, alignment: .leading)
    }

    private func section<Content: View>(_ title: String, @ViewBuilder content: () -> Content) -> some View {
        VStack(alignment: .leading, spacing: 9) {
            Text(title).font(.caption.bold()).foregroundStyle(.secondary)
            content()
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(12)
        .background(Color.primary.opacity(0.04), in: RoundedRectangle(cornerRadius: 10))
    }

    private func row(_ title: String, _ value: String, id: String? = nil) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 12) {
            Text(title).foregroundStyle(.secondary).frame(width: 110, alignment: .leading)
            Spacer(minLength: 0)
            Text(value).multilineTextAlignment(.trailing).textSelection(.enabled)
                .accessibilityIdentifier(id ?? title)
        }.font(.callout)
    }

    private var jobsSection: some View {
        section("LOCAL JOBS") {
            HStack(spacing: 18) {
                jobCount(model.count, "Running")
                jobCount(model.value("jobs", "queued"), "Queued at origin")
                jobCount(model.value("slots", "reserved"), "Reserved")
            }
            if let error = model.jobsError { Text(error).font(.caption).foregroundStyle(.orange) }
            ForEach(Array(model.jobs.prefix(5).enumerated()), id: \.offset) { _, job in
                VStack(alignment: .leading, spacing: 3) {
                    Text(model.jobDescription(job)).font(.caption).lineLimit(2)
                    if let recovery = StatusPresentation.quarantineRecovery(job) {
                        Text(recovery).font(.caption2).foregroundStyle(.orange)
                    }
                }
            }
            DisclosureGroup("Origin journal") {
                if model.originJobs.isEmpty {
                    Text(model.jobsError == nil ? "No jobs submitted from this Mac." : "Job history unavailable.")
                        .font(.caption).foregroundStyle(.secondary)
                }
                ForEach(Array(model.originJobs.prefix(3).enumerated()), id: \.offset) { _, job in
                    HStack {
                        Text("\(job["model"] as? String ?? "Model unavailable") · \(StatusPresentation.label(job["state"] as? String)) · \(model.originLocation(job))")
                            .font(.caption).lineLimit(2)
                        Spacer()
                        if let id = job["job_id"] as? String,
                           let state = job["state"] as? String,
                           ["queued", "reserving", "running", "retry_pending", "unknown_outcome"].contains(state),
                           (job["cancel_requested"] as? Bool) != true {
                            Button("Cancel…") { cancelJobID = id }.disabled(!model.connected || model.busy)
                                .accessibilityLabel("Cancel origin job \(job["model"] as? String ?? "Model unavailable")")
                        }
                    }
                }
            }
        }
    }

    private func jobCount(_ value: String, _ title: String) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            Text(value).font(.title3.weight(.semibold)).monospacedDigit()
            Text(title).font(.caption).foregroundStyle(.secondary)
        }.frame(maxWidth: .infinity, alignment: .leading)
    }

    private var resourcesSection: some View {
        section("THIS MAC") {
            row("CPU", model.presentation.cpu, id: "cpuValue")
            row("Memory", model.presentation.memory, id: "memoryValue")
            row("GPU", model.presentation.gpu)
                .help("GPU utilization is shown only when reported by the host sensor. Missing data does not mean 0% usage.")
            row("Power", model.presentation.power, id: "powerValue")
            if model.snapshot?.resources?.battery_percent != nil { row("Battery", model.presentation.battery) }
            row("Thermal", model.presentation.thermal)
                .help("The current macOS host sensor does not report thermal state. Missing data is never treated as a nominal measurement.")
            Divider()
            row("Backend", model.presentation.backend, id: "backendValue")
            if model.snapshot?.backend?.state == "offline" {
                Text("Start Ollama to run local models. Check Diagnostics for connection details.")
                    .font(.caption).foregroundStyle(.orange)
            }
            row("Loaded models", model.presentation.loadedModels)
            row("Unoccupied slots", "\(model.availableSlots) of \(model.value("slots", "total"))")
                .help("Unoccupied slots describe local lease occupancy. Admission also checks policy, quarantine, backend and model resources.")
            if let quarantined = model.snapshot?.slots.quarantined, quarantined > 0 {
                Label("\(quarantined) quarantined slot(s) · see job recovery status", systemImage: "exclamationmark.triangle")
                    .font(.caption).foregroundStyle(.orange)
            }
            Text("\(model.presentation.sensorSource) · \(model.telemetryAge)")
                .font(.caption2).foregroundStyle(.secondary)
        }
    }

    private var clusterSection: some View {
        section("PEERS · REACHABLE ESTIMATE") {
            if model.snapshot?.peers?.known == 0 {
                Text("No other peers discovered").font(.callout)
                Text("This Mac can run local inference on its own when its backend and resources are ready.")
                    .font(.caption).foregroundStyle(.secondary)
            } else {
                row("Peers", "\(model.value("peers", "reachable")) reachable · \(model.value("peers", "known")) known · \(model.value("peers", "stale")) stale")
            }
            row("Remote slots", model.presentation.remoteSlots, id: "remoteSlotsValue")
                .help("An advisory estimate from fresh peer observations. The destination worker must admit each request.")
            row("Capacity waiters", model.value("waiters"))
            if let peers = model.status["peer_details"] as? [[String: Any]], !peers.isEmpty {
                DisclosureGroup("Device details") {
                    ForEach(Array(peers.enumerated()), id: \.offset) { _, peer in
                        VStack(alignment: .leading, spacing: 4) {
                            Text((peer["local"] as? Bool) == true ? "This Mac · \(peer["hostname"] as? String ?? "Local")" : "\(peer["hostname"] as? String ?? peer["worker_id"] as? String ?? "Peer") · \((peer["reachable"] as? Bool) == true ? "Reachable" : "Unreachable")")
                                .font(.caption.bold())
                            Text("\(StatusPresentation.label(peer["platform"] as? String)) · \(StatusPresentation.label(peer["backend"] as? String))")
                            Text("Models: \((peer["models"] as? [String]).map { $0.isEmpty ? "None advertised" : $0.joined(separator: ", ") } ?? "Not reported")")
                            if let reason = peer["reason"] as? String { Text(StatusPresentation.label(reason)).foregroundStyle(.secondary) }
                        }.font(.caption).textSelection(.enabled).padding(.vertical, 4)
                    }
                }.font(.callout)
            }
        }
    }

    private var sharingSection: some View {
        section("MODEL INVENTORY & TRANSFERS") {
            if let error = model.sharingError { Text(error).font(.caption).foregroundStyle(.orange) }
            if model.inventory.isEmpty && model.sharingError == nil {
                Text("No shareable models found").font(.callout)
                Text("Inventory lists models eligible for sharing, including models advertised by peers.")
                    .font(.caption).foregroundStyle(.secondary)
            }
            ForEach(model.inventory.prefix(5)) { item in
                Text("\(item.model_id) · \(item.format ?? "Format not reported") · \((item.total_bytes ?? item.size_bytes).map { ByteCountFormatter.string(fromByteCount: $0, countStyle: .file) } ?? "Size not reported")")
                    .font(.caption).lineLimit(2)
            }
            if model.inventory.count > 5 { Text("\(model.inventory.count - 5) more models available in the copy dialog").font(.caption).foregroundStyle(.secondary) }
            ForEach(model.transfers) { transfer in
                VStack(alignment: .leading) {
                    HStack {
                        Text("\(transfer.model_id ?? "Model unavailable") · \(StatusPresentation.label(transfer.state))").lineLimit(2)
                        Spacer()
                        if ["queued", "transferring", "downloading"].contains(transfer.state) {
                            Button("Pause") { model.transferAction(transfer, action: "pause") }.disabled(!model.connected || model.busy)
                        } else if ["paused", "failed", "waiting", "rejected"].contains(transfer.state) {
                            Button("Retry…") { retryTransfer = transfer }.disabled(!model.connected || model.busy)
                        }
                    }
                    if transfer.total_bytes > 0 {
                        ProgressView(value: Double(min(transfer.total_bytes, max(0, transfer.completed_bytes))), total: Double(transfer.total_bytes))
                        Text("\(ByteCountFormatter.string(fromByteCount: transfer.completed_bytes, countStyle: .file)) / \(ByteCountFormatter.string(fromByteCount: transfer.total_bytes, countStyle: .file))").font(.caption)
                    }
                    if let reason = transfer.reason, !reason.isEmpty { Text(reason).font(.caption).foregroundStyle(.secondary).textSelection(.enabled) }
                }.font(.callout)
            }
            Button("Copy model from peer…") { showingTransfer = true }.disabled(!model.connected || model.busy)
        }
    }

    private var controlsSection: some View {
        section("PARTICIPATION") {
            HStack {
                modeButton("Automatic", "auto") { model.changeMode("auto") }
                modeButton("Drain", "draining") { model.changeMode("draining") }
                modeButton("Offline", "offline") { confirm = "offline" }.accessibilityLabel("Go offline")
                modeButton("Pause", "paused") { confirm = "paused" }.accessibilityLabel("Pause new inference")
            }.disabled(model.busy || !model.connected)
            Button("Offline for 1 hour") { model.changeMode("offline", duration: 3600) }.disabled(model.busy || !model.connected)
            Text("Offline keeps local inference available. Pause stops new inference. Running work finishes.")
                .font(.caption).foregroundStyle(.secondary)
        }
    }

    private func modeButton(_ title: String, _ mode: String, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            HStack(spacing: 3) {
                if model.snapshot?.mode.mode == mode { Image(systemName: "checkmark").font(.caption2) }
                Text(title)
            }
        }.help(model.snapshot?.mode.mode == mode ? "Current mode: \(title)" : "Switch to \(title)")
    }

    private var footer: some View {
        HStack(spacing: 8) {
            Button("Dashboard") { NSWorkspace.shared.open(URL(string: "http://127.0.0.1:11435/dashboard")!) }
            Button("Preferences…") { showingPreferences = true }
            Menu {
                Button("Diagnostics…") { Task { await loadDiagnostics() } }
                Button("Refresh now") { Task { await model.refresh() } }
                Divider()
                Button("Quit UI") { NSApplication.shared.terminate(nil) }
            } label: { Image(systemName: "ellipsis.circle") }
                .menuStyle(.borderlessButton).fixedSize().accessibilityLabel("More actions")
            Spacer(minLength: 0)
            Text("Daemon runs independently").font(.caption2).foregroundStyle(.secondary)
        }
    }

    func loadDiagnostics() async {
        do {
            let data = try await model.client.fetch("v1/node/diagnostics")
            // Display only reason/provenance fields, never arbitrary diagnostic payloads.
            let checks = data["checks"] as? [[String: Any]] ?? []
            diagnostics = checks.isEmpty ? "No diagnostic checks available." : checks.map { check in
                let title = check["title"] as? String ?? "Check"
                let state = check["state"] as? String ?? "unknown"
                let detail = check["detail"] as? String ?? ""
                let remedy = check["remedy"] as? String ?? ""
                return "\(title): \(state)\n\(detail)" + (remedy.isEmpty ? "" : "\nRemedy: \(remedy)")
            }.joined(separator: "\n\n")
        } catch { diagnostics = error.localizedDescription }
        showingDiagnostics = true
    }
}

struct PreferencesView: View {
    @ObservedObject var model: NodeModel
    @Environment(\.dismiss) var dismiss
    @AppStorage("hideJobCount") private var hideCount = false
    @State private var launchAtLogin = SMAppService.mainApp.status == .enabled
    @State private var message = ""
    @State private var version: Int?
    @State private var idleSeconds = ""
    @State private var batteryFloor = ""
    @State private var backgroundPolicy = "idle_only"
    @State private var requireAC = false
    @State private var quietHours = false
    @State private var quietStart = ""
    @State private var quietEnd = ""
    @State private var peerMax = ""
    @State private var notificationsEnabled = false
    @State private var serviceEnabled: Bool?
    @State private var serviceSupported = false
    var body: some View {
        ScrollView { VStack(alignment: .leading, spacing: 12) {
            Text("Preferences").font(.headline)
            Toggle("Hide job count in menu bar", isOn: $hideCount)
            Toggle("Launch menu bar app at login", isOn: $launchAtLogin).onChange(of: launchAtLogin) { value in
                do { if value { try SMAppService.mainApp.register() } else { try SMAppService.mainApp.unregister() } }
                catch { message = error.localizedDescription; launchAtLogin = SMAppService.mainApp.status == .enabled }
            }
            Text("Daemon policy").font(.subheadline.bold())
            Picker("Background policy", selection: $backgroundPolicy) {
                Text("Always (subject to resource limits)").tag("always")
                Text("Only while idle").tag("idle_only")
                Text("When system usage is low").tag("utilization_only")
                Text("Manual approval").tag("manual")
                Text("Never").tag("never")
            }
            TextField("Idle time (seconds)", text: $idleSeconds)
            TextField("Battery floor (%)", text: $batteryFloor)
            Toggle("Require AC power", isOn: $requireAC)
            Toggle("Block peer work during quiet hours", isOn: $quietHours)
            Text("Host local time; equal start and end means all day.").font(.caption)
            TextField("Quiet start (minutes after midnight, 0–1439)", text: $quietStart)
            TextField("Quiet end (minutes after midnight, 0–1439)", text: $quietEnd)
            TextField("Maximum concurrent work per peer (0–128)", text: $peerMax)
            Toggle("Enable dashboard notifications", isOn: $notificationsEnabled)
            Text("Notifications are available in the browser dashboard and require browser permission.").font(.caption)
            Button("Save daemon policy") { Task { await savePolicy() } }.disabled(version == nil)
            if serviceSupported {
                HStack {
                    Text("Daemon start at login: \(serviceEnabled.map { $0 ? "enabled" : "disabled" } ?? "unknown")")
                    Button("Enable") { Task { await setService(true) } }
                    Button("Disable") { Task { await setService(false) } }
                }
            }
            Text("The daemon runs separately and is not stopped when this app quits.").font(.caption)
            Button("Import daemon control credential…") {
                let panel = NSOpenPanel(); panel.canChooseDirectories = false; panel.allowsMultipleSelection = false
                if panel.runModal() == .OK, let url = panel.url {
                    do { try CredentialStore.importFile(url); message = "Credential imported into Keychain."; Task { await model.refresh() } }
                    catch { message = error.localizedDescription }
                }
            }
            Text(message).font(.caption)
            Button("Close") { dismiss() }
        }.padding() }.frame(width: 420, height: 650).task { await loadPolicy() }
    }
    func loadPolicy() async {
        do {
            let data = try await model.client.fetch("v1/node/preferences")
            guard let values = data["values"] as? [String: Any], let revision = data["version"] as? Int else { throw ClientError.invalidResponse }
            version = revision
            idleSeconds = String(describing: values["idle_seconds"] ?? "")
            batteryFloor = String(describing: values["battery_floor_percent"] ?? "")
            backgroundPolicy = values["background_policy"] as? String ?? "idle_only"
            requireAC = values["require_ac"] as? Bool ?? false
            quietHours = values["quiet_hours_enabled"] as? Bool ?? false
            quietStart = String(describing: values["quiet_hours_start_minute"] ?? 1320)
            quietEnd = String(describing: values["quiet_hours_end_minute"] ?? 420)
            peerMax = String(describing: values["peer_max_concurrent"] ?? 128)
            notificationsEnabled = values["notifications_enabled"] as? Bool ?? false
            hideCount = values["hide_job_counts"] as? Bool ?? false
            if let service = try? await model.client.fetch("v1/node/service") {
                serviceSupported = service["supported"] as? Bool ?? false
                serviceEnabled = service["enabled"] as? Bool
            }
        } catch { message = error.localizedDescription }
    }
    func savePolicy() async {
        guard let revision = version, let idle = Double(idleSeconds), let floor = Double(batteryFloor), idle.isFinite, floor.isFinite else {
            message = "Enter valid numeric idle time and battery floor."; return
        }
        guard let start = Int(quietStart), (0...1439).contains(start),
              let end = Int(quietEnd), (0...1439).contains(end),
              let quota = Int(peerMax), (0...128).contains(quota) else {
            message = "Enter quiet hours from 0 to 1439 and a peer limit from 0 to 128."; return
        }
        do {
            try await model.client.updatePreferences(version: revision, values: ["idle_seconds": idle, "battery_floor_percent": floor, "background_policy": backgroundPolicy, "require_ac": requireAC, "quiet_hours_enabled": quietHours, "quiet_hours_start_minute": start, "quiet_hours_end_minute": end, "peer_max_concurrent": quota, "notifications_enabled": notificationsEnabled, "hide_job_counts": hideCount])
            await loadPolicy(); message = "Daemon policy saved."
        } catch { message = error.localizedDescription; await loadPolicy() }
    }
    func setService(_ enabled: Bool) async {
        do { try await model.client.setService(enabled: enabled); await loadPolicy(); message = "Daemon login setting updated." }
        catch { message = error.localizedDescription }
    }
}

struct TransferView: View {
    @ObservedObject var model: NodeModel
    @Environment(\.dismiss) var dismiss
    @State private var digest = ""
    @State private var manifestDigest = ""
    @State private var source = ""
    @State private var permission = false
    @State private var license = false
    private let retry: Bool
    init(model: NodeModel, retryTransfer: ModelTransfer? = nil) {
        self.model = model; self.retry = retryTransfer != nil
        _digest = State(initialValue: retryTransfer?.digest ?? "")
        _manifestDigest = State(initialValue: retryTransfer?.manifest_digest ?? "")
        _source = State(initialValue: retryTransfer?.source_worker_id ?? "")
    }
    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(retry ? "Retry model copy" : "Copy model from peer").font(.headline)
            Text("Choose an advertised model or enter details from a trusted peer. Review the license and provenance before copying.")
            if !retry { ScrollView { VStack(alignment: .leading) {
            ForEach(model.inventory) { item in
                Button("\(item.model_id) · \(item.source_worker_id ?? "source unknown")") {
                    digest = item.digest; manifestDigest = item.manifest_digest ?? ""; source = item.source_worker_id ?? ""
                    permission = false; license = false
                }
                Text("License: \(item.license ?? "unknown") · Provenance: \(item.provenance ?? "unknown")").font(.caption).textSelection(.enabled)
            }
            } }.frame(maxHeight: 150) }
            TextField("SHA-256 digest", text: $digest).disabled(retry)
            TextField("Manifest SHA-256 digest", text: $manifestDigest).disabled(retry)
            TextField("Source worker ID", text: $source)
            Toggle("I authorize this copy from the selected peer", isOn: $permission)
            Toggle("I have reviewed and accept the model's license", isOn: $license)
            HStack {
                Button("Cancel") { dismiss() }
                Button(retry ? "Retry copy" : "Start copy") {
                    model.startTransfer(digest: digest.trimmingCharacters(in: .whitespacesAndNewlines), manifestDigest: manifestDigest.trimmingCharacters(in: .whitespacesAndNewlines), source: source.trimmingCharacters(in: .whitespacesAndNewlines), licenseAccepted: license, retry: retry)
                    dismiss()
                }.disabled(!permission || !license || digest.isEmpty || manifestDigest.isEmpty || source.isEmpty || model.busy || !model.connected)
            }
        }.padding().frame(width: 440)
    }
}
