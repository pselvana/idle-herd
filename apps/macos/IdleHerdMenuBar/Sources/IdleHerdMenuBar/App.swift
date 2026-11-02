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
        do {
            let snapshot = try await client.fetch("v1/node/status")
            let jobSnapshot = try? await client.fetch("v1/node/jobs")
            try apply(snapshot)
            jobs = (jobSnapshot?["worker_leases"] as? [[String: Any]]) ?? []
            originJobs = (jobSnapshot?["jobs"] as? [[String: Any]]) ?? []
            do {
                let inventory = try await client.inventory()
                self.inventory = inventory
                if self.snapshot?.transfers == nil { self.transfers = try await client.transfers() }
                sharingError = nil
            } catch { inventory = self.snapshot?.model_inventory ?? []; sharingError = "Model sharing status unavailable: \(error.localizedDescription)" }
            connected = true; lastUpdate = Date(); error = nil
        } catch { connected = false; self.error = error.localizedDescription }
    }
    func apply(_ object: [String: Any]) throws {
        let validated = try NodeSnapshot.decode(object)
        snapshot = validated; status = object
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
        for key in keys { guard let dict = current as? [String: Any], let next = dict[key] else { return "unknown" }; current = next }
        if let value = current as? String { return value }
        if let value = current as? NSNumber { return value.stringValue }
        return "unknown"
    }
    var count: String { connected ? snapshot.map { String($0.slots.running) } ?? "unknown" : "unknown" }
    var eligibility: String {
        guard let idle = snapshot?.idle else { return "unknown" }
        return idle.reason ?? (idle.eligible ? "Eligible" : "unknown")
    }
    var availableSlots: String {
        guard let slots = snapshot?.slots else { return "unknown" }
        return String(max(0, slots.total - slots.occupied))
    }
    var models: String {
        guard let backend = status["backend"] as? [String: Any], let models = backend["models_loaded"] as? [String] else { return "unknown" }
        return models.isEmpty ? "none" : models.joined(separator: ", ")
    }
    func originLocation(_ job: [String: Any]) -> String {
        guard let worker = job["worker_id"] as? String else { return "unassigned or unknown" }
        guard let local = status["worker_id"] as? String else { return "location unknown" }
        return worker == local ? "local worker" : "remote worker"
    }
    var telemetryAge: String {
        guard let resource = status["resources"] as? [String: Any], let stamp = resource["observed_at"] as? Double else { return "unknown" }
        return "\(max(0, Int(Date().timeIntervalSince1970 - stamp)))s ago"
    }
    func jobDescription(_ job: [String: Any]) -> String {
        let name = job["model"] as? String ?? "unknown model"
        let state = job["state"] as? String ?? "unknown"
        let origin = (job["peer_origin"] as? Bool) == true ? "peer origin" : "local origin"
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
            Label(hideCount || !model.connected ? "Idle-Herd" : "Idle-Herd \(model.count)", systemImage: model.connected ? "circle.grid.2x2.fill" : "exclamationmark.circle")
                .onAppear { model.start() }
                .accessibilityLabel(model.connected ? "Idle-Herd, \(model.count) local jobs" : "Idle-Herd, disconnected")
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
        VStack(alignment: .leading, spacing: 9) {
            HStack { Text("Idle-Herd").font(.headline); Spacer(); Text(model.connected ? "Connected" : "Disconnected · stale").foregroundStyle(model.connected ? Color.secondary : Color.orange) }
            Text("Mode: \(model.snapshot?.mode.mode ?? "unknown") · Eligibility: \(model.eligibility)")
            if let last = model.lastUpdate { Text("Last updated \(last.formatted(date: .omitted, time: .standard))").font(.caption).foregroundStyle(.secondary) }
            Divider()
            Text("LOCAL JOBS").font(.caption.bold())
            Text("Running: \(model.count) · Queued: \(model.value("jobs", "queued")) · Reserved: \(model.value("slots", "reserved"))")
            ForEach(Array(model.jobs.prefix(5).enumerated()), id: \.offset) { _, job in
                Text(model.jobDescription(job))
                    .lineLimit(1)
            }
            Text("ORIGIN JOURNAL").font(.caption.bold())
            ForEach(Array(model.originJobs.prefix(3).enumerated()), id: \.offset) { _, job in
                HStack {
                    Text("\(job["model"] as? String ?? "unknown model") · \(job["state"] as? String ?? "unknown") · \(model.originLocation(job))").lineLimit(1)
                    Spacer()
                    if let id = job["job_id"] as? String,
                       let state = job["state"] as? String,
                       ["queued", "reserving", "running", "retry_pending", "unknown_outcome"].contains(state),
                       (job["cancel_requested"] as? Bool) != true {
                        Button("Cancel…") { cancelJobID = id }.disabled(!model.connected || model.busy)
                            .accessibilityLabel("Cancel origin job \(job["model"] as? String ?? "unknown model")")
                    }
                }
            }
            Text("RESOURCES").font(.caption.bold())
            Text("CPU: \(model.value("resources", "cpu_percent"))% · Memory available: \(model.value("resources", "memory_available_gb")) / \(model.value("resources", "memory_total_gb")) GB")
            Text("GPU: \(model.value("resources", "gpu_percent")) · Unoccupied slots: \(model.availableSlots) / \(model.value("slots", "total")) · Backend: \(model.value("backend", "state"))")
            Text("Loaded models: \(model.models)").lineLimit(2)
            Text("AC: \(model.value("resources", "on_ac")) · Battery: \(model.value("resources", "battery_percent"))% · Thermal: \(model.value("resources", "thermal_state"))")
            Text("Resource sample: \(model.telemetryAge) · \(model.value("resources", "source"))").font(.caption)
            Text("CLUSTER (reachable estimate)").font(.caption.bold())
            Text("Known: \(model.value("peers", "known")) · Reachable: \(model.value("peers", "reachable")) · Stale: \(model.value("peers", "stale"))")
            Text("Remote slots indicated: \(model.value("peers", "observed_available_slots"))")
            Text("MODEL INVENTORY & TRANSFERS").font(.caption.bold())
            if let error = model.sharingError { Text(error).font(.caption).foregroundStyle(.orange) }
            if model.inventory.isEmpty && model.sharingError == nil { Text("No models in inventory").font(.caption) }
            ScrollView { VStack(alignment: .leading) {
            ForEach(model.inventory) { item in
                Text("\(item.model_id) · \(item.format ?? "unknown format") · \((item.total_bytes ?? item.size_bytes).map { ByteCountFormatter.string(fromByteCount: $0, countStyle: .file) } ?? "size unknown")").lineLimit(1)
            }
            ForEach(model.transfers) { transfer in
                VStack(alignment: .leading) {
                    HStack {
                        Text("\(transfer.model_id ?? "unknown model") · \(transfer.state)").lineLimit(1)
                        Spacer()
                        if ["queued", "transferring", "downloading"].contains(transfer.state) {
                            Button("Pause") { model.transferAction(transfer, action: "pause") }.disabled(!model.connected || model.busy)
                        } else if ["paused", "failed", "waiting", "rejected"].contains(transfer.state) {
                            Button("Retry…") { retryTransfer = transfer }.disabled(!model.connected || model.busy)
                        }
                    }
                    if transfer.total_bytes > 0 {
                        ProgressView(value: Double(max(0, transfer.completed_bytes)), total: Double(transfer.total_bytes))
                        Text("\(ByteCountFormatter.string(fromByteCount: transfer.completed_bytes, countStyle: .file)) / \(ByteCountFormatter.string(fromByteCount: transfer.total_bytes, countStyle: .file))").font(.caption)
                    }
                    if let reason = transfer.reason, !reason.isEmpty { Text(reason).font(.caption).foregroundStyle(.secondary).textSelection(.enabled) }
                }
            }
            } }.frame(maxHeight: 180)
            Button("Copy model from peer…") { showingTransfer = true }.disabled(!model.connected || model.busy)
            Divider()
            HStack {
                Button("Automatic") { model.changeMode("auto") }
                Button("Drain") { model.changeMode("draining") }
                Button("Offline") { confirm = "offline" }.accessibilityLabel("Go offline")
                Button("Pause") { confirm = "paused" }.accessibilityLabel("Pause new inference")
            }.disabled(model.busy || !model.connected)
            Button("Offline for 1 hour") { model.changeMode("offline", duration: 3600) }.disabled(model.busy || !model.connected)
            Text("Offline keeps local inference available. Pause stops new inference. Running work finishes.").font(.caption).foregroundStyle(.secondary)
            if let error = model.error { Text(error).foregroundStyle(.red).font(.caption).textSelection(.enabled) }
            if let error = model.actionError {
                HStack {
                    Text(error).foregroundStyle(.red).font(.caption).textSelection(.enabled)
                    Button("Dismiss") { model.actionError = nil }
                }
            }
            Divider()
            HStack {
                Button("Dashboard") { NSWorkspace.shared.open(URL(string: "http://127.0.0.1:11435/dashboard")!) }
                Button("Preferences…") { showingPreferences = true }
                Button("Diagnostics…") { Task { await loadDiagnostics() } }
                Spacer()
                Button("Quit UI") { NSApplication.shared.terminate(nil) }
            }
        }.padding(14).frame(width: 440)
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
    func loadDiagnostics() async {
        do {
            let data = try await model.client.fetch("v1/node/diagnostics")
            // Display only reason/provenance fields, never arbitrary diagnostic payloads.
            diagnostics = ["service", "peer_discovery"].map { "\($0): \(data[$0] as? String ?? "unknown")" }.joined(separator: "\n")
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
    @State private var serviceEnabled: Bool?
    @State private var serviceSupported = false
    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Preferences").font(.headline)
            Toggle("Hide job count in menu bar", isOn: $hideCount)
            Toggle("Launch menu bar app at login", isOn: $launchAtLogin).onChange(of: launchAtLogin) { value in
                do { if value { try SMAppService.mainApp.register() } else { try SMAppService.mainApp.unregister() } }
                catch { message = error.localizedDescription; launchAtLogin = SMAppService.mainApp.status == .enabled }
            }
            Text("Daemon policy").font(.subheadline.bold())
            Picker("Background policy", selection: $backgroundPolicy) {
                ForEach(["always", "idle_only", "utilization_only", "manual", "never"], id: \.self) { Text($0) }
            }
            TextField("Idle time (seconds)", text: $idleSeconds)
            TextField("Battery floor (%)", text: $batteryFloor)
            Toggle("Require AC power", isOn: $requireAC)
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
        }.padding().frame(width: 420).task { await loadPolicy() }
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
        do {
            try await model.client.updatePreferences(version: revision, values: ["idle_seconds": idle, "battery_floor_percent": floor, "background_policy": backgroundPolicy, "require_ac": requireAC])
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
