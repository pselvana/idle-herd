import SwiftUI
import AppKit
import ServiceManagement
import IdleHerdCore

@MainActor final class NodeModel: ObservableObject {
    @Published var status: [String: Any] = [:]
    @Published var snapshot: NodeSnapshot?
    @Published var jobs: [[String: Any]] = []
    @Published private(set) var jobsCurrent = false
    @Published var originJobs: [[String: Any]] = []
    @Published private(set) var originJobsCurrent = false
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
                    jobsCurrent = jobSnapshot["worker_leases"] is [[String: Any]]
                    originJobs = (jobSnapshot["jobs"] as? [[String: Any]]) ?? []
                    originJobsCurrent = jobSnapshot["jobs"] is [[String: Any]]
                    jobsError = nil
                } catch {
                    jobsCurrent = false
                    originJobsCurrent = false
                    jobsError = "Job details unavailable: \(error.localizedDescription)"
                }
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
        jobsCurrent = object["worker_leases"] is [[String: Any]]
        originJobsCurrent = object["job_history"] is [[String: Any]]
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
    func startTransfer(digest: String, manifestDigest: String, source: String, licenseAccepted: Bool, retry: Bool) async throws {
        actionError = nil; busy = true
        defer { busy = false }
        try await client.startTransfer(digest: digest, manifestDigest: manifestDigest, sourceWorkerID: source, licenseAccepted: licenseAccepted, retry: retry)
        await refresh()
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
    var menuBarActivity: MenuBarActivity {
        MenuBarActivity(workerLeases: jobs, originJobs: originJobs,
                        localWorkerID: snapshot?.worker_id,
                        connectedRemoteHosts: snapshot?.peers?.reachable ?? 0,
                        outboundRunning: snapshot?.outbound_running, current: connected,
                        workerLeasesCurrent: jobsCurrent, originJobsCurrent: originJobsCurrent)
    }
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
    @StateObject private var preferences = PreferencesWindowController()
    @AppStorage("hideJobCount") private var hideCount = false
    var body: some Scene {
        MenuBarExtra {
            ContentView(model: model, showPreferences: { preferences.show(model: model) })
                .onAppear { model.start() }
        } label: {
            Label {
                Text(hideCount || (model.status["hide_job_counts"] as? Bool == true) || !model.connected ? "Idle-Herd" : "Idle-Herd \(model.count)")
            } icon: {
                Image(nsImage: HerdMenuBarIcon.image(for: model.menuBarActivity))
            }
                .onAppear { model.start() }
                .accessibilityLabel(model.connected ? ((hideCount || model.status["hide_job_counts"] as? Bool == true) ? "Idle-Herd, connected" : "Idle-Herd, \(model.count) local jobs") : "Idle-Herd, disconnected")
                .accessibilityValue(model.menuBarActivity.accessibilityDescription)
        }.menuBarExtraStyle(.window)
    }
}

/// Create Preferences on demand so a menu-bar-only launch never opens a window.
@MainActor final class PreferencesWindowController: NSObject, ObservableObject, NSWindowDelegate {
    private var window: NSWindow?

    func show(model: NodeModel) {
        if let window {
            window.makeKeyAndOrderFront(nil)
        } else {
            let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 660, height: 740),
                                  styleMask: [.titled, .closable, .miniaturizable],
                                  backing: .buffered, defer: false)
            window.title = "Idle-Herd Preferences"
            window.isReleasedWhenClosed = false
            window.delegate = self
            let content = NSHostingView(rootView: PreferencesView(model: model, onClose: { [weak self] in
                self?.window?.close()
            }))
            window.contentView = content
            window.setContentSize(content.fittingSize)
            window.center()
            self.window = window
            window.makeKeyAndOrderFront(nil)
        }
        NSApplication.shared.activate(ignoringOtherApps: true)
    }

    func windowWillClose(_ notification: Notification) {
        window = nil
    }
}

struct ContentView: View {
    @ObservedObject var model: NodeModel
    var showPreferences: () -> Void = {}
    @Environment(\.colorScheme) private var colorScheme
    @State private var confirm: String?
    @State private var confirmDuration: Int?
    @State private var cancelJobID: String?
    @State private var showingDiagnostics = false
    @State private var showingTransfer = false
    @State private var retryTransfer: ModelTransfer?
    @State private var diagnostics = ""
    var body: some View {
        VStack(spacing: 0) {
            header.padding(20)
            Divider()
            ScrollView {
                VStack(alignment: .leading, spacing: 12) {
                    if let error = model.error {
                        Label(error, systemImage: "exclamationmark.triangle")
                            .font(.caption).foregroundStyle(.orange).textSelection(.enabled)
                    }
                    jobsSection
                    clusterSection
                    DisclosureGroup("Resources & backend") { resourcesSection.padding(.top, 8) }
                        .font(.callout)
                    DisclosureGroup("Model inventory & transfers") {
                        sharingSection.padding(.top, 8)
                    }.font(.callout)
                    if let error = model.actionError {
                        HStack {
                            Text(error).foregroundStyle(.red).font(.caption).textSelection(.enabled)
                            Button("Dismiss") { model.actionError = nil }
                        }
                    }
                }.padding(20)
            }
            Divider()
            controlsSection.padding(20)
            Divider()
            footer.padding(.horizontal, 20).padding(.vertical, 14)
        }
        .frame(width: 480, height: min(760, max(360, (NSScreen.main?.visibleFrame.height ?? 850) - 90)))
        .background(panelBackground)
        .tint(.blue)
            .alert(confirm == "offline" ? "Go offline?" : "Pause new inference?", isPresented: Binding(get: { confirm != nil }, set: { if !$0 { confirm = nil; confirmDuration = nil } })) {
                Button(confirm == "offline" ? "Go offline" : "Pause") {
                    if let mode = confirm { model.changeMode(mode, duration: confirmDuration) }
                    confirm = nil; confirmDuration = nil
                }
                Button("Cancel", role: .cancel) { confirm = nil; confirmDuration = nil }
            } message: {
                Text((confirm == "offline" ? "Peer work stops; local inference remains available. Running work will finish." : "New local and remote inference will be refused. Running work will finish.")
                     + (confirmDuration == 3600 ? " Automatic participation resumes in 1 hour." : ""))
            }
            .alert("Cancel this job?", isPresented: Binding(get: { cancelJobID != nil }, set: { if !$0 { cancelJobID = nil } })) {
                Button("Cancel job", role: .destructive) { if let id = cancelJobID { model.cancelJob(id) }; cancelJobID = nil }
                Button("Keep job", role: .cancel) { cancelJobID = nil }
            } message: { Text("Cancellation is best effort. The outcome may remain unknown.") }
            .sheet(isPresented: $showingTransfer) { TransferView(model: model) }
            .sheet(item: $retryTransfer) { transfer in TransferView(model: model, retryTransfer: transfer) }
            .sheet(isPresented: $showingDiagnostics) {
                VStack(alignment: .leading) { Text("Diagnostics").font(.headline); ScrollView { Text(diagnostics).textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading) }; Button("Close") { showingDiagnostics = false } }.padding().frame(width: 480, height: 300)
            }
    }
    private var panelBackground: Color {
        colorScheme == .dark ? Color(red: 0.11, green: 0.115, blue: 0.13) : Color(nsColor: .windowBackgroundColor)
    }
    private var cardBackground: Color {
        colorScheme == .dark ? Color.white.opacity(0.035) : Color.white.opacity(0.65)
    }
    private var positiveColor: Color {
        colorScheme == .dark ? .mint : Color(red: 0, green: 0.42, blue: 0.32)
    }
    private var statusColor: Color { model.connected ? positiveColor : .orange }
    private var modeTitle: String {
        model.snapshot?.mode.mode == "auto" ? "Auto" : model.presentation.mode
    }
    private func requestMode(_ mode: String, duration: Int? = nil) {
        if mode == "offline" || mode == "paused" {
            confirmDuration = duration; confirm = mode
        } else { model.changeMode(mode, duration: duration) }
    }
    private func badge(_ title: String, color: Color) -> some View {
        Text(title).font(.system(size: 11, weight: .medium))
            .foregroundStyle(color).padding(.horizontal, 7).padding(.vertical, 3)
            .background(color.opacity(0.12), in: RoundedRectangle(cornerRadius: 6))
    }
    private func sectionTitle(_ title: String) -> some View {
        Text(title).font(.system(size: 11, weight: .semibold)).tracking(1.1)
            .foregroundStyle(.secondary)
    }
    private var header: some View {
        HStack(spacing: 12) {
            Image(systemName: "server.rack")
                .font(.system(size: 22, weight: .light))
                .foregroundStyle(LinearGradient(colors: [.cyan, .indigo], startPoint: .topTrailing, endPoint: .bottomLeading))
                .frame(width: 38, height: 38)
                .overlay(RoundedRectangle(cornerRadius: 11).strokeBorder(
                    LinearGradient(colors: [.cyan, .indigo], startPoint: .topTrailing, endPoint: .bottomLeading), lineWidth: 1.3))
            VStack(alignment: .leading, spacing: 6) {
                HStack(spacing: 8) {
                    Text("Idle-Herd").font(.system(size: 18, weight: .semibold))
                    HStack(spacing: 4) {
                        Circle().fill(statusColor).frame(width: 6, height: 6)
                        Text(model.connected ? "Connected" : (model.lastUpdate == nil ? "Connecting…" : "Disconnected · stale"))
                            .font(.system(size: 10, weight: .semibold))
                    }.foregroundStyle(statusColor)
                        .padding(.horizontal, 7).padding(.vertical, 4)
                        .background(statusColor.opacity(0.1), in: Capsule())
                }
                Text(model.connected ? model.presentation.eligibility : "Waiting for a fresh daemon snapshot")
                    .font(.system(size: 12)).foregroundStyle(.secondary).lineLimit(2)
                    .help("Incoming peer eligibility. Local inference follows its own policy and backend availability.")
                if let last = model.lastUpdate {
                    Text("\(model.connected ? "Updated" : "Last received") \(last.formatted(date: .omitted, time: .shortened))")
                        .font(.system(size: 10)).foregroundStyle(.secondary)
                }
            }
            Spacer(minLength: 0)
            if model.busy { ProgressView().controlSize(.small) }
            Menu {
                Button("Automatic") { requestMode("auto") }
                Button("Drain") { requestMode("draining") }
                Button("Offline…") { requestMode("offline") }
                Button("Pause…") { requestMode("paused") }
                Divider()
                Button("Offline for 1 hour…") { requestMode("offline", duration: 3600) }
            } label: {
                Text(modeTitle).font(.system(size: 12, weight: .medium))
            }.menuStyle(.borderlessButton)
                .fixedSize().padding(.horizontal, 10).padding(.vertical, 7)
                .background(Color.primary.opacity(0.05), in: RoundedRectangle(cornerRadius: 8))
                .overlay(RoundedRectangle(cornerRadius: 8).strokeBorder(Color.primary.opacity(0.1)))
                .disabled(model.busy || !model.connected)
                .accessibilityLabel("Mode: \(model.presentation.mode)")
        }
    }

    private func section<Content: View>(_ title: String, @ViewBuilder content: () -> Content) -> some View {
        VStack(alignment: .leading, spacing: 9) {
            sectionTitle(title)
            content()
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(14)
        .background(cardBackground, in: RoundedRectangle(cornerRadius: 14))
        .overlay(RoundedRectangle(cornerRadius: 14).strokeBorder(Color.primary.opacity(0.08)))
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
        VStack(alignment: .leading, spacing: 14) {
            HStack {
                Circle().fill(model.connected && (model.snapshot?.slots.running ?? 0) > 0 ? Color.blue : Color.secondary)
                    .frame(width: 7, height: 7)
                sectionTitle("ACTIVE INFERENCE")
                Spacer()
                badge(model.connected ? "\(model.count) running" : "Last received", color: model.connected ? .blue : .orange)
            }
            if let error = model.jobsError { Text(error).font(.caption).foregroundStyle(.orange) }
            if model.jobs.isEmpty {
                VStack(alignment: .leading, spacing: 5) {
                    Text(model.snapshot == nil ? "Waiting for status" : ((model.snapshot?.slots.running ?? 0) > 0 ? "Waiting for job details" : "No active inference"))
                        .font(.system(size: 17, weight: .semibold))
                    Text(model.connected ? "New work appears here when admitted by this worker." : "Job status is unavailable until the daemon reconnects.")
                        .font(.caption).foregroundStyle(.secondary)
                }.padding(.vertical, 4)
            }
            TimelineView(.periodic(from: .now, by: 1)) { context in
                VStack(alignment: .leading, spacing: 12) {
                    ForEach(Array(model.jobs.prefix(3).enumerated()), id: \.offset) { index, job in
                        if index > 0 { Divider() }
                        inferenceRow(job, now: model.connected ? context.date : (model.lastUpdate ?? context.date))
                    }
                }
            }
            if model.jobs.count > 3 {
                Text("+\(model.jobs.count - 3) more worker leases · see Dashboard")
                    .font(.caption).foregroundStyle(.secondary)
            }
            if let slots = model.snapshot?.slots, slots.total > 0 {
                // Occupancy is measurable; inference completion and token speed are not.
                GeometryReader { geometry in
                    Capsule().fill(Color.primary.opacity(0.08))
                    Capsule().fill(LinearGradient(colors: [.blue, .indigo], startPoint: .leading, endPoint: .trailing))
                        .frame(width: geometry.size.width * min(1, Double(slots.occupied) / Double(slots.total)))
                }.frame(height: 6)
                    .accessibilityLabel("Local slot occupancy")
                    .accessibilityValue("\(slots.occupied) of \(slots.total)")
                HStack {
                    Text("Slots: \(slots.occupied)/\(slots.total) occupied")
                    Spacer()
                    Text("\(model.value("jobs", "queued")) queued · \(model.value("slots", "reserved")) reserved")
                }.font(.system(size: 10, design: .monospaced)).foregroundStyle(.secondary)
            }
            if let quarantined = model.snapshot?.slots.quarantined, quarantined > 0 {
                Label("\(quarantined) quarantined · capacity held until confirmed recovery", systemImage: "exclamationmark.triangle")
                    .font(.caption).foregroundStyle(.orange)
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
            }.font(.caption)
        }
        .padding(16)
        .background(LinearGradient(colors: [Color.blue.opacity(0.065), cardBackground], startPoint: .topTrailing, endPoint: .bottomLeading), in: RoundedRectangle(cornerRadius: 16))
        .overlay(RoundedRectangle(cornerRadius: 16).strokeBorder(Color.primary.opacity(0.1)))
    }

    private func inferenceRow(_ job: [String: Any], now: Date) -> some View {
        let state = job["state"] as? String
        return VStack(alignment: .leading, spacing: 6) {
            HStack(alignment: .firstTextBaseline) {
                Text(job["model"] as? String ?? "Model unavailable")
                    .font(.system(size: 17, weight: .semibold)).lineLimit(2).textSelection(.enabled)
                Spacer(minLength: 8)
                Text(StatusPresentation.label(state))
                    .font(.system(size: 12, weight: .medium, design: .monospaced))
                    .foregroundStyle(state == "running" && model.connected ? positiveColor : Color.orange)
            }
            HStack(alignment: .top) {
                Text(StatusPresentation.jobOrigin(job)).lineLimit(2).textSelection(.enabled)
                Spacer(minLength: 8)
                if let created = job["created_at"] as? Double, created.isFinite, created > 0 {
                    Text("\(max(0, Int(now.timeIntervalSince1970 - created)))s elapsed").monospacedDigit()
                }
            }.font(.system(size: 12)).foregroundStyle(.secondary)
            if let recovery = StatusPresentation.quarantineRecovery(job, now: now) {
                Label(recovery, systemImage: "exclamationmark.triangle")
                    .font(.caption).foregroundStyle(.orange).fixedSize(horizontal: false, vertical: true)
            }
        }
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

    private var remotePeers: [[String: Any]] {
        (model.status["peer_details"] as? [[String: Any]] ?? []).filter { ($0["local"] as? Bool) != true }
    }
    private var clusterSection: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                sectionTitle("CLUSTER TOPOLOGY (\(model.snapshot.map { ($0.peers?.known ?? 0) + 1 }.map(String.init) ?? "—") NODES)")
                Spacer(minLength: 6)
                HStack(spacing: 4) {
                    Circle().fill(model.connected ? positiveColor : Color.orange).frame(width: 6, height: 6)
                    Text(model.presentation.remoteSlots).accessibilityIdentifier("remoteSlotsValue")
                    Text(model.connected ? (model.snapshot?.peers?.observed_available_slots == 1 ? "remote slot" : "remote slots") : "slots · last seen")
                }.font(.system(size: 10, weight: .medium, design: .monospaced))
                    .foregroundStyle(model.connected ? positiveColor : Color.orange)
                    .help("Advisory estimate from fresh authenticated peer observations. Each worker admits its own work.")
            }
            HStack(spacing: 12) {
                deviceIcon("desktopcomputer")
                VStack(alignment: .leading, spacing: 5) {
                    HStack(spacing: 6) {
                        Text("This Mac").font(.system(size: 15, weight: .semibold))
                        badge("Host", color: .secondary)
                    }
                    Text("\(StatusPresentation.label(model.status["platform"] as? String, fallback: "Platform not reported")) · \(model.presentation.backend)")
                        .font(.system(size: 11)).foregroundStyle(.secondary).lineLimit(2)
                }
                Spacer(minLength: 4)
                VStack(alignment: .trailing, spacing: 5) {
                    Text("\(model.presentation.cpu) CPU").font(.system(size: 13, design: .monospaced))
                    Text(localMemory).font(.system(size: 11, design: .monospaced)).foregroundStyle(.secondary)
                }.fixedSize()
            }.padding(13).background(cardBackground, in: RoundedRectangle(cornerRadius: 14))
                .overlay(RoundedRectangle(cornerRadius: 14).strokeBorder(Color.primary.opacity(0.08)))
            ForEach(Array(remotePeers.enumerated()), id: \.offset) { _, peer in
                peerCard(peer)
            }
            if model.snapshot?.peers?.known == 0 {
                Text("No other peers discovered").font(.caption).foregroundStyle(.secondary)
                Text("This Mac can run local inference on its own when its backend and resources are ready.")
                    .font(.caption).foregroundStyle(.secondary)
            } else if remotePeers.isEmpty {
                Text("Peer device details not reported by this daemon.").font(.caption).foregroundStyle(.secondary)
            }
            HStack {
                Text("\(model.value("peers", "reachable")) reachable · \(model.value("peers", "stale")) stale")
                Spacer()
                Text("\(model.value("waiters")) capacity waiters")
            }.font(.system(size: 10)).foregroundStyle(.secondary)
        }
    }
    private var localMemory: String {
        guard let value = model.snapshot?.resources?.memory_available_gb, value.isFinite, value >= 0 else { return "Memory not reported" }
        return "\(value.formatted(.number.precision(.fractionLength(1)))) GB free"
    }
    private func deviceIcon(_ name: String) -> some View {
        Image(systemName: name).font(.system(size: 18, weight: .regular)).foregroundStyle(.secondary)
            .frame(width: 34, height: 34)
            .background(Color.primary.opacity(0.025), in: RoundedRectangle(cornerRadius: 10))
            .overlay(RoundedRectangle(cornerRadius: 10).strokeBorder(Color.primary.opacity(0.1)))
    }
    private func peerBackend(_ backend: String?) -> String {
        switch backend {
        case "mlx": return "MLX"
        case "ollama": return "Ollama"
        default: return StatusPresentation.label(backend)
        }
    }
    private func peerCard(_ peer: [String: Any]) -> some View {
        let reachable = peer["reachable"] as? Bool == true
        let slots = peer["slots"] as? [String: Any]
        return HStack(alignment: .top, spacing: 12) {
            deviceIcon("network")
            VStack(alignment: .leading, spacing: 5) {
                Text(peer["hostname"] as? String ?? peer["worker_id"] as? String ?? "Unknown peer")
                    .font(.system(size: 15, weight: .semibold)).lineLimit(2).textSelection(.enabled)
                HStack(spacing: 5) {
                    badge(model.connected ? (reachable ? "Reachable" : "Unreachable") : "Last seen", color: reachable && model.connected ? positiveColor : .orange)
                    Text(peerBackend(peer["backend"] as? String))
                        .font(.system(size: 11)).foregroundStyle(.secondary)
                }
                Text(StatusPresentation.label(peer["platform"] as? String, fallback: "Platform not reported"))
                    .font(.system(size: 11)).foregroundStyle(.secondary)
                if let reason = peer["reason"] as? String ?? peer["eligibility_reason"] as? String {
                    Text(StatusPresentation.label(reason)).font(.caption2).foregroundStyle(.secondary)
                }
                DisclosureGroup("Models") {
                    Text((peer["models"] as? [String]).map { $0.isEmpty ? "None advertised" : $0.joined(separator: ", ") } ?? "Not reported")
                        .font(.caption).foregroundStyle(.secondary).textSelection(.enabled)
                }.font(.caption2)
            }
            Spacer(minLength: 0)
            VStack(alignment: .trailing, spacing: 6) {
                if let total = slots?["total"] as? Int {
                    Text("\(total) slots").font(.system(size: 12, design: .monospaced))
                        .padding(.horizontal, 7).padding(.vertical, 4)
                        .background(Color.primary.opacity(0.055), in: RoundedRectangle(cornerRadius: 6))
                } else { Text("Slots unknown").font(.caption2).foregroundStyle(.secondary) }
                if let running = slots?["running"] as? Int {
                    Text("\(running) running").font(.system(size: 10, design: .monospaced)).foregroundStyle(.secondary)
                }
                if let quarantined = slots?["quarantined"] as? Int, quarantined > 0 {
                    Text("\(quarantined) quarantined").font(.caption2).foregroundStyle(.orange)
                }
            }.fixedSize()
        }.padding(13).background(cardBackground, in: RoundedRectangle(cornerRadius: 14))
            .overlay(RoundedRectangle(cornerRadius: 14).strokeBorder(Color.primary.opacity(0.08)))
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
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                sectionTitle("PARTICIPATION MODE")
                Spacer()
                Button("Pause for 1 hr") { requestMode("paused", duration: 3600) }
                    .buttonStyle(.plain).foregroundStyle(.blue).font(.system(size: 12))
                    .disabled(model.busy || !model.connected)
            }
            HStack(spacing: 3) {
                modeButton("Auto", "auto")
                modeButton("Drain", "draining")
                modeButton("Offline", "offline").accessibilityLabel("Go offline")
                modeButton("Pause", "paused").accessibilityLabel("Pause new inference")
            }.padding(4)
                .background(Color.black.opacity(colorScheme == .dark ? 0.35 : 0.05), in: RoundedRectangle(cornerRadius: 13))
                .disabled(model.busy || !model.connected)
            Text(modeExplanation).font(.system(size: 12)).foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true).lineSpacing(3)
            if let expiry = model.snapshot?.mode.expires_at {
                Text("Returns to Automatic at \(Date(timeIntervalSince1970: expiry).formatted(date: .omitted, time: .shortened))")
                    .font(.caption).foregroundStyle(.secondary)
            }
        }
    }
    private var modeExplanation: String {
        switch model.snapshot?.mode.mode {
        case "auto": return "Accepts local and peer inference when worker policy and capacity allow."
        case "draining": return "Finishes existing peer work. Local inference and outbound dispatch remain available."
        case "offline": return "Withdraws from the cluster. Local inference remains available; running work finishes."
        case "paused": return "Refuses new local and peer inference. Running work finishes."
        default: return "Connect to the daemon to manage participation."
        }
    }
    private func modeButton(_ title: String, _ mode: String) -> some View {
        let selected = model.snapshot?.mode.mode == mode
        return Button { requestMode(mode) } label: {
            HStack(spacing: 5) {
                if selected { Image(systemName: "checkmark").font(.system(size: 11, weight: .bold)).foregroundStyle(positiveColor) }
                Text(title).font(.system(size: 13, weight: selected ? .semibold : .medium))
            }.frame(maxWidth: .infinity).padding(.vertical, 10)
                .foregroundStyle(selected ? Color.primary : Color.secondary)
                .background(selected ? Color.primary.opacity(0.12) : .clear, in: RoundedRectangle(cornerRadius: 10))
                .contentShape(RoundedRectangle(cornerRadius: 10))
        }.buttonStyle(.plain)
            .accessibilityAddTraits(selected ? .isSelected : [])
            .help(selected ? "Current mode: \(title)" : "Switch to \(title)")
    }

    private var footer: some View {
        HStack(spacing: 14) {
            Button { NSWorkspace.shared.open(URL(string: "http://127.0.0.1:11435/dashboard")!) } label: {
                HStack(spacing: 6) {
                    Text("Dashboard")
                    Image(systemName: "arrow.up.right.square").foregroundStyle(.secondary)
                }.font(.system(size: 12)).padding(.horizontal, 10).padding(.vertical, 7)
                    .background(Color.primary.opacity(0.05), in: RoundedRectangle(cornerRadius: 7))
                    .overlay(RoundedRectangle(cornerRadius: 7).strokeBorder(Color.primary.opacity(0.1)))
            }.buttonStyle(.plain)
            Button("Preferences…", action: showPreferences)
                .buttonStyle(.plain).font(.system(size: 12)).foregroundStyle(.secondary)
            Spacer(minLength: 0)
            HStack(spacing: 5) {
                Circle().fill(statusColor).frame(width: 6, height: 6)
                Text(model.connected ? "Daemon active" : "Daemon unavailable").font(.system(size: 10)).foregroundStyle(.secondary)
            }.help("The daemon runs independently. Quitting this UI does not stop inference.")
            Menu {
                Button("Diagnostics…") { Task { await loadDiagnostics() } }
                Button("Refresh now") { Task { await model.refresh() } }
                Divider()
                Button("Quit UI") { NSApplication.shared.terminate(nil) }
            } label: { Image(systemName: "ellipsis") }
                .menuStyle(.borderlessButton).menuIndicator(.hidden).fixedSize().accessibilityLabel("More actions")
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
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("hideJobCount") private var hideCount = false
    @State private var launchAtLogin = SMAppService.mainApp.status == .enabled
    @State private var tab: PreferenceTab = .general
    @State private var draft = PreferencesDraft()
    @State private var saved: PreferencesDraft?
    @State private var version: Int?
    @State private var working = false
    @State private var message = ""
    @State private var messageIsError = false
    @State private var needsReload = false
    @State private var serviceEnabled: Bool?
    @State private var serviceInstalled: Bool?
    @State private var serviceSupported = false
    @State private var serviceMessage: String?
    private var onClose: (() -> Void)?

    enum PreferenceTab: String, CaseIterable {
        case general = "General"
        case policy = "Daemon Policy"
        case power = "Power & Limits"
        case security = "Security & RPC"
    }

    init(model: NodeModel, initialTab: PreferenceTab = .general, onClose: (() -> Void)? = nil) {
        self.model = model
        self.onClose = onClose
        _tab = State(initialValue: initialTab)
    }

    private var panelBackground: Color {
        colorScheme == .dark ? Color(red: 0.065, green: 0.07, blue: 0.08) : Color(nsColor: .windowBackgroundColor)
    }
    private var cardBackground: Color {
        colorScheme == .dark ? Color.white.opacity(0.04) : Color.white.opacity(0.7)
    }
    private var positiveColor: Color {
        colorScheme == .dark ? .mint : Color(red: 0, green: 0.42, blue: 0.32)
    }
    private var hasChanges: Bool {
        guard let saved else { return false }
        // Compare effective numeric values, so 300 and 300.0 are equivalent.
        return (try? draft.changes(from: saved).isEmpty) != true
    }
    private var canEdit: Bool { version != nil && !working }

    var body: some View {
        VStack(spacing: 0) {
            header
            Divider()
            Picker("Preferences section", selection: $tab) {
                ForEach(PreferenceTab.allCases, id: \.self) { Text($0.rawValue).tag($0) }
            }.pickerStyle(.segmented).controlSize(.large).labelsHidden().padding(.horizontal, 44).padding(.vertical, 14)
            Divider()
            ScrollView {
                VStack(alignment: .leading, spacing: 24) {
                    if version == nil && !working {
                        Label("Daemon preferences unavailable. Import a control credential or reload to try again.", systemImage: "exclamationmark.triangle")
                            .font(.callout).foregroundStyle(.orange)
                    }
                    switch tab {
                    case .general: generalContent
                    case .policy: policyContent
                    case .power: powerContent
                    case .security: securityContent
                    }
                }.padding(26).frame(maxWidth: .infinity, alignment: .leading)
            }
            Divider()
            footer
        }
        .frame(width: 660, height: min(740, max(420, (NSScreen.main?.visibleFrame.height ?? 850) - 90)))
        .background(panelBackground).tint(.blue)
        .task { await loadPolicy() }
    }

    private var header: some View {
        HStack(spacing: 9) {
            Image(systemName: "server.rack").foregroundStyle(.blue).font(.system(size: 18))
            Text("Idle-Herd Preferences").font(.system(size: 15, weight: .semibold))
            Spacer()
            if working { ProgressView().controlSize(.small) }
            HStack(spacing: 5) {
                Circle().fill(model.connected ? positiveColor : .orange).frame(width: 6, height: 6)
                Text(model.connected ? "Daemon connected" : "Daemon unavailable")
                    .font(.system(size: 11, weight: .medium))
            }.foregroundStyle(model.connected ? positiveColor : .orange)
                .padding(.horizontal, 10).padding(.vertical, 6)
                .background((model.connected ? positiveColor : .orange).opacity(0.08), in: Capsule())
        }.padding(.horizontal, 26).padding(.vertical, 18)
    }
    private func heading(_ title: String, detail: String? = nil) -> some View {
        HStack {
            Text(title).font(.system(size: 11, weight: .semibold)).tracking(1)
            Spacer(minLength: 10)
            if let detail { Text(detail).font(.system(size: 10, design: .monospaced)).lineLimit(1) }
        }.foregroundStyle(.secondary)
    }
    private func card<Content: View>(@ViewBuilder content: () -> Content) -> some View {
        VStack(alignment: .leading, spacing: 0, content: content)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(cardBackground, in: RoundedRectangle(cornerRadius: 12))
            .overlay(RoundedRectangle(cornerRadius: 12).strokeBorder(Color.primary.opacity(0.08)))
    }
    private func settingToggle(_ title: String, detail: String, icon: String? = nil, value: Binding<Bool>) -> some View {
        Toggle(isOn: value) {
            HStack(alignment: .top, spacing: 11) {
                if let icon {
                    Image(systemName: icon).foregroundStyle(.blue).font(.system(size: 16))
                        .frame(width: 28, height: 28)
                        .background(Color.blue.opacity(0.08), in: RoundedRectangle(cornerRadius: 7))
                }
                VStack(alignment: .leading, spacing: 5) {
                    Text(title).font(.system(size: 14, weight: .medium))
                    Text(detail).font(.system(size: 12)).foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true).lineSpacing(3)
                }.padding(.trailing, 14)
            }.frame(maxWidth: .infinity, alignment: .leading)
        }.toggleStyle(.switch).controlSize(.regular).padding(16)
            .accessibilityLabel(title)
    }
    private func numericField(_ title: String, detail: String, unit: String, text: Binding<String>, id: String) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(title).font(.system(size: 13, weight: .medium))
            HStack {
                TextField(title, text: text).textFieldStyle(.plain).labelsHidden()
                    .accessibilityIdentifier(id)
                Text(unit).font(.system(size: 12)).foregroundStyle(.secondary)
            }.padding(11)
                .background(panelBackground.opacity(0.75), in: RoundedRectangle(cornerRadius: 7))
                .overlay(RoundedRectangle(cornerRadius: 7).strokeBorder(Color.primary.opacity(0.1)))
            Text(detail).font(.system(size: 11)).foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true).lineSpacing(3)
        }.frame(maxWidth: .infinity, alignment: .topLeading)
    }

    @ViewBuilder private var generalContent: some View {
        VStack(alignment: .leading, spacing: 12) {
            heading("APPLICATION BEHAVIOR")
            card {
                settingToggle("Hide job count in menu bar", detail: "Keep the Idle-Herd name and status icon visible without a running-job count. Applies immediately on this Mac.", value: Binding(get: { hideCount }, set: { hideCount = $0; draft.hideJobCounts = $0 }))
                    .disabled(working)
                Divider()
                settingToggle("Launch menu bar app at login", detail: "Open this companion when you sign in to macOS. Applies immediately; the daemon runs separately.", value: Binding(get: { launchAtLogin }, set: updateLaunchAtLogin))
                    .disabled(working)
            }
        }
        VStack(alignment: .leading, spacing: 12) {
            heading("DAEMON & NOTIFICATIONS")
            card {
                settingToggle("Enable dashboard notifications", detail: "Allow notifications in the browser dashboard. Browser permission is also required.", icon: "bell", value: $draft.notificationsEnabled)
                    .disabled(!canEdit)
                Divider()
                VStack(alignment: .leading, spacing: 10) {
                    HStack {
                        VStack(alignment: .leading, spacing: 5) {
                            Text("Start daemon at login").font(.system(size: 14, weight: .medium))
                            Text(serviceInstalled == false ? "User service not installed" : (serviceEnabled.map { $0 ? "Enabled for the installed user service" : "Disabled for the installed user service" } ?? "Login service status unavailable"))
                                .font(.system(size: 12)).foregroundStyle(.secondary)
                        }
                        Spacer()
                        if serviceSupported {
                            if let enabled = serviceEnabled {
                                Button(enabled ? "Disable" : "Enable") {
                                    Task { await setService(!enabled) }
                                }.disabled(working)
                            } else {
                                Button("Enable") { Task { await setService(true) } }.disabled(working)
                                Button("Disable") { Task { await setService(false) } }.disabled(working)
                            }
                        }
                    }
                    Text(serviceMessage ?? "Changes apply at the next login. Existing inference keeps running when this UI closes.")
                        .font(.system(size: 11)).foregroundStyle(.secondary).lineSpacing(3)
                }.padding(16)
            }
        }
    }
    @ViewBuilder private var policyContent: some View {
        VStack(alignment: .leading, spacing: 12) {
            heading("DAEMON EXECUTION POLICY", detail: model.status["hostname"] as? String)
            card {
                VStack(alignment: .leading, spacing: 16) {
                    Picker("Background policy", selection: $draft.backgroundPolicy) {
                        Text("Always (subject to resource limits)").tag("always")
                        Text("Only while idle").tag("idle_only")
                        Text("When system usage is low").tag("utilization_only")
                        Text("Manual approval").tag("manual")
                        Text("Never").tag("never")
                    }.font(.system(size: 13)).accessibilityIdentifier("backgroundPolicy")
                    Text(policyExplanation).font(.system(size: 12)).foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true).lineSpacing(3)
                    Divider()
                    numericField("Idle time", detail: "Seconds of inactivity before idle-only peer work can be considered. Unknown activity never grants idle-only work.", unit: "seconds", text: $draft.idleSeconds, id: "idleSecondsField")
                }.padding(16)
            }.disabled(!canEdit)
        }
        VStack(alignment: .leading, spacing: 12) {
            heading("QUIET HOURS")
            card {
                settingToggle("Block peer work during quiet hours", detail: "Use this host’s local time. Matching start and end times block peer work all day.", icon: "moon", value: $draft.quietHours)
                Divider()
                HStack(alignment: .top, spacing: 20) {
                    numericField("Quiet start", detail: "24-hour time, such as 22:00.", unit: "HH:mm", text: $draft.quietStart, id: "quietStartField")
                    numericField("Quiet end", detail: "An earlier end crosses midnight.", unit: "HH:mm", text: $draft.quietEnd, id: "quietEndField")
                }.padding(16).disabled(!draft.quietHours)
            }.disabled(!canEdit)
        }
    }
    private var policyExplanation: String {
        switch draft.backgroundPolicy {
        case "always": return "Peer work may run while you are active, subject to worker resource and power limits."
        case "idle_only": return "Accept peer work only after the host reports enough inactivity and resources are available."
        case "utilization_only": return "Use measured system utilization to decide when peer work is eligible."
        case "manual": return "Peer work requires manual approval; this dialog does not grant per-job approval."
        case "never": return "Do not accept background peer inference."
        default: return "Choose when this worker may accept background peer inference."
        }
    }
    @ViewBuilder private var powerContent: some View {
        VStack(alignment: .leading, spacing: 12) {
            heading("POWER & BATTERY")
            card {
                settingToggle("Require AC power", detail: "Accept new peer work only while connected to power. Changing this setting does not cancel running work.", icon: "bolt", value: $draft.requireAC)
                Divider()
                numericField("Battery floor", detail: "Minimum battery charge for peer contribution, from 0 to 100 percent.", unit: "%", text: $draft.batteryFloor, id: "batteryFloorField")
                    .padding(16)
            }.disabled(!canEdit)
        }
        VStack(alignment: .leading, spacing: 12) {
            heading("PEER CONTRIBUTION BUDGET")
            card {
                numericField("Maximum concurrent work per peer", detail: "Limit work admitted from each peer, from 0 to 128. Zero blocks new peer work. Local worker capacity and resource policy still apply.", unit: "jobs / peer", text: $draft.peerMax, id: "peerMaxField")
                    .padding(16)
            }.disabled(!canEdit)
            Label("These settings apply to this worker. Peers admit their own work independently.", systemImage: "desktopcomputer")
                .font(.system(size: 12)).foregroundStyle(.secondary)
        }
    }
    @ViewBuilder private var securityContent: some View {
        VStack(alignment: .leading, spacing: 12) {
            heading("LOCAL CONTROL CONNECTION")
            card {
                VStack(alignment: .leading, spacing: 14) {
                    Label("Authenticated loopback control", systemImage: "lock.shield")
                        .font(.system(size: 14, weight: .medium))
                    Text(model.client.baseURL.absoluteString).font(.system(size: 13, design: .monospaced))
                        .textSelection(.enabled)
                    Text("This companion connects to the local daemon. Control credentials are stored in macOS Keychain and are separate from peer RPC identity.")
                        .font(.system(size: 12)).foregroundStyle(.secondary).lineSpacing(3)
                    Divider()
                    Button("Import daemon control credential…") { importCredential() }.disabled(working)
                    Text("Choose the daemon’s owner-only control-credentials.json file. Importing does not rotate peer credentials or change LAN trust.")
                        .font(.system(size: 11)).foregroundStyle(.secondary).lineSpacing(3)
                }.padding(18)
            }
        }
        VStack(alignment: .leading, spacing: 12) {
            heading("PEER RPC")
            card {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Device identity & discovery").font(.system(size: 14, weight: .medium))
                    Text("Peer TLS, request signing and LAN discovery are managed by the daemon. This dialog does not change RPC endpoints or trust configuration.")
                        .font(.system(size: 12)).foregroundStyle(.secondary).lineSpacing(3)
                }.padding(18)
            }
        }
    }
    private var footer: some View {
        VStack(alignment: .leading, spacing: 12) {
            if !message.isEmpty {
                Label(message, systemImage: messageIsError ? "exclamationmark.triangle" : "checkmark.circle")
                    .font(.system(size: 12)).foregroundStyle(messageIsError ? Color.orange : positiveColor)
                    .fixedSize(horizontal: false, vertical: true).textSelection(.enabled)
                    .accessibilityIdentifier("preferencesMessage")
            }
            HStack(spacing: 12) {
                if needsReload || version == nil {
                    Button("Reload policy") { Task { await loadPolicy() } }.disabled(working)
                } else {
                    Button("Revert changes") {
                        if let saved { draft = saved; hideCount = saved.hideJobCounts }
                        report("Unsaved daemon edits reverted.")
                    }.buttonStyle(.plain).foregroundStyle(.secondary).disabled(!hasChanges || working)
                }
                Spacer()
                if hasChanges { Text("Unsaved changes").font(.system(size: 11)).foregroundStyle(.secondary) }
                Button("Close") {
                    if let onClose { onClose() } else { dismiss() }
                }.keyboardShortcut(.cancelAction)
                Button { Task { await savePolicy() } } label: {
                    Label("Save Daemon Policy", systemImage: "checkmark").padding(.horizontal, 6)
                }.buttonStyle(.borderedProminent).controlSize(.large)
                    .keyboardShortcut("s", modifiers: .command)
                    .disabled(!canEdit || !hasChanges || needsReload)
            }
        }.padding(.horizontal, 26).padding(.vertical, 16)
    }
    private func report(_ text: String, error: Bool = false) {
        message = text; messageIsError = error
    }
    private func applyPolicy(_ data: [String: Any]) throws {
        guard let values = data["values"] as? [String: Any], let revision = data["version"] as? Int, revision >= 1 else {
            throw ClientError.invalidResponse
        }
        let loaded = try PreferencesDraft(values: values)
        draft = loaded; saved = loaded; version = revision
        hideCount = loaded.hideJobCounts; needsReload = false
    }
    func loadPolicy() async {
        guard !working else { return }
        working = true
        defer { working = false }
        do {
            try applyPolicy(try await model.client.fetch("v1/node/preferences"))
            report("")
        } catch { report(error.localizedDescription, error: true) }
        await loadService()
    }
    private func loadService() async {
        do {
            let service = try await model.client.fetch("v1/node/service")
            serviceSupported = service["supported"] as? Bool ?? false
            serviceEnabled = service["enabled"] as? Bool
            serviceInstalled = service["installed"] as? Bool
            if !serviceSupported {
                serviceMessage = "Daemon login controls are not supported on this platform."
            } else if serviceInstalled == false {
                serviceMessage = "Enable installs the user service for future logins. The current daemon stays running."
            } else { serviceMessage = nil }
        } catch {
            serviceSupported = false; serviceEnabled = nil; serviceInstalled = nil
            serviceMessage = "Login service status unavailable: \(error.localizedDescription)"
        }
    }
    func savePolicy() async {
        guard !working, let revision = version, let saved, !needsReload else { return }
        do {
            let changes = try draft.changes(from: saved)
            guard !changes.isEmpty else { return }
            working = true
            defer { working = false }
            try await model.client.updatePreferences(version: revision, values: changes)
            // A successful PATCH is committed even if the following read fails.
            do {
                try applyPolicy(try await model.client.fetch("v1/node/preferences"))
                report("Daemon policy saved.")
            } catch {
                version = nil; needsReload = true
                report("Policy saved, but its latest revision could not be read. Reload before editing again.", error: true)
            }
            await model.refresh()
        } catch ClientError.response(409) {
            needsReload = true
            report("The daemon policy changed elsewhere. Your edits are still shown. Reload policy before saving again.", error: true)
        } catch { report(error.localizedDescription, error: true) }
    }
    func setService(_ enabled: Bool) async {
        guard !working else { return }
        working = true
        defer { working = false }
        do {
            try await model.client.setService(enabled: enabled)
            await loadService()
            report("Daemon login setting updated.")
        } catch { report(error.localizedDescription, error: true) }
    }
    private func updateLaunchAtLogin(_ enabled: Bool) {
        do {
            if enabled { try SMAppService.mainApp.register() }
            else { try SMAppService.mainApp.unregister() }
            launchAtLogin = enabled
        } catch {
            launchAtLogin = SMAppService.mainApp.status == .enabled
            report(error.localizedDescription, error: true)
        }
    }
    private func importCredential() {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = false; panel.allowsMultipleSelection = false
        guard panel.runModal() == .OK, let url = panel.url else { return }
        do {
            try CredentialStore.importFile(url)
            needsReload = true
            report("Credential imported into Keychain. Reload policy to connect.")
            Task { await model.refresh() }
        } catch { report(error.localizedDescription, error: true) }
    }
}

struct TransferView: View {
    @ObservedObject var model: NodeModel
    @Environment(\.dismiss) var dismiss
    @Environment(\.colorScheme) private var colorScheme
    @State private var source = ""
    @State private var catalog: [ModelInventoryItem] = []
    @State private var selectedID = ""
    @State private var permission = false
    @State private var license = false
    @State private var loading = false
    @State private var submitting = false
    @State private var catalogError: String?
    @State private var submitError: String?
    @State private var refreshID = UUID()
    private let retryTransfer: ModelTransfer?
    private var retry: Bool { retryTransfer != nil }

    init(model: NodeModel, retryTransfer: ModelTransfer? = nil) {
        self.model = model; self.retryTransfer = retryTransfer
        _source = State(initialValue: retryTransfer?.source_worker_id ?? "")
    }
    private var peers: [ModelPeer] {
        (model.snapshot?.peer_details ?? []).filter { !$0.local && $0.reachable }
            .sorted { $0.displayName.localizedStandardCompare($1.displayName) == .orderedAscending }
    }
    private var sourceAvailable: Bool { peers.contains { $0.id == source } }
    private var selected: ModelInventoryItem? { catalog.first { $0.id == selectedID } }
    private var canCopy: Bool {
        selected != nil && sourceAvailable && permission && license && !loading &&
            !model.busy && !submitting && model.connected
    }
    private var panelBackground: Color {
        colorScheme == .dark ? Color(red: 0.065, green: 0.07, blue: 0.08) : Color(nsColor: .windowBackgroundColor)
    }
    private var cardBackground: Color {
        colorScheme == .dark ? Color.white.opacity(0.04) : Color.white.opacity(0.7)
    }

    var body: some View {
        VStack(spacing: 0) {
            header
            Divider()
            ScrollView {
                VStack(alignment: .leading, spacing: 22) {
                    peerSection
                    modelsSection
                    if let selected { detailsSection(selected) }
                    consentSection
                }.padding(24).frame(maxWidth: .infinity, alignment: .leading)
            }
            Divider()
            footer
        }
        .frame(width: 600, height: min(720, max(420, (NSScreen.main?.visibleFrame.height ?? 850) - 90)))
        .background(panelBackground).tint(.blue)
        .onAppear { chooseInitialPeer() }
        .onChange(of: peers.map(\.id)) { _ in chooseInitialPeer() }
        .task(id: "\(source):\(sourceAvailable):\(refreshID)") { await loadCatalog() }
    }
    private var header: some View {
        HStack(spacing: 12) {
            Image(systemName: "square.and.arrow.down")
                .font(.system(size: 21, weight: .light)).foregroundStyle(.blue)
                .frame(width: 38, height: 38)
                .background(Color.blue.opacity(0.08), in: RoundedRectangle(cornerRadius: 11))
            VStack(alignment: .leading, spacing: 5) {
                Text(retry ? "Retry model copy" : "Copy model from peer")
                    .font(.system(size: 17, weight: .semibold))
                Text("Choose a peer, then browse the models it shares with this Mac.")
                    .font(.system(size: 12)).foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
            Spacer(minLength: 0)
            if loading || submitting { ProgressView().controlSize(.small) }
        }.padding(24)
    }
    private func heading(_ title: String) -> some View {
        Text(title).font(.system(size: 11, weight: .semibold)).tracking(1)
            .foregroundStyle(.secondary)
    }
    private func card<Content: View>(@ViewBuilder content: () -> Content) -> some View {
        VStack(alignment: .leading, spacing: 12, content: content)
            .padding(16).frame(maxWidth: .infinity, alignment: .leading)
            .background(cardBackground, in: RoundedRectangle(cornerRadius: 12))
            .overlay(RoundedRectangle(cornerRadius: 12).strokeBorder(Color.primary.opacity(0.08)))
    }
    private var peerSection: some View {
        VStack(alignment: .leading, spacing: 10) {
            heading("SOURCE PEER")
            card {
                HStack(spacing: 12) {
                    Image(systemName: "desktopcomputer").foregroundStyle(.blue)
                    Picker("Peer", selection: Binding(get: { source }, set: {
                        source = $0; clearSelection(); catalog = []; catalogError = nil
                    })) {
                        Text("Select a peer…").tag("")
                        if !source.isEmpty && !sourceAvailable {
                            Text("Previous peer · unavailable").tag(source)
                        }
                        ForEach(peers) { peer in Text(peer.displayName).tag(peer.id) }
                    }.accessibilityIdentifier("copyPeerPicker")
                        .disabled(peers.isEmpty || submitting)
                }
                Text(peers.isEmpty ? "No reachable peers. Connect a trusted peer to browse its shared models." : "Only models authorized for this Mac are available to copy.")
                    .font(.system(size: 12)).foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
    }
    private var modelsSection: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                heading("AVAILABLE MODELS")
                Spacer()
                Button { clearSelection(); catalog = []; refreshID = UUID() } label: {
                    Label("Refresh", systemImage: "arrow.clockwise")
                }.buttonStyle(.borderless).font(.system(size: 12))
                    .disabled(!sourceAvailable || loading || submitting || !model.connected)
                    .accessibilityIdentifier("refreshPeerModels")
            }
            card {
                if !model.connected {
                    notice("Daemon unavailable", detail: "Reconnect to browse models.", icon: "exclamationmark.triangle")
                } else if !sourceAvailable {
                    notice("Select a reachable peer", detail: "Its shared models will appear here.", icon: "network")
                } else if loading {
                    HStack(spacing: 10) {
                        ProgressView().controlSize(.small)
                        Text("Fetching models from peer…").font(.system(size: 13))
                    }.padding(.vertical, 12)
                } else if let catalogError {
                    notice("Couldn’t load models", detail: catalogError, icon: "exclamationmark.triangle")
                } else if catalog.isEmpty {
                    notice(retry ? "Original model unavailable" : "No shared models available",
                           detail: retry ? "Choose another peer or ask the source owner to share the original model revision again." : "The peer has no models authorized for this Mac. Ask its owner to register a model and grant sharing permission.",
                           icon: "shippingbox")
                } else {
                    ScrollView {
                        LazyVStack(spacing: 4) {
                            ForEach(catalog) { item in
                                Button {
                                    clearSelection(); selectedID = item.id
                                } label: {
                                    HStack(spacing: 12) {
                                        Image(systemName: selectedID == item.id ? "checkmark.circle.fill" : "circle")
                                            .foregroundStyle(selectedID == item.id ? Color.blue : Color.secondary)
                                        VStack(alignment: .leading, spacing: 5) {
                                            Text(item.model_id).font(.system(size: 13, weight: .medium)).foregroundStyle(.primary)
                                            Text(modelSummary(item)).font(.system(size: 11)).foregroundStyle(.secondary)
                                        }
                                        Spacer(minLength: 0)
                                    }.padding(10).contentShape(Rectangle())
                                        .background(selectedID == item.id ? Color.blue.opacity(0.09) : Color.clear, in: RoundedRectangle(cornerRadius: 8))
                                }.buttonStyle(.plain).disabled(submitting)
                                    .accessibilityLabel("Select \(item.model_id)")
                                    .accessibilityValue(selectedID == item.id ? "Selected" : "Not selected")
                            }
                        }
                    }.frame(height: min(210, CGFloat(catalog.count) * 60))
                }
            }
        }
    }
    private func notice(_ title: String, detail: String, icon: String) -> some View {
        HStack(alignment: .top, spacing: 12) {
            Image(systemName: icon).foregroundStyle(.secondary).font(.system(size: 20))
            VStack(alignment: .leading, spacing: 6) {
                Text(title).font(.system(size: 13, weight: .medium))
                Text(detail).font(.system(size: 12)).foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }.padding(.vertical, 8)
    }
    private func modelSummary(_ item: ModelInventoryItem) -> String {
        [item.format?.uppercased(), item.backend?.capitalized, item.quantization,
         item.size_bytes.map { ByteCountFormatter.string(fromByteCount: $0, countStyle: .file) }]
            .compactMap { $0 }.joined(separator: " · ")
    }
    private func detailsSection(_ item: ModelInventoryItem) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            heading("MODEL DETAILS")
            card {
                detail("License", value: item.license ?? "Not provided")
                Divider()
                detail("Provenance", value: item.provenance ?? "Not provided")
            }.textSelection(.enabled)
        }
    }
    private func detail(_ title: String, value: String) -> some View {
        VStack(alignment: .leading, spacing: 5) {
            Text(title).font(.system(size: 11)).foregroundStyle(.secondary)
            Text(value).font(.system(size: 13)).fixedSize(horizontal: false, vertical: true)
        }
    }
    private var consentSection: some View {
        VStack(alignment: .leading, spacing: 10) {
            heading("PERMISSION & LICENSE")
            card {
                Toggle("I authorize this copy from the selected peer", isOn: $permission)
                Divider()
                Toggle("I have reviewed and accept the model’s license", isOn: $license)
            }.toggleStyle(.checkbox).font(.system(size: 12))
                .disabled(selected == nil || loading || submitting || !sourceAvailable || !model.connected)
        }
    }
    private var footer: some View {
        VStack(alignment: .leading, spacing: 10) {
            if let submitError {
                Text(submitError).font(.system(size: 12)).foregroundStyle(.red).textSelection(.enabled)
                    .accessibilityIdentifier("copyError")
            }
            HStack {
                Text(retry ? "Resume the original model revision." : "The model will be installed on this Mac.")
                    .font(.system(size: 11)).foregroundStyle(.secondary)
                Spacer()
                Button("Cancel") { dismiss() }.keyboardShortcut(.cancelAction).disabled(submitting)
                Button(retry ? "Retry copy" : "Start copy") {
                    Task { await startCopy() }
                }.buttonStyle(.borderedProminent).keyboardShortcut(.defaultAction).disabled(!canCopy)
            }
        }.padding(.horizontal, 24).padding(.vertical, 18)
    }
    private func chooseInitialPeer() {
        if source.isEmpty, let first = peers.first { source = first.id }
    }
    private func clearSelection() {
        selectedID = ""; permission = false; license = false; submitError = nil
    }
    private func loadCatalog() async {
        clearSelection(); catalog = []; catalogError = nil
        guard !source.isEmpty && sourceAvailable else { loading = false; return }
        let requestedSource = source
        loading = true
        do {
            let items = try await model.client.inventory(sourceWorkerID: requestedSource)
            guard !Task.isCancelled, source == requestedSource else { return }
            catalog = items.filter { $0.canCopy(from: requestedSource, retrying: retryTransfer) }
                .sorted { $0.model_id.localizedStandardCompare($1.model_id) == .orderedAscending }
            if retry, let first = catalog.first { selectedID = first.id }
        } catch {
            guard !Task.isCancelled, source == requestedSource else { return }
            catalogError = "Check that the peer is online and sharing is enabled, then refresh. \(error.localizedDescription)"
        }
        loading = false
    }
    private func startCopy() async {
        guard canCopy, let selected, let manifestDigest = selected.manifest_digest else { return }
        submitting = true; submitError = nil
        defer { submitting = false }
        do {
            try await model.startTransfer(digest: selected.digest, manifestDigest: manifestDigest,
                                          source: source, licenseAccepted: license, retry: retry)
            dismiss()
        } catch { submitError = error.localizedDescription }
    }
}
