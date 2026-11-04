# Automatic LAN discovery

Run `uv run idle-herd` on each native machine. Nodes browse and advertise
`_idle-herd._tcp.local.` and automatically connect to other Idle-Herd nodes on
the LAN. No router, seeds, enrollment commands, first-node authority, admin quorum,
or menu bar app is required.

The daemon chooses a private IPv4 interface and an OS-assigned TCP port for its
separate TLS peer listener. Local ingress remains on 127.0.0.1:11435 and the backend
port map is unchanged. The socket is reserved before startup; advertising starts
after TLS is listening. Restarted workers replace their previous discovered
endpoint when the port changes. Native address changes update advertisements.
Without a private IPv4 interface, advertising waits for one; local inference
remains available. Explicit private IPv6 endpoints are also supported.

## Trust and admission

Automatic LAN trust is enabled by default: an Idle-Herd node that can advertise
on the LAN can join the inference mesh and request background work. Each node
automatically creates its own persistent identity. This mode treats the local
network as the authorization boundary for first contact.

Bounded Bonjour TXT records carry public certificates and a device signature
binding the identity to the resolved private IP/port. Private keys and owner
credentials are never advertised. Nodes validate the proof, certificate lifetime,
and resolved address, then persist device-specific pins in
`peer-identity/lan-peers.json`. Existing pins reject changes to a worker's key,
authority or identity domain. Subsequent connections still require verified TLS,
signed RPC/stream binding and durable nonce replay protection. Discovery supplies
candidates; a direct authenticated exchange must succeed before capacity is live.

Authenticated status polling also learns a bounded hostname for display. The peer
directory retains it across departures, restarts and endpoint changes. Job rows
show that origin hostname (or its known endpoint IP for older peers); hostname
labels do not grant trust or change routing. Bonjour descriptors remain compatible
with earlier workers.

The mesh federates independently initialized identity domains. Worker/cluster UUIDs,
durable lease/job history, and optional admin Raft membership are preserved.
Authority-issued revocations remain within their original identity domain. For
centrally managed enrollment/revocation, disable automatic LAN trust and use
[manual enrollment](phase-2.md). Existing automatic pins are ignored when that
mode is disabled. To replace a pinned key/authority, stop the daemon and remove
that worker's entries from `peer-identity/lan-peers.json` and `known-peers.json`.

Receiving workers enforce background/idle policy and durable admission on every
request; model sharing still requires explicit owner permission. Offline mode
rejects peer work immediately and withdraws browsing/advertising on the next
discovery update (at most five seconds). Local inference remains available.
Auto resumes discovery. Quitting the UI has no effect on Bonjour or the daemon.

## Settings

Configure the daemon environment. The legacy `~/.fleet-manager/env` is not loaded.

| Variable | Native default | Meaning |
| --- | --- | --- |
| `IDLE_HERD_MDNS_ENABLED` | `true` | Browse/advertise Bonjour; automatically allocate an unset peer endpoint. |
| `IDLE_HERD_LAN_AUTO_TRUST` | `true` | Trust valid LAN announcements and load saved LAN pins. False requires enrolled identities. |
| `IDLE_HERD_MDNS_SERVICE_TYPE` | `_idle-herd._tcp.local.` | Discovery namespace for separate deployments/tests; not an authentication boundary. |
| `IDLE_HERD_PEER_ENDPOINT` | automatic | Optional fixed private HTTPS literal-IP endpoint/port, outside the backend port map. |
| `IDLE_HERD_SEEDS` | `[]` | Optional candidate endpoints when multicast is unavailable. |

Strict enrolled clusters can keep Bonjour enabled: set only
`IDLE_HERD_LAN_AUTO_TRUST=false` and enroll devices under one authority. Bonjour
finds those peers without seed IPs. Fully manual deployments set both switches
false and configure explicit endpoints/seeds. User services retain their saved
settings; reinstall after changing settings.

Containers retain seeded/enrolled defaults: Docker bridge/Desktop networks do not
reliably carry physical-LAN multicast. Bonjour and LAN trust require explicit
environment opt-in and suitable container networking.

## Diagnosis and limits

Use `idle-herd peers`, `idle-herd doctor`, localhost `GET /health`, and
`GET /v1/cluster/peers`. Health reports `bonjour_error` independently of the TLS
listener. Multicast failures preserve local inference and available seeded TLS
connections. Rejected announcements log error types without untrusted TXT content.
Resolutions, trust pins and known endpoints are each bounded to 128. Bonjour
departure, failed probes and direct-exchange TTL withdraw stale capacity;
identity pins survive departure/restart.

Allow UDP 5353 multicast and the peer TCP listener on the intended LAN. mDNS is
link-local; routed subnets, client-isolated Wi-Fi, VPNs and Docker bridges may need
explicit seeds. Automatic selection advertises one IPv4 interface at a time;
physical multi-homed routing/network roaming need host-specific verification.
Real-multicast tests use a unique discovery namespace so fabricated workers never
join a live mesh. Credentials retain the existing 90-day maximum lifetime;
expired identities withdraw participation and require credential renewal.
