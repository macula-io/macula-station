# Plan: git-over-mesh — decentralized git as a core hecate capability

**Status:** Phase 1 in progress — foundational daemon apps scaffolded
**Created:** 2026-04-21
**Last Updated:** 2026-04-21
**Owner:** hecate-daemon + hecate-web (core capability, not a plugin)
**Consumers:** macula-realm (gitops), hecate-app-martha (persona repos), hecate-app-briefcase (archive snapshots), future plugins

## Goal

Make git a first-class mesh-native capability of Hecate:
- Every Hecate node can host bare git repositories
- Repositories are reachable by any other node through the Macula Mesh using existing V2 primitives (`macula:advertise/3`, `macula:call/4`, `macula:subscribe/3`)
- Existing `git` clients work unchanged through a small `git-remote-mesh` helper
- Browsing, cloning, pushing, issue-style comments, and subscription to ref updates happen over the mesh — no central git host required
- Developers on their workstation can `git clone mesh://<did>/<repo>` and it Just Works

No GitHub. No GitLab. No Radicle. No central host of any kind. Every Hecate node is potentially a git origin, reachable through the relay mesh we already operate.

## Non-goals

- **Full GitHub feature parity.** We're not building Actions-style CI, vulnerability scanning, or enterprise permissions in v1. Those can layer on later.
- **A decentralized git _protocol_ reinvention.** Git's own pack protocol is fine. We only replace the transport.
- **CRDT-based collaborative editing.** Patches and issues are append-only signed FACTs, not real-time collaborative documents.
- **Monorepo-scale (1M+ files).** v1 targets config repos, persona repos, small app repos. Big repos can come later once we understand streaming performance.
- **Replacing the `.git` object database.** Each repo is a normal bare git repo on disk. We serve it with standard `git upload-pack` / `git receive-pack`.

## Why this plan exists

`PLAN_DEFERRED_WORK.md` already references `PLAN_GIT_OVER_MESH.md` as a future application. The substrate is ready:

- Macula V2 shipped `macula:advertise/call/subscribe/3` (PART6 protocol, PART7 implementation)
- `procedure_advertisement` records are live (PART3 discovery)
- Station handler dispatch registry is operational

What's missing is the design of the actual git-over-mesh protocol built on those primitives, and the client tooling that bridges standard git into it. This plan fills that gap.

## Constraint driving the architecture

**Hecate nodes cannot open direct peer-to-peer sockets.** Every node maintains a single outbound QUIC connection to a Macula relay; inbound connections from other nodes are impossible (NAT, firewalls, identity-bound routing).

This rules out:
- Radicle's libp2p transport (assumes direct peer dialing)
- git-smart-HTTP listened on each node (no public ingress)
- SSH remotes for cross-node git (same problem)

It forces:
- All cross-node traffic rides Macula Mesh RPC + FACT primitives
- "Remote" in `git remote add` becomes a mesh procedure identifier, not an HTTP URL or SSH host

This turns out to be a *feature*: the existing relay mesh handles NAT traversal, identity, encryption, and authorization for free. We only need to pack git's bytes into mesh messages.

## Architecture

### Conceptual layer diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│  Developer's workstation                                            │
│                                                                     │
│  git clone mesh://did:realm:alice/config                            │
│    │                                                                │
│    ▼                                                                │
│  git-remote-mesh        ← small Rust binary or escript              │
│    │                                                                │
│    ▼                                                                │
│  SDK client (macula:call)                                           │
└─────────┬───────────────────────────────────────────────────────────┘
          │ QUIC
          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Macula relay (existing fleet)                                      │
│                                                                     │
│  Forwards RPC via mesh routing to target node                       │
└─────────┬───────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Hecate node (Alice)                                                │
│                                                                     │
│  serve_git_over_mesh                ← procedure advertised          │
│    ├── receives macula:call                                         │
│    ├── dispatches to git-upload-pack or git-receive-pack            │
│    ├── streams pack data back via RPC reply chunks                  │
│    └── emits ref-updated FACTs on successful push                   │
│                                                                     │
│  guide_repo_lifecycle               ← CMD: create, delete, rename   │
│  project_repos                      ← PRJ: repo catalog read model  │
│  query_repos                        ← QRY: list, search repos       │
│                                                                     │
│  /var/hecate/repos/<repo-id>.git    ← bare repo on disk             │
└─────────────────────────────────────────────────────────────────────┘
```

### Hecate-daemon umbrella apps (vertical slicing)

Per Hecate conventions. No horizontal "services/", "repos/", "helpers/" layers.

| App | Department | Role |
|---|---|---|
| `guide_repo_lifecycle` | CMD | Desks: `initiate_repo/`, `archive_repo/`, `rename_repo/`, `set_repo_description/`. Events: `repo_initiated_v1`, `repo_archived_v1`, `repo_renamed_v1`. Aggregate = one repo. |
| `project_repos` | PRJ | Projects repo-lifecycle events into ETS read models — the repo catalog per realm. |
| `query_repos` | QRY | Read facade: `list_repos_by_owner`, `get_repo_by_id`, `search_repos_by_tag`. |
| `serve_git_over_mesh` | Infrastructure | Advertises `mri:proc:{realm}/git/{repo_id}/fetch`, `.../push`, `.../refs`. Dispatches to `git-upload-pack` and `git-receive-pack` binaries. |
| `announce_ref_updates` | Infrastructure | Publishes `{realm}.git.{repo_id}.ref_updated_v1` FACTs to the mesh on successful push. Subscribers (clones watching for updates) receive in real-time. |

### hecate-web additions (Svelte)

New route: `/plugin/git` (or nested in daemon-level UI since this is core, not a plugin).

| Component | Purpose |
|---|---|
| `RepoList.svelte` | List repos: owner, name, description, last-update, mesh MRI |
| `RepoBrowser.svelte` | Tree browser — navigate directories, view files, click to see commit history |
| `CommitLog.svelte` | Scrollable commit log with author, message, timestamp, SHA |
| `DiffViewer.svelte` | Before/after diff for a commit or a blob |
| `RefSubscription.svelte` | Live badge: "master updated 30s ago" — driven by mesh FACT subscription |
| `CloneUrlCopy.svelte` | Copy `mesh://did:realm:alice/config` to clipboard |

Component calls flow: Svelte → fetch(`/api/git/...`) → hecate-web server-side → Unix socket → hecate-daemon → `query_repos` or `serve_git_over_mesh`.

### `git-remote-mesh` helper binary

Git's remote-helper protocol (`gitremote-helpers(7)`) lets an arbitrary executable named `git-remote-<scheme>` speak a documented line protocol to git, translating to any backend. Examples in the wild: `git-remote-dropbox`, `git-remote-ipfs`, `git-remote-s3`.

Ours is `git-remote-mesh`. Invoked automatically when user types:

```bash
git clone mesh://did:realm:alice/config
git push mesh://did:realm:alice/config master
git ls-remote mesh://did:realm:alice/config
```

Implementation:
- Language: Rust (single static binary, small, fast, cross-platform)
- Dependencies: quinn (QUIC client), macula-sdk (for advertise/call/subscribe), standard git pack libraries
- Workflow:
  1. Parse `mesh://<did>/<repo-path>` URL → `(realm_did, repo_id)`
  2. Resolve via macula DHT → procedure MRI `mri:proc:{realm}/git/{repo_id}/fetch`
  3. Open QUIC to user's nearest relay
  4. `macula:call(proc_mri, "upload-pack-request", pack_request_bytes)`
  5. Stream pack response bytes to git's stdout
  6. Reverse for push (receive-pack)

The helper is stateless per invocation. Auth keys (realm cert, capability tokens) picked up from `~/.config/hecate/identity.json` — same identity management as `hecate-cli`.

### RPC contract (the wire format)

Three procedures per repo, advertised by the owning node:

**`fetch`** — git upload-pack, for clone/fetch/ls-remote
```
Request:
  {
    operation:   "ls-refs" | "fetch",
    capabilities: [...],              // git protocol v2 capabilities
    want:        [sha, ...],          // refs the client wants
    have:        [sha, ...],          // refs the client already has
    depth:       integer | null,      // for shallow clones
    filter:      string | null        // partial clone spec
  }

Response (streamed in chunks via RPC reply iterator):
  {
    chunk_type: "pack" | "refs" | "ack" | "done",
    data:       binary                // raw git pack data or refs advertisement
  }
```

**`push`** — git receive-pack, for push
```
Request (streamed):
  {
    chunk_type: "command" | "pack" | "done",
    data:       binary                // ref updates, then pack data
  }

Response:
  {
    status:  "ok" | "error",
    reports: [{ref, status, message}, ...]
  }
```

**`describe`** — metadata, no git protocol involvement
```
Request: {}
Response:
  {
    name:        string,
    description: string,
    default_branch: string,
    owner_did:   string,
    tags:        [string, ...],
    size_bytes:  integer,
    last_push:   utc_datetime
  }
```

### FACT channels (for live ref updates and events)

**Per-repo ref updates:** `{realm}.git.{repo_id}.ref_updated_v1`
```
{
  repo_id: binary,
  ref:     binary,          // e.g. "refs/heads/master"
  old_sha: binary | null,   // null for new ref
  new_sha: binary,          // zero sha for deletion
  pusher:  did,             // who pushed
  timestamp: utc_datetime
}
```

**Per-repo lifecycle events:** `{realm}.git.{repo_id}.lifecycle_v1`
- initiated, renamed, archived, visibility_changed

**Realm-wide catalog updates:** `{realm}.git.catalog_updated_v1`
- Emitted whenever a repo is created/archived/made-public. Lets discovery UIs refresh.

Subscribers use `macula:subscribe(Topic, Handler)` to get real-time events. Martha's persona-install flow can subscribe to `persona-repo.ref_updated_v1` and auto-pull updates.

### Storage

Each Hecate node has `/var/hecate/repos/` (or configurable) with one bare git repo per `repo_id`:

```
/var/hecate/repos/
├── 01HZY...ABC.git/                   (bare repo, UUIDv7 or blake3-derived id)
│   ├── HEAD
│   ├── objects/
│   ├── refs/
│   └── hooks/
│       └── post-receive               ← emits ref_updated FACT
├── 01HZY...DEF.git/
└── ...
```

The event store (reckon-db) holds metadata (repo_id → name, owner, tags, timestamps). Actual git objects are git's native store. This separation keeps the event store small and lets git do what git does best.

Backups: a trusted peer node can `git clone mesh://...` to pull a full mirror. Automated mirroring (push scheduled to peers) is a follow-up feature.

### Auth

Three layers, defense-in-depth:

1. **Realm membership check.** Only members of a repo's realm can even resolve its procedure MRI. Enforced by mesh routing.
2. **Realm cert on the caller.** Every `macula:call` carries the caller's realm cert (existing Macula V2 mechanism). `serve_git_over_mesh` verifies the caller's DID against the repo's ACL.
3. **Per-repo capability tokens** (optional). For fine-grained access: "bob can read this repo but not push." Capability tokens are stored on the owner's node and issued via a `grant_repo_capability_v1` command.

Read-public repos skip layer 3 (anyone in the realm can read). Private repos require an explicit capability.

Cross-realm access goes through the **realm-federation JWT** defined in `PLAN_HANKO_PLATFORM_IDP.md`. Alice's realm cert signs a short-lived JWT scoped to Bob's realm; Bob's realm verifies the signature against Alice's realm CA (published at a well-known URL or discovered via mesh FACT).

### Streaming and large packs

Git pack data can be large (MBs to GBs). RPC reply in chunks:

- Macula V2 supports chunked streaming replies (`macula:call_stream` in the protocol)
- Helper flushes pack bytes in 64 KB chunks
- Server-side `git upload-pack` runs as a subprocess; its stdout is piped through the chunk stream
- Back-pressure handled at the QUIC layer
- Progress updates: optional side-channel FACTs for UI progress bars ("45% of pack received")

For very large repos (>100 MB pack), we recommend the client use partial clone (`--filter=blob:none`) to fetch metadata first and lazy-load blobs. Standard git feature.

## Discovery

Three mechanisms, complementary:

1. **Direct URI.** User knows `mesh://did:realm:alice/config` — clone directly. Works when you've been given the URI.
2. **Realm catalog.** `query_repos:list_realm_repos/1` returns all public repos in a realm. Populated by `project_repos` from lifecycle events. Drives hecate-web's Repo List page.
3. **Cross-realm search.** (v2) Publish public-repo advertisements via mesh FACTs to a well-known topic. Subscribers across realms see them.

For Martha's persona browsing specifically: personas are repos tagged `martha-persona`, discoverable via `query_repos:search_by_tag("martha-persona", Realm)`. The Svelte UI adds a "Martha Persona Store" view that filters the realm catalog.

## Interaction patterns for downstream plugins

### macula-realm gitops (replacing GitHub App)

1. User configures gitops for their Hecate node via macula-realm admin UI
2. macula-realm calls `guide_repo_lifecycle.initiate_repo` on that user's node — the node now hosts a bare repo at `mri:proc:macula.io/git/{user_id}-gitops/`
3. User adds the mesh URI as a git remote: `git remote add hecate mesh://did:realm:macula.io/{user_id}-gitops`
4. User pushes config; node's git hooks emit `ref_updated` FACT
5. Local gitops reconciler subscribes to `ref_updated` on its own repo, pulls locally, applies quadlets

No GitHub App. No external host. User's gitops lives on their own node, mirrored via mesh if they wish.

### Martha persona install

1. Persona author publishes their persona repo on their own Hecate node (tagged `martha-persona`)
2. Martha plugin's discovery UI queries realm catalog for `martha-persona` tagged repos
3. User clicks "Install Marcus Aurelius persona" → plugin calls `git-over-mesh/fetch` under the hood, clones into local Martha data dir
4. Plugin subscribes to that repo's `ref_updated` FACT — auto-updates when author publishes new prompts
5. User can customize/fork via `git-remote-mesh push mesh://did:realm:me/marcus-aurelius-tuned`

### Briefcase archive snapshots

- Briefcase already uses content-addressed files (BLAKE3). For SET-of-files snapshots (an "archive"), use a git repo as the container. File-id tracking across versions via git's rename detection.

## Phases

### Phase 0 — Plan review (this doc)
- Sign off on architecture + MRI naming + RPC shape

### Phase 1 — Foundational daemon apps (1 week) — ✅ scaffolded 2026-04-21
- [x] `guide_repo_lifecycle` umbrella app with aggregate + 4 desks (initiate, archive, rename, set_repo_description)
- [x] Events: `repo_initiated_v1`, `repo_renamed_v1`, `repo_description_set_v1`, `repo_archived_v1`
- [x] `project_repos` ETS catalog (`repos` table) + merged projection covering all 4 events
- [x] `query_repos` read API (`/api/repos`, `/api/repos/:id`, `/api/repos/search`)
- [x] Tests — 11 eunit cases across aggregate + store (all passing)
- [x] `repo_store` registered in `hecate_app.erl` `?STORES` list; release apps + route discovery list updated

Pending for Phase 2 (covered by the next plan iteration, not blockers on Phase 1 close):
- `serve_git_over_mesh` (procedure advertisements + `git upload-pack` / `git receive-pack` subprocess)
- `announce_ref_updates` (post-receive FACT emitter)
- CT integration suite (cross-node fetch, subscription round-trip) — requires Phase 2 wire protocol

### Phase 2 — Server-side git RPC (1-1.5 weeks)
- [ ] `serve_git_over_mesh` advertises 3 procedures per repo on repo-initiated event
- [ ] `git upload-pack` subprocess integration (erlexec or similar)
- [ ] `git receive-pack` subprocess integration
- [ ] Post-receive hook emits `ref_updated_v1` FACT
- [ ] Chunked streaming of pack bytes through RPC reply
- [ ] Auth: realm cert verification on every call
- [ ] Integration test: node A invokes fetch on node B, pack bytes round-trip

### Phase 3 — `git-remote-mesh` client (1 week)
- [ ] Rust binary, vendored in a new repo `hecate-social/git-remote-mesh` OR bundled with `hecate-cli`
- [ ] URL parsing: `mesh://<did>/<repo>`
- [ ] macula-sdk client integration (reuses identity config)
- [ ] git-remote-helper protocol: capabilities, list, push, fetch commands
- [ ] Test: `git clone mesh://...` from dev workstation to Hecate node
- [ ] Distribution: single binary in GitHub releases, homebrew cask, AUR pkg

### Phase 4 — hecate-web Svelte browsing UX (1-1.5 weeks)
- [ ] New route `/git` in hecate-web
- [ ] `RepoList` component: query_repos lists + mesh URI copy
- [ ] `RepoBrowser` tree component: recursive tree fetch via `macula:call`
- [ ] `CommitLog` component: paginated log via RPC
- [ ] `DiffViewer` component
- [ ] `RefSubscription` component with live FACT updates
- [ ] Wire to viewstate pattern — daemon computes all presentation state

### Phase 5 — macula-realm gitops migration (0.5-1 week)
- [ ] Replace GitHub App calls in macula-realm with calls to `guide_repo_lifecycle.initiate_repo` on user's node
- [ ] Delete `system/apps/macula_realm/lib/macula_realm/github/` directory
- [ ] Update gitops_setup_live to generate mesh URIs instead of GitHub URLs
- [ ] Docs: user onboarding flow

### Phase 6 — Integration + first consumer (Martha) (1-1.5 weeks, later)
- [ ] Martha plugin consumes git-over-mesh for persona install
- [ ] Persona catalog view in hecate-web
- [ ] First published persona round-trips: browse → install → use

### Phase 7 — Optional: Forgejo-like mirror (deferred, maybe never)
- [ ] A Svelte view is enough for browsing. Forgejo mirror becomes unnecessary unless specific collaboration features demand it.

## Timelines

**Core (Phases 1-5): ~5 weeks** at focused solo pace.

This gives a fully decentralized git stack with Svelte browser, client binary, and macula-realm migration.

Phase 6 + Martha: +1.5 weeks when Martha itself lands.

## Files to create / modify

### Create (hecate-daemon)
- `apps/guide_repo_lifecycle/{src,test,include}/...`
- `apps/project_repos/{src,test}/...`
- `apps/query_repos/{src,test}/...`
- `apps/serve_git_over_mesh/{src,test}/...`
- `apps/announce_ref_updates/{src,test}/...`

### Create (new repo)
- `hecate-social/git-remote-mesh` — Rust binary + docs

### Create (hecate-web)
- `src/lib/components/git/RepoList.svelte`
- `src/lib/components/git/RepoBrowser.svelte`
- `src/lib/components/git/CommitLog.svelte`
- `src/lib/components/git/DiffViewer.svelte`
- `src/lib/components/git/RefSubscription.svelte`
- `src/routes/git/+page.svelte`
- `src/routes/git/[repo_id]/+page.svelte`

### Modify (macula-realm)
- `system/apps/macula_realm_web/lib/macula_realm_web/live/gitops_setup_live.ex` — use mesh URIs instead of GitHub
- Delete: `system/apps/macula_realm/lib/macula_realm/github/` entire directory

### Modify (hecate-station)
- `plans/PLAN_DEFERRED_WORK.md` — move git-over-mesh from deferred to active

## Open questions

- **URL scheme:** `mesh://` vs `hecate://` vs `rad://`-like? `mesh://` reads cleanly; `hecate://` is branded. Lean `mesh://` (generic, not Hecate-coupled).
- **Repo IDs:** UUIDv7 (time-sortable) or BLAKE3 of initial name+realm (deterministic)? UUIDv7 is simpler; BLAKE3 is deterministic and enables content-addressed URIs. Lean UUIDv7 for v1.
- **Ref-advertisement cadence:** push to FACT on every ref update (real-time, noisy) or batch (every 10s, quieter)? Real-time is simpler; batch saves bandwidth for active repos. Lean real-time.
- **Partial/shallow clones:** git's pack filter spec — do we pass through filter params transparently or explicitly support/reject them? Pass-through is simpler.
- **Max pack size:** enforce a ceiling (say 500 MB) in v1 to protect the relay? Soft warning, hard block? Lean: warn at 100 MB, block at 1 GB in v1.
- **Content-addressed referencing across realms:** can I reference a specific commit `mesh://did:realm:alice/config@sha256:abc...` for immutability? Useful for reproducibility; needs URL-scheme extension.
- **Hosting someone else's repo as a mirror:** does node B mirroring node A's repo advertise as a separate MRI, or re-advertise under the same MRI with a "mirror of" flag? Matters for failover. Deferred.

## Success criteria

- [ ] `git clone mesh://did:realm:alice/config` from any workstation lands a working clone
- [ ] `git push mesh://did:realm:alice/config master` from dev machine updates the repo on Alice's node and triggers a `ref_updated_v1` FACT that other subscribers receive in <1 s
- [ ] Two Hecate nodes can exchange git data without any external git host
- [ ] macula-realm gitops works end-to-end without a GitHub App
- [ ] hecate-web Svelte browser shows at least: repo list, tree, commit log, live ref updates
- [ ] Zero references to `github.com`, Forgejo, GitLab, or Radicle in the gitops path

## Strategic positioning

**Before this plan:**
- "We want to build a decentralized stack but we depend on GitHub."

**After this plan:**
- "Every Hecate node hosts git. Repositories flow through the same mesh our apps use. No external git host. No central identity. No proprietary protocol."

This is a genuine differentiator — not vaporware, built on primitives we already operate. It makes the Hecate sovereign-app-platform pitch completely self-consistent.

## Links

- Macula V2 primitives: `PLAN_MACULA_V2_PART6_PROTOCOL.md`, `PLAN_MACULA_V2_PART7_IMPLEMENTATION.md`
- Procedure advertisements: `PLAN_MACULA_V2_PART3_DISCOVERY.md`
- Deferred work index: `PLAN_DEFERRED_WORK.md`
- Companion: `PLAN_HANKO_PLATFORM_IDP.md` (macula-architecture) — defines realm federation JWT used for cross-realm git access
- git remote helpers spec: `git-remote-helpers(7)` / https://git-scm.com/docs/gitremote-helpers
