/**
 * The `ctx.jobs` client service: reference-counted streams over the `job`
 * namespace — one `job.list` roster stream per watched session and one
 * `job.follow` stream per observed job — so overlapping viewers share a
 * stream, rosters resume whole after a reconnect, and observations resume
 * from the model's cursor, plus the human kill passthrough over `job.kill`.
 * @module @deepseek-ai/dsh-api-job-controller/client/service
 */

import { Service, type Context } from '@deepseek-ai/cordis'
import { RemoteStreamCarrierError, type ClientRemote, type RemoteStream } from '@deepseek-ai/dsh-api-gateway/client'
import type { JobId } from '@deepseek-ai/dsh-jobs/brand'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import type { RemoteResult } from '@deepseek-ai/dsh-typert-protocol'
import type {
  JobFollowFrame,
  JobFollowRequest,
  JobKillRequest,
  JobKillValue,
  JobListFrame,
  JobListRequest,
} from '../types.ts'
import type { ClientJobsModel, JobsSource } from './model.ts'

/** The generated `job` namespace face the stream runners drive. */
export interface JobRemote {
  /**
   * Open one roster generation.
   * @param request - the session whose visible set to mirror.
   * @param signal - generation cancellation.
   * @returns the whole-set frame sequence of one generation.
   */
  list(request: JobListRequest, signal?: AbortSignal): AsyncIterable<JobListFrame>
  /**
   * Open one observation generation.
   * @param request - target job, owning session, and optional resume offset.
   * @param signal - generation cancellation.
   * @returns the frame sequence of one generation.
   */
  follow(request: JobFollowRequest, signal?: AbortSignal): AsyncIterable<JobFollowFrame>
  /**
   * Kill one job on the human's behalf.
   * @param request - the session whose list carries the job, and the job id.
   * @returns the registry's admission, or the business/transport failure.
   */
  kill(request: JobKillRequest): Promise<RemoteResult<JobKillValue>>
}

/** Remote faces the runners drive: the Gateway stream factory and the `job` namespace. */
export interface JobsRemote {
  readonly $stream: ClientRemote['$stream']
  readonly job: JobRemote
}

/** The client jobs service face. */
export interface IJobs {
  /** Rosters and per-job observation state. */
  readonly state: JobsSource
  /**
   * Keep one session's roster current; reference-counted, so two watchers of
   * the same session share one stream and the rows leave with the last.
   * @param sessionId - the session whose visible jobs to mirror.
   * @returns stop function releasing this watcher's reference.
   */
  watchRows(sessionId: SessionId): () => void
  /**
   * Retry a terminally failed watched roster, retaining its last received rows.
   * Loading, ready, unwatched and disposed rosters are unchanged; repeated
   * calls share one replacement and create no watcher.
   * @param sessionId - the watched session to retry.
   */
  retryRows(sessionId: SessionId): void
  /**
   * Start observing one job's live output; reference-counted, so two viewers
   * of the same job share one stream.
   * @param sessionId - owning session used for the fenced read; undefined for an unowned job.
   * @param id - job to observe.
   * @returns stop function releasing this observer's reference.
   */
  observe(sessionId: SessionId | undefined, id: JobId): () => void
  /**
   * Kill one background job from a session's job list. Pure RPC passthrough:
   * row state converges through the roster stream, and the caller (the
   * job-list control) owns error presentation.
   * @param sessionId - session whose job list carries the job.
   * @param id - the job row's registry id.
   * @returns the registry's admission, or the business/transport failure.
   */
  kill(sessionId: SessionId, id: JobId): Promise<RemoteResult<JobKillValue>>
}

/** One reference-counted stream. */
interface StreamEntry {
  refs: number
  stopped: boolean
  dispose: () => Promise<void>
}

/** Watch references outlive failed roster streams and bind their exact owner. */
interface RosterOwner {
  refs: number
  generation: RosterGeneration
}

/** One logical Gateway stream, including all of its physical carrier retries. */
interface RosterGeneration {
  failed: boolean
  stream?: RemoteStream<JobListFrame>
  closing?: Promise<void>
}

declare module '@deepseek-ai/cordis' {
  interface Context {
    /** React-free client job rosters and observation control. */
    jobs: IJobs
  }
}

/** Owns the bare jobs snapshot and the per-session and per-job streams. */
export class ClientJobs extends Service implements IJobs {
  readonly state: JobsSource
  private readonly rowsEntries = new Map<SessionId, RosterOwner>()
  private readonly rowTasks = new Set<Promise<void>>()
  private readonly observations = new Map<string, StreamEntry>()
  private disposed = false

  /**
   * @param ctx - client root Context.
   * @param remote - the Gateway stream factory plus the generated `job` namespace, both resolved by the caller.
   * @param model - shared client jobs model.
   */
  constructor(
    ctx: Context,
    private readonly remote: JobsRemote,
    private readonly model: ClientJobsModel,
  ) {
    super(ctx, 'jobs')
    this.state = model
    ctx.effect(
      () => async () => {
        this.disposed = true
        const rows = [...this.rowsEntries.entries()]
        const open = [...this.observations.values()]
        this.rowsEntries.clear()
        this.observations.clear()
        this.model.rowsCleared()
        for (const entry of open) entry.stopped = true
        // Cordis awaits an async disposer, so the fiber stays unloading until
        // every carrier iterator has closed and a successor plugin instance
        // cannot overlap one. A carrier whose teardown fails is stopped all the
        // same; its failure has no consumer here.
        await Promise.allSettled([
          ...rows.map(([, owner]) => this.closeRows(owner.generation)),
          ...this.rowTasks,
          ...open.map(entry => entry.dispose()),
        ])
      },
      'job-controller.client.streams',
    )
  }

  kill(sessionId: SessionId, id: JobId): Promise<RemoteResult<JobKillValue>> {
    return this.remote.job.kill({ sessionId, jobId: id })
  }

  watchRows(sessionId: SessionId): () => void {
    if (this.disposed) return () => {}
    let owner = this.rowsEntries.get(sessionId)
    if (owner === undefined) {
      owner = { refs: 1, generation: { failed: false } }
      this.rowsEntries.set(sessionId, owner)
      this.startRows(sessionId, owner, true)
    } else {
      owner.refs++
      this.retryRows(sessionId)
    }
    const held = owner
    let released = false
    return () => {
      if (released || this.rowsEntries.get(sessionId) !== held) return
      released = true
      if (--held.refs > 0) return
      this.rowsEntries.delete(sessionId)
      const closing = this.closeRows(held.generation)
      void Promise.allSettled([closing]).then(() => {
        if (!this.rowsEntries.has(sessionId)) this.model.rowsDropped(sessionId)
      })
    }
  }

  retryRows(sessionId: SessionId): void {
    const owner = this.rowsEntries.get(sessionId)
    if (this.disposed || owner?.generation.failed !== true) return
    const predecessor = owner.generation
    owner.generation = { failed: false }
    this.startRows(sessionId, owner, false, predecessor)
  }

  observe(sessionId: SessionId | undefined, id: JobId): () => void {
    if (this.disposed) return () => {}
    return this.acquire(
      this.observations,
      String(id),
      () => this.startObservation(sessionId, id),
      () => {
        this.model.observeStopped(id)
      },
    )
  }

  /** Share the live entry under `key` or start one, and hand back its release. */
  private acquire(
    entries: Map<string, StreamEntry>,
    key: string,
    start: () => StreamEntry,
    cleared: () => void,
  ): () => void {
    const existing = entries.get(key)
    if (existing !== undefined && !existing.stopped) {
      existing.refs += 1
      return this.releaser(entries, key, existing, cleared)
    }
    const entry = start()
    entries.set(key, entry)
    return this.releaser(entries, key, entry, cleared)
  }

  /**
   * Release closures bind the exact entry they were minted for, never the
   * map's current occupant: a later acquire on the same key may have replaced
   * a stopped entry, and decrementing or disposing through the key alone
   * would tear down that newer stream's references.
   */
  private releaser(
    entries: Map<string, StreamEntry>,
    key: string,
    entry: StreamEntry,
    cleared: () => void,
  ): () => void {
    let released = false
    return () => {
      if (released) return
      released = true
      entry.refs -= 1
      if (entry.refs > 0) return
      if (entries.get(key) === entry) entries.delete(key)
      entry.stopped = true
      void entry.dispose().then(() => {
        // Clear the state only while no successor holds the key: a re-acquire
        // inside the dispose round-trip already refilled the model, and a
        // stale clear would blank it for good.
        if (entries.has(key)) return
        cleared()
      })
    }
  }

  private rowsCurrent(sessionId: SessionId, owner: RosterOwner, generation: RosterGeneration): boolean {
    return !this.disposed && this.rowsEntries.get(sessionId) === owner && owner.generation === generation
  }

  private closeRows(generation: RosterGeneration): Promise<void> {
    if (generation.stream === undefined) return Promise.resolve()
    generation.closing ??= generation.stream.dispose()
    return generation.closing
  }

  private startRows(sessionId: SessionId, owner: RosterOwner, fresh: boolean, predecessor?: RosterGeneration): void {
    const name = `job rows ${String(sessionId)}`
    const generation = owner.generation
    // Publish only after installing both owners: notifications may release
    // this watch, re-watch, retry or dispose the service synchronously.
    this.model.rowsLoading(sessionId, fresh)
    if (!this.rowsCurrent(sessionId, owner, generation)) return
    const task = (async () => {
      try {
        if (predecessor !== undefined) await this.closeRows(predecessor)
        if (!this.rowsCurrent(sessionId, owner, generation)) return
        const stream = this.remote.$stream<JobListFrame>({
          name,
          open: (signal) => {
            if (this.rowsCurrent(sessionId, owner, generation)) this.model.rowsLoading(sessionId)
            signal.throwIfAborted()
            if (!this.rowsCurrent(sessionId, owner, generation)) throw new Error(`${name} released before open`)
            return this.remote.job.list({ sessionId }, signal)
          },
          carrierFailed: () => {
            if (this.rowsCurrent(sessionId, owner, generation)) this.model.rowsLoading(sessionId)
          },
          // A watched roster has no natural end. After a baseline, reconnect
          // can replace it whole; ending before a baseline is terminal.
          ended: accepted =>
            accepted
              ? new RemoteStreamCarrierError(`${name} ended before release`)
              : new Error(`${name} ended before its first frame`),
        })
        generation.stream = stream
        if (!this.rowsCurrent(sessionId, owner, generation)) return
        for await (const item of stream) {
          if (!this.rowsCurrent(sessionId, owner, generation) || item.signal.aborted) continue
          this.model.rowsReplaced(sessionId, item.value.jobs)
          item.accept()
        }
      } catch (error) {
        if (this.rowsCurrent(sessionId, owner, generation)) {
          generation.failed = true
          this.model.rowsFailed(sessionId, error)
        }
      } finally {
        await Promise.allSettled([this.closeRows(generation)])
      }
    })()
    this.rowTasks.add(task)
    void task.then(() => {
      this.rowTasks.delete(task)
    })
  }

  private startObservation(sessionId: SessionId | undefined, id: JobId): StreamEntry {
    const name = `job observation ${String(id)}`
    const stream = this.remote.$stream<JobFollowFrame>({
      name,
      open: (signal) => {
        const from = this.model.cursorOf(id)
        return this.remote.job.follow(
          {
            jobId: id,
            ...(sessionId !== undefined ? { sessionId } : {}),
            ...(from !== undefined ? { from } : {}),
          },
          signal,
        )
      },
      // A premature end after the anchor is retryable (a Host reload closes the
      // generation); resuming from the cursor loses nothing. An end before the
      // anchor is terminal.
      ended: accepted =>
        accepted
          ? new RemoteStreamCarrierError(`${name} ended before settlement`)
          : new Error(`${name} ended before its anchor`),
    })
    const entry: StreamEntry = {
      refs: 1,
      stopped: false,
      dispose: () => stream.dispose(),
    }
    void (async () => {
      try {
        for await (const item of stream) {
          const frame = item.value
          if (frame.type === 'opened') {
            this.model.observeOpened(id, frame)
            item.accept()
            continue
          }
          if (frame.type === 'output') {
            this.model.observeOutput(id, frame)
            continue
          }
          // Terminal status: leave the loop before the generation end is
          // classified, then close the stream for good.
          this.model.observeSettled(id)
          break
        }
      } catch (error) {
        if (!entry.stopped) this.model.observeFailed(id, error)
      } finally {
        entry.stopped = true
        void entry.dispose()
      }
    })()
    return entry
  }
}
