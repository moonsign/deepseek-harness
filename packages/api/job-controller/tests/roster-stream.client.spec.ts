import { Context } from '@deepseek-ai/cordis'
import { RemoteStream, RemoteStreamCarrierError, type RemoteStreamOptions } from '@deepseek-ai/dsh-api-gateway/client'
import type { ConnectionHandle } from '@deepseek-ai/dsh-client-connection/client'
import { createSnapshotStore } from '@deepseek-ai/dsh-client-store'
import { SessionId } from '@deepseek-ai/dsh-session/types'
import { afterEach, expect, it } from 'vitest'
import { ClientJobsModel } from '../src/client/model.ts'
import { ClientJobs, type JobsRemote } from '../src/client/service.ts'
import type { JobListFrame, JobView } from '../src/types.ts'

const SESSION = SessionId('roster-fixture')
const JOB: JobView = {
  id: 'bash-roster' as JobView['id'],
  kind: 'bash',
  label: 'retained job',
  status: 'running',
  startedAt: 1,
  output: { total: 0, earliest: 0 },
}

/** Controlled wire frames; the actual Gateway owns generations and cancellation. */
class Wire {
  private readonly frames: JobListFrame[] = []
  private failure?: Error
  private waiter?: () => void
  finished = false
  closed = false
  private releaseCleanup?: () => void
  private cleanup?: Promise<void>

  push(jobs: readonly JobView[]): void {
    this.frames.push({ type: 'rows', jobs })
    this.waiter?.()
  }
  fail(error: Error): void {
    this.failure = error
    this.waiter?.()
  }
  end(): void {
    this.finished = true
    this.waiter?.()
  }
  holdCleanup(): void {
    this.cleanup = new Promise((resolve) => {
      this.releaseCleanup = resolve
    })
  }
  release(): void {
    this.releaseCleanup?.()
  }

  async *read(signal: AbortSignal): AsyncGenerator<JobListFrame> {
    const wake = () => this.waiter?.()
    signal.addEventListener('abort', wake)
    try {
      while (!signal.aborted) {
        if (this.failure) throw this.failure
        const frame = this.frames.shift()
        if (frame) yield frame
        else if (this.finished) return
        else
          await new Promise<void>((resolve) => {
            this.waiter = resolve
          })
      }
    } finally {
      signal.removeEventListener('abort', wake)
      await this.cleanup
      this.closed = true
    }
  }
}

const worlds: { ctx: Context; wires: Wire[] }[] = []
afterEach(async () => {
  for (const { ctx, wires } of worlds.splice(0)) {
    for (const wire of wires) wire.release()
    await ctx.fiber.dispose()
  }
})

function world() {
  const ctx = new Context()
  const model = new ClientJobsModel()
  const generation = createSnapshotStore<ReturnType<ConnectionHandle['generation']['getSnapshot']>>({
    id: 1,
    host: { home: '/roster-fixture' },
  })
  const wires: Wire[] = []
  const remote: JobsRemote = {
    $stream: <Item>(options: RemoteStreamOptions<Item>) => new RemoteStream({ generation }, options),
    job: {
      list: (_request, signal) => {
        if (!signal) throw new Error('Roster opener did not supply cancellation')
        const wire = new Wire()
        wires.push(wire)
        return wire.read(signal)
      },
      follow: async function* () {
        /* This fixture observes rosters only. */
      },
      kill: async () => ({ ok: true, value: { outcome: 'already-finished' } }),
    },
  }
  const jobs = new ClientJobs(ctx, remote, model)
  worlds.push({ ctx, wires })
  return { ctx, model, jobs, wires, generation }
}

it('marks real carrier loss and replacement opening loading until a whole-set frame arrives', async () => {
  const { jobs, model, wires, generation } = world()
  const stop = jobs.watchRows(SESSION)
  await expect.poll(() => wires.length).toBe(1)
  wires[0]!.push([JOB])
  await expect.poll(() => model.getSnapshot().rosterStatus[SESSION]?.state).toBe('ready')
  generation.set(undefined)
  wires[0]!.fail(new RemoteStreamCarrierError('carrier disconnected'))
  await expect.poll(() => model.getSnapshot().rosterStatus[SESSION]?.state).toBe('loading')
  expect(model.getSnapshot().rows[SESSION]).toEqual([JOB])
  expect(wires).toHaveLength(1)
  generation.set({ id: 2, host: { home: '/roster-fixture' } })
  await expect.poll(() => wires.length).toBe(2)
  expect(model.getSnapshot().rosterStatus[SESSION]?.state).toBe('loading')
  expect(model.getSnapshot().rows[SESSION]).toEqual([JOB])
  wires[1]!.push([])
  await expect.poll(() => model.getSnapshot().rosterStatus[SESSION]?.state).toBe('ready')
  expect(model.getSnapshot().rows[SESSION]).toEqual([])
  stop()
  await expect.poll(() => wires.every(wire => wire.closed)).toBe(true)
  await expect.poll(() => model.getSnapshot().rosterStatus[SESSION]).toBeUndefined()
})

it('makes a real end before baseline terminal and awaits its cleanup before one coalesced retry', async () => {
  const { jobs, model, wires } = world()
  const first = jobs.watchRows(SESSION)
  const second = jobs.watchRows(SESSION)
  await expect.poll(() => wires.length).toBe(1)
  wires[0]!.end()
  await expect.poll(() => model.getSnapshot().rosterStatus[SESSION]?.state).toBe('error')
  expect(model.getSnapshot().rows[SESSION]).toBeUndefined()
  jobs.retryRows(SESSION)
  jobs.retryRows(SESSION)
  await expect.poll(() => wires.length).toBe(2)
  expect(wires[0]!.closed).toBe(true)
  wires[1]!.push([JOB])
  await expect.poll(() => model.getSnapshot().rosterStatus[SESSION]?.state).toBe('ready')
  first()
  expect(wires[1]!.closed).toBe(false)
  second()
  await expect.poll(() => wires[1]!.closed).toBe(true)
})

it('service disposal clears and awaits a previously released real carrier', async () => {
  const { ctx, jobs, model, wires } = world()
  const stop = jobs.watchRows(SESSION)
  await expect.poll(() => wires.length).toBe(1)
  wires[0]!.push([JOB])
  await expect.poll(() => model.getSnapshot().rosterStatus[SESSION]?.state).toBe('ready')
  wires[0]!.holdCleanup()
  stop()
  let done = false
  const disposal = ctx.fiber.dispose().then(() => {
    done = true
  })
  await expect.poll(() => model.getSnapshot().rosterStatus[SESSION]).toBeUndefined()
  expect(done).toBe(false)
  wires[0]!.release()
  await disposal
  expect(wires[0]!.closed).toBe(true)
  jobs.retryRows(SESSION)
  expect(wires).toHaveLength(1)
})
