// Session-header background jobs driven by a real `ctx.jobs` entry. No model
// call is involved.
import type { Agent } from '@deepseek-ai/dsh-agent'
import type { JobController } from '@deepseek-ai/dsh-api-job-controller'
import { JobId } from '@deepseek-ai/dsh-jobs'
import { ToolCallId } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol'
import { readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { Browser, Page } from 'playwright'
import { chromium } from 'playwright'
import { afterAll, beforeAll, describe, expect, it, onTestFailed, vi } from 'vitest'
import {
  assertFixtureInventory,
  captureStableAria,
  compareOrRefreshGolden,
  launchWebScaffold,
  seedSession,
  watchConsole,
  webSnapshotMode,
  type WebScaffold,
} from './scaffold.ts'
import { newEnglishPage, saveFailureShot } from './support.ts'

const FIXTURE = fileURLToPath(new URL('../../../snapshots/web/fresh-round-trip/session.v3.jsonl', import.meta.url))
const SNAPSHOT_DIR = fileURLToPath(new URL('../../../snapshots/web/background-job-list', import.meta.url))
const RUNNING_EXPECTED = join(SNAPSHOT_DIR, 'running.expected.md')
const SETTLED_EXPECTED = join(SNAPSHOT_DIR, 'settled.expected.md')
const LOADING_EXPECTED = join(SNAPSHOT_DIR, 'loading.expected.md')
const FAILED_EXPECTED = join(SNAPSHOT_DIR, 'failed.expected.md')
const RETRYING_EXPECTED = join(SNAPSHOT_DIR, 'retrying.expected.md')
const MODE = webSnapshotMode()
const SEED_ID = 'background-job-list-web-e2e'
// A hold on a barrier file the test owns in the job's cwd: the process never
// exits on its own, so no CI stall can settle it before the scenario kills it.
// The bounded loop only caps an orphan's lifetime if the runner dies before
// `afterAll` releases the barrier.
const RELEASE = '.background-job-list.release'
const COMMAND = `for _ in $(seq 1 3000); do [ -e ${RELEASE} ] && break; sleep 0.2; done`

/**
 * Wait for opening a session to publish its live Agent.
 * @param scaffold - the booted web scaffold.
 * @param sessionId - the opened session's identity.
 * @returns the registered Agent instance.
 */
async function liveAgent(scaffold: WebScaffold, sessionId: SessionId): Promise<Agent> {
  const deadline = Date.now() + 30_000
  for (;;) {
    const found = scaffold.ctx.agents.get(sessionId)
    if (found !== undefined) return found
    if (Date.now() > deadline) throw new Error(`opening session "${sessionId}" published no live Agent`)
    await new Promise(resolve => setTimeout(resolve, 100))
  }
}

describe.skipIf(MODE === 'record')('web e2e: background job list', () => {
  let scaffold: WebScaffold
  let browser: Browser
  let page: Page
  let tripwire: ReturnType<typeof watchConsole>
  let agent: Agent
  const initial = Promise.withResolvers<undefined>()
  const failure = Promise.withResolvers<undefined>()
  const retry = Promise.withResolvers<undefined>()
  let opens = 0
  let closed = 0
  let restoreList: (() => void) | undefined

  beforeAll(async () => {
    scaffold = await launchWebScaffold({})
    const list = scaffold.ctx.jobController.list.bind(scaffold.ctx.jobController)
    const spy = vi.spyOn(scaffold.ctx.jobController, 'list').mockImplementation(async function* (
      this: JobController,
      request,
      signal,
    ) {
      const ordinal = ++opens
      const controller = new AbortController()
      const abort = () => {
        controller.abort(signal.reason)
      }
      signal.addEventListener('abort', abort, { once: true })
      if (signal.aborted) abort()
      const iterator = list.call(this, request, controller.signal)[Symbol.asyncIterator]()
      try {
        // Delay actual Host delivery, then fail only its first carrier. The
        // shipping Gateway and jobs service own every client state transition.
        await (ordinal === 1 ? initial.promise : retry.promise)
        while (!signal.aborted) {
          const next = iterator.next()
          const result =
            ordinal === 1
              ? await Promise.race([
                next,
                failure.promise.then(() => {
                  throw new RemoteError('gateway/bad-request', 'Roster fixture interrupted', {})
                }),
              ])
              : await next
          if (result.done) return
          yield result.value
        }
      } finally {
        controller.abort()
        await iterator.return?.()
        signal.removeEventListener('abort', abort)
        closed++
      }
    })
    restoreList = () => {
      spy.mockRestore()
    }
    await seedSession(scaffold, await readFile(FIXTURE, 'utf8'), SEED_ID)
    browser = await chromium.launch()
    page = await newEnglishPage(browser)
    tripwire = watchConsole(page)
    await page.goto(scaffold.authenticatedUrl, { waitUntil: 'load' })
    await page.waitForSelector('[class*="frame"]', { timeout: 30_000 })

    const groupRow = page.locator('[role="treeitem"]').first()
    await groupRow.waitFor({ timeout: 15_000 })
    await groupRow.click()
    const sessionRow = page.locator('[role="treeitem"]').nth(1)
    await sessionRow.waitFor({ timeout: 10_000 })
    await sessionRow.click()

    // Opening the session drives the Host's ordinary Agent resolution; the
    // job owner must be that exact live instance, never a second one.
    // `expect.poll` is test-scoped, so this hook polls by hand.
    agent = await liveAgent(scaffold, SessionId(SEED_ID))
  }, 120_000)

  afterAll(async () => {
    initial.resolve(undefined)
    failure.resolve(undefined)
    retry.resolve(undefined)
    const failures: unknown[] = []
    if (scaffold !== undefined)
      await writeFile(join(scaffold.workspaceCwd, RELEASE), '').catch((error: unknown) => failures.push(error))
    await browser?.close().catch((error: unknown) => failures.push(error))
    await scaffold?.close().catch((error: unknown) => failures.push(error))
    restoreList?.()
    if (failures.length === 1) throw failures[0]
    if (failures.length > 1) throw new AggregateError(failures, 'Background jobs teardown failed')
  })

  it('distinguishes an opening empty roster, retains failed rows and retries before confirming a real job kill', async () => {
    let phase = 'running'
    onTestFailed(() => saveFailureShot(page, `web-e2e-background-job-${phase}`))
    const loading = page.getByRole('button', { name: 'Connecting to background jobs', exact: true })
    await loading.waitFor()
    await loading.click()
    await page.getByRole('status').filter({ hasText: 'Connecting to background jobs' }).waitFor()
    await compareOrRefreshGolden(
      LOADING_EXPECTED,
      await captureStableAria(page, '[class*="menu"]', scaffold.workspaceCwd),
      MODE,
    )
    initial.resolve(undefined)
    await loading.waitFor({ state: 'detached' })
    expect(opens).toBe(1)
    const trigger = page.getByRole('button', { name: '1 background job running' })
    expect(await trigger.count()).toBe(0)

    const started = await scaffold.ctx.tools.execute({
      signal: new AbortController().signal,
      callId: ToolCallId('background-job-list-e2e'),
      name: 'bash',
      arguments: { command: COMMAND, description: 'Hold a background slot open', run_in_background: true },
      agent,
    })
    const reported = started.content.map(block => (block.type === 'text' ? block.text : '')).join('')
    const matched = /\bbash-\d+\b/.exec(reported)
    if (matched === null) throw new Error(`background bash reported no job id: ${reported}`)
    const jobId = JobId(matched[0])

    await trigger.waitFor({ timeout: 15_000 })
    await trigger.click()
    const row = page.getByRole('list', { name: 'Background jobs' }).getByRole('listitem').first()
    await row.waitFor({ timeout: 10_000 })
    await expect.poll(() => row.textContent()).toContain(COMMAND)

    const running = await captureStableAria(page, '[class*="menu"]', scaffold.workspaceCwd, { runningJobs: 'keep' })
    await compareOrRefreshGolden(RUNNING_EXPECTED, running, MODE)
    expect(tripwire.pageErrors).toEqual([])
    expect(tripwire.warnings).toEqual([])

    phase = 'failed-roster'
    const armed = page.locator('[data-kill-state]')
    await armed.click()
    await expect.poll(() => armed.getAttribute('data-kill-state')).toBe('armed')
    failure.resolve(undefined)
    await page.getByRole('alert').filter({ hasText: 'Roster fixture interrupted' }).waitFor()
    expect(await page.locator('[data-kill-state]').count()).toBe(0)
    expect(scaffold.ctx.jobs.get(jobId, agent.id).status).toBe('running')
    await compareOrRefreshGolden(
      FAILED_EXPECTED,
      await captureStableAria(page, '[class*="menu"]', scaffold.workspaceCwd, { runningJobs: 'keep' }),
      MODE,
    )
    await page.getByRole('button', { name: 'Retry', exact: true }).click()
    await page.getByRole('status').filter({ hasText: 'Connecting to background jobs' }).waitFor()
    await expect.poll(() => opens).toBe(2)
    expect(closed).toBe(1)
    expect(await page.locator('[data-kill-state]').count()).toBe(0)
    await compareOrRefreshGolden(
      RETRYING_EXPECTED,
      await captureStableAria(page, '[class*="menu"]', scaffold.workspaceCwd, { runningJobs: 'keep' }),
      MODE,
    )
    retry.resolve(undefined)
    await trigger.waitFor()
    await expect.poll(() => page.locator('[data-kill-state]').getAttribute('data-kill-state')).toBe('idle')
    expect(opens).toBe(2)

    phase = 'settled'
    // The whole human path: arm, confirm, job.kill, registry kill, jobs
    // frames flipping the row — no registry call from the test.
    const stop = page.locator('[data-kill-state]')
    await stop.waitFor({ timeout: 10_000 })
    await stop.click()
    await expect.poll(() => stop.getAttribute('data-kill-state')).toBe('armed')
    await stop.click()

    // Exact: the running trigger's name ('1 background job running') contains this label.
    const idle = page.getByRole('button', { name: '1 background job', exact: true })
    await idle.waitFor({ timeout: 20_000 })
    // The unclaimed report's reason lands in the settled row's detail.
    await expect
      .poll(() => page.getByRole('list', { name: 'Background jobs' }).textContent(), { timeout: 15_000 })
      .toContain('cancelled by the user')
    expect(scaffold.ctx.jobs.get(jobId, agent.id).status).toBe('killed')

    const settled = await captureStableAria(page, '[class*="menu"]', scaffold.workspaceCwd)
    await compareOrRefreshGolden(SETTLED_EXPECTED, settled, MODE)
    expect(tripwire.pageErrors).toEqual([])
    expect(tripwire.warnings).toEqual([])
  }, 90_000)

  it('keeps its snapshot inventory closed', async () => {
    await assertFixtureInventory(SNAPSHOT_DIR, [
      'loading.expected.md',
      'running.expected.md',
      'failed.expected.md',
      'retrying.expected.md',
      'settled.expected.md',
    ])
  })
})
