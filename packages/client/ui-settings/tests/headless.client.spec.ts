/** Detailed writes, explicit shared refresh and provider teardown. */
import { Context } from '@deepseek-ai/cordis'
import type {
  RemoteFailure,
  RemoteResult,
  SettingsNamespaceView,
  SettingsPathOpView,
} from '@deepseek-ai/dsh-api-remotes/client'
import { RemoteError, TestRemote } from '@deepseek-ai/dsh-client-test-runtime'
import type { JsonValue } from '@deepseek-ai/dsh-util-values'
import z from '@deepseek-ai/schemastery'
import { expect, it, onTestFinished, vi } from 'vitest'
import { ConfigFormController, ConfigForms } from '../src/client/config-form.ts'
import { apply, inject } from '../src/client/index.ts'
import { SettingsSchemaService } from '../src/client/schema.ts'
import { SettingsDescribeMirror, type SettingsDescribeView } from '../src/client/settings-mirror.ts'

const schema = JSON.parse(JSON.stringify(z.object({ field: z.string() }).toJSON())) as JsonValue
function view(field = 'initial', revision = 1): SettingsNamespaceView {
  return { ns: 'native-settings', value: { field }, schema, autoGenerate: true, applies: 'live', secrets: [], revision }
}
function described(field = 'initial', revision = 1): RemoteResult<SettingsDescribeView> {
  return { ok: true, value: { writable: true, hasDocument: true, namespaces: [view(field, revision)] } }
}
function deferred<T>() {
  return Promise.withResolvers<T>()
}
function fixture(mode: 'host' | 'memory' = 'host', release = () => {}) {
  const ctx = new Context()
  const describe = vi.fn<() => Promise<RemoteResult<SettingsDescribeView>>>().mockResolvedValue(described())
  const mutate = vi
    .fn<
    (namespace: string, ops: SettingsPathOpView[], revision?: number) => Promise<RemoteResult<SettingsNamespaceView>>
  >()
    .mockResolvedValue({ ok: true, value: view('changed', 2) })
  new TestRemote(ctx, { settings: { describe, mutate } })
  const mirror = new SettingsDescribeMirror(ctx, mode)
  const form = new ConfigFormController<{ field: string }>(
    ctx,
    { namespace: 'native-settings' },
    mirror,
    mode,
    new SettingsSchemaService(ctx),
  )
  onTestFinished(async () => {
    release()
    await form.dispose()
    await mirror.dispose()
    await ctx.fiber.dispose()
  })
  return { ctx, describe, mutate, mirror, form }
}

it('returns the original accepted mutation answer and keeps boolean writes compatible', async () => {
  const { mirror, form, mutate } = fixture()
  await mirror.ensure()
  const answer: RemoteResult<SettingsNamespaceView> = { ok: true, value: view('changed', 2) }
  mutate.mockResolvedValue(answer)
  expect(await form.mutateResult([{ op: 'set', path: ['field'], value: 'changed' }], 1)).toBe(answer)
  expect(form.getSnapshot().value).toEqual({ field: 'changed' })
  expect(await form.set('field', 'changed')).toBe(true)
  expect(await form.unset('field')).toBe(true)
})

it.each([
  new RemoteError('settings/conflict', 'changed elsewhere', { ns: 'native-settings', expected: 1, actual: 4 }),
  new RemoteError('settings/rejected', 'Host validation refused the field', { ns: 'native-settings' }),
  new RemoteError('gateway/internal', 'connection lost', {}),
])(
  'preserves $code and its details after recovery without turning it into acceptance',
  async (error: RemoteFailure) => {
    const { mirror, form, mutate, describe } = fixture()
    await mirror.ensure()
    const answer: RemoteResult<SettingsNamespaceView> = { ok: false, error }
    mutate.mockResolvedValue(answer)
    describe.mockResolvedValue(described('current', 4))
    const result = await form.mutateResult([{ op: 'set', path: ['field'], value: 'draft' }], 1)
    expect(result).toBe(answer)
    expect(form.getSnapshot()).toMatchObject({ value: { field: 'current' }, revision: 4 })
    expect(result?.ok).toBe(false)
    if (result?.ok !== false) throw new Error('expected a refused result')
    expect(result.error).toBe(error)
    if (result.error.code === 'settings/conflict')
      expect(result.error.details).toEqual({ ns: 'native-settings', expected: 1, actual: 4 })
    expect(await form.mutate([])).toBe(false)
  },
)

it('returns the original refusal even when its recovery read fails', async () => {
  const { mirror, form, mutate, describe } = fixture()
  await mirror.ensure()
  const answer: RemoteResult<SettingsNamespaceView> = {
    ok: false,
    error: new RemoteError('settings/rejected', 'invalid field', { ns: 'native-settings' }),
  }
  mutate.mockResolvedValue(answer)
  describe.mockRejectedValue(new Error('offline during recovery'))
  expect(await form.mutateResult([])).toBe(answer)
  expect(form.getSnapshot().value).toEqual({ field: 'initial' })
  expect(mirror.getSnapshot().error).toBe('offline during recovery')
})

it('shares ordering and revision chaining between detailed and boolean mutations', async () => {
  const first = deferred<RemoteResult<SettingsNamespaceView>>()
  const { mirror, form, mutate } = fixture('host', () => {
    first.resolve({ ok: true, value: view('one', 2) })
  })
  await mirror.ensure()
  mutate.mockReturnValueOnce(first.promise).mockResolvedValueOnce({ ok: true, value: view('two', 3) })
  const detailed = form.mutateResult([{ op: 'set', path: ['field'], value: 'one' }])
  const boolean = form.set('field', 'two')
  await vi.waitFor(() => {
    expect(mutate).toHaveBeenCalledOnce()
  })
  first.resolve({ ok: true, value: view('one', 2) })
  expect((await detailed)?.ok).toBe(true)
  expect(await boolean).toBe(true)
  expect(mutate.mock.calls.map(call => call[2])).toEqual([1, 2])
  expect(form.getSnapshot()).toMatchObject({ value: { field: 'two' }, revision: 3 })
})

it.each(['host', 'memory'] as const)('reports local %s skips without a Host write', async (mode) => {
  const { form, mutate } = fixture(mode)
  if (mode === 'host') await form.dispose()
  expect(await form.mutateResult([])).toBeUndefined()
  expect(await form.mutate([])).toBe(false)
  expect(mutate).not.toHaveBeenCalled()
})

it('drains a started detailed write, skips its queued successor and suppresses publication after form disposal', async () => {
  const first = deferred<RemoteResult<SettingsNamespaceView>>()
  const answer: RemoteResult<SettingsNamespaceView> = { ok: true, value: view('changed', 2) }
  const { mirror, form, mutate } = fixture('host', () => {
    first.resolve(answer)
  })
  await mirror.ensure()
  mutate.mockReturnValue(first.promise)
  const active = form.mutateResult([{ op: 'set', path: ['field'], value: 'changed' }])
  const queued = form.mutateResult([])
  await vi.waitFor(() => {
    expect(mutate).toHaveBeenCalledOnce()
  })
  const held = form.getSnapshot()
  const disposal = form.dispose()
  first.resolve(answer)
  expect(await active).toBe(answer)
  expect(await queued).toBeUndefined()
  await disposal
  expect(form.getSnapshot()).toBe(held)
  expect(mirror.namespace('native-settings')?.revision).toBe(1)
  expect(mutate).toHaveBeenCalledOnce()
  await mirror.refresh()
  expect(mirror.getSnapshot().status).toBe('ready')
})

it('refreshes a ready shared mirror and coalesces mid-flight refreshes into one rerun', async () => {
  const pending = deferred<RemoteResult<SettingsDescribeView>>()
  const { mirror, describe } = fixture('host', () => {
    pending.resolve(described('second', 2))
  })
  await mirror.ensure()
  await mirror.ensure()
  expect(describe).toHaveBeenCalledOnce()
  describe.mockReturnValueOnce(pending.promise).mockResolvedValueOnce(described('latest', 3))
  const refreshing = mirror.refresh()
  await Promise.resolve()
  const mid = mirror.refresh()
  const midToo = mirror.refresh()
  pending.resolve(described('second', 2))
  await Promise.all([refreshing, mid, midToo])
  expect(describe).toHaveBeenCalledTimes(3)
  expect(mirror.namespace('native-settings')?.revision).toBe(3)
})

it('disposes before a scheduled read starts and never resumes through the public face', async () => {
  const { mirror, describe } = fixture()
  const held = mirror.getSnapshot()
  const scheduled = mirror.refresh()
  await mirror.dispose()
  await scheduled
  await mirror.ensure()
  await mirror.refresh()
  mirror.acceptView(view('late', 8))
  expect(describe).not.toHaveBeenCalled()
  expect(mirror.getSnapshot()).toBe(held)
})

it('waits for the shared initial answer before registering a served namespace', async () => {
  const { ctx, mirror, describe } = fixture()
  const forms = new ConfigForms(ctx, { mirror, schema: ctx.settingsSchema, persistence: 'host' })
  const withdrawn = vi.fn()
  const register = vi.fn(() => withdrawn)
  const off = forms.whileServed(['native-settings'], register)
  onTestFinished(off)
  expect(register).not.toHaveBeenCalled()
  await forms.describe().ensure()
  expect(describe).toHaveBeenCalledOnce()
  expect(register).toHaveBeenCalledExactlyOnceWith(new Set(['native-settings']))
  off()
  expect(withdrawn).toHaveBeenCalledOnce()
})

it('issues no read when a loading subscriber disposes the shared mirror', async () => {
  const { mirror, describe } = fixture()
  const off = mirror.subscribe(() => {
    void mirror.dispose()
  })
  onTestFinished(off)
  await mirror.refresh()
  expect(describe).not.toHaveBeenCalled()
})

it.each(['success', 'failure', 'rejection'] as const)(
  'drains a held read and drops its late %s and queued rerun',
  async (outcome) => {
    const pending = deferred<RemoteResult<SettingsDescribeView>>()
    const answer: RemoteResult<SettingsDescribeView> =
      outcome === 'success'
        ? described('late', 9)
        : { ok: false, error: new RemoteError('gateway/internal', 'offline', {}) }
    const { mirror, describe } = fixture('host', () => {
      pending.resolve(answer)
    })
    await mirror.ensure()
    describe.mockReturnValue(pending.promise)
    const reading = mirror.refresh()
    await Promise.resolve()
    void mirror.refresh()
    const held = mirror.getSnapshot()
    const listener = vi.fn()
    const off = mirror.subscribe(listener)
    onTestFinished(off)
    let drained = false
    const disposal = mirror.dispose().then(() => {
      drained = true
    })
    await Promise.resolve()
    expect(drained).toBe(false)
    if (outcome === 'rejection') pending.reject('offline')
    else pending.resolve(answer)
    await Promise.all([reading, disposal])
    expect(mirror.getSnapshot()).toBe(held)
    expect(listener).not.toHaveBeenCalled()
    expect(describe).toHaveBeenCalledTimes(2)
  },
)

it('keeps provider disposal pending until its describe settles and removes its invalidations', async () => {
  const pending = deferred<RemoteResult<SettingsDescribeView>>()
  const ctx = new Context()
  const describe = vi.fn().mockReturnValue(pending.promise)
  const remote = new TestRemote(ctx, { settings: { describe } })
  onTestFinished(async () => {
    pending.resolve(described())
    await ctx.fiber.dispose()
  })
  const fiber = ctx.plugin({ inject: [...inject], apply })
  await fiber.await()
  await vi.waitFor(() => {
    expect(describe).toHaveBeenCalledOnce()
  })
  const face = ctx.get('configForms')!.describe()
  const held = face.getSnapshot()
  const listener = vi.fn()
  const off = face.subscribe(listener)
  onTestFinished(off)
  let drained = false
  const disposal = fiber.dispose().then(() => {
    drained = true
  })
  await vi.waitFor(() => {
    expect(ctx.get('configForms')).toBeUndefined()
  })
  expect(drained).toBe(false)
  remote.emit('settings/document-updated', ['native-settings', 4])
  ctx.emit('connection/reset')
  pending.resolve(described('late', 4))
  await disposal
  expect(face.getSnapshot()).toBe(held)
  expect(listener).not.toHaveBeenCalled()
  expect(describe).toHaveBeenCalledOnce()
})
