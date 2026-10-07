/** View-independent manager subscriptions and operation ownership. */
import { Context } from '@deepseek-ai/cordis'
import type {
  ChangeResult,
  InspectOptions,
  PluginSpecInspection,
  RemoteResult,
} from '@deepseek-ai/dsh-api-remotes/client'
import { TestRemote } from '@deepseek-ai/dsh-client-test-runtime'
import { expect, it, onTestFinished, vi } from 'vitest'
import { PluginManagerController } from '../src/client/manager-store.ts'

const inspected: RemoteResult<PluginSpecInspection> = {
  ok: true,
  value: {
    status: 'accepted',
    kind: 'registry',
    name: 'native-bundle',
    version: '1.0.0',
    bundle: true,
    registry: null,
  },
}
const installed = {
  ok: true,
  value: { changed: true, application: 'applied', stage: 'install', target: 'native-bundle', bundle: 'native-bundle' },
} satisfies RemoteResult<ChangeResult>

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((accept) => {
    resolve = accept
  })
  return { promise, resolve }
}

function fixture(release = () => {}) {
  const ctx = new Context()
  const inspect = vi
    .fn<
    (_spec: string, _options: InspectOptions, _signal: AbortSignal) => Promise<RemoteResult<PluginSpecInspection>>
  >()
    .mockResolvedValue(inspected)
  const installBundle = vi.fn().mockResolvedValue(installed)
  const cancelInstall = vi.fn()
  const setBundleEnabled = vi.fn().mockResolvedValue({ ok: true, value: { ...installed.value, stage: 'enable' } })
  new TestRemote(ctx, {
    pluginInventory: {
      list: vi.fn().mockResolvedValue({ ok: true, value: { entries: [], managementAvailable: true } }),
    },
    pluginManager: {
      listBundles: vi.fn().mockResolvedValue({ ok: true, value: [] }),
      listPlugins: vi.fn().mockResolvedValue({ ok: true, value: [] }),
      registries: vi
        .fn()
        .mockResolvedValue({ ok: true, value: { registry: null, fallbackRegistries: [], resolved: null } }),
      inspect,
      installBundle,
      cancelInstall,
      setBundleEnabled,
    },
    pluginRegistryProbe: { fastest: vi.fn() },
  })
  const controller = new PluginManagerController(ctx)
  const face = controller.headless()
  onTestFinished(async () => {
    release()
    controller.dispose()
    await ctx.fiber.dispose()
  })
  return { ctx, controller, face, inspect, installBundle, cancelInstall, setBundleEnabled }
}

it('observes and changes manager state without configuration, locale or slot services', async () => {
  const { ctx, controller, face, setBundleEnabled } = fixture()
  expect(ctx.get('configForms')).toBeUndefined()
  expect(ctx.get('slots')).toBeUndefined()
  expect(ctx.get('locale')).toBeUndefined()
  expect(face.getSnapshot()).toBe(controller.getSnapshot())
  const listener = vi.fn()
  const unsubscribe = face.subscribe(listener)
  onTestFinished(unsubscribe)
  face.ensure()
  await controller.load()
  expect(face.getSnapshot().status).toBe('ready')
  expect(listener).toHaveBeenCalled()
  unsubscribe()
  listener.mockClear()
  face.setEnabled('native-bundle', true)
  await vi.waitFor(() => {
    expect(face.getSnapshot().busy).toEqual([])
  })
  expect(setBundleEnabled).toHaveBeenCalledExactlyOnceWith('native-bundle', true)
  expect(listener).not.toHaveBeenCalled()
})

it('retains inspection across view subscriptions and aborts it when its controller is disposed', async () => {
  const pending = deferred<RemoteResult<PluginSpecInspection>>()
  const { controller, face, inspect, installBundle, cancelInstall } = fixture(() => {
    pending.resolve(inspected)
  })
  inspect.mockReturnValue(pending.promise)
  face.openInstall()
  face.editInstallSpec('native-bundle')
  face.runInstall()
  await vi.waitFor(() => {
    expect(inspect).toHaveBeenCalledOnce()
  })
  const signal = inspect.mock.calls[0]![2]
  const unsubscribe = face.subscribe(() => {})
  unsubscribe()
  expect(signal.aborted).toBe(false)
  expect(controller.headless().getSnapshot()).toBe(face.getSnapshot())
  const held = face.getSnapshot()
  const listener = vi.fn()
  const off = face.subscribe(listener)
  onTestFinished(off)
  controller.dispose()
  expect(signal.aborted).toBe(true)
  pending.resolve(inspected)
  await pending.promise
  expect(face.getSnapshot()).toBe(held)
  expect(listener).not.toHaveBeenCalled()
  expect(installBundle).not.toHaveBeenCalled()
  expect(cancelInstall).not.toHaveBeenCalled()
})

it('disposes local tracking without cancelling a Host-owned installation or publishing its late reply', async () => {
  const pending = deferred<RemoteResult<ChangeResult>>()
  const { controller, face, installBundle, cancelInstall } = fixture(() => {
    pending.resolve(installed)
  })
  installBundle.mockReturnValue(pending.promise)
  face.openInstall()
  face.editInstallSpec('native-bundle')
  face.runInstall()
  await vi.waitFor(() => {
    expect(installBundle).toHaveBeenCalledOnce()
  })
  const held = face.getSnapshot()
  expect(held.install.phase).toBe('starting')
  controller.dispose()
  pending.resolve(installed)
  await pending.promise
  expect(cancelInstall).not.toHaveBeenCalled()
  expect(face.getSnapshot()).toBe(held)
})
