/** Test doubles for settings transport. */
import type { ConfigForm, ConfigFormSnapshot } from '@deepseek-ai/dsh-client-ui-settings/client'
import { vi } from 'vitest'

/** Handle over one stubbed scope: the scope, its write spy, and publication controls. */
export interface StubConfigForm<T> {
  /** The scope face handed to the service under test. */
  scope: ConfigForm<T>
  /** Spy behind `scope.set`; resolves immediately. */
  set: ReturnType<typeof vi.fn>
  /** Spy behind `scope.mutate`; resolves immediately. */
  mutate: ReturnType<typeof vi.fn<ConfigForm<T>['mutate']>>
  /** Spy behind `scope.mutateResult`; defaults to a local skip until scripted. */
  mutateResult: ReturnType<typeof vi.fn<ConfigForm<T>['mutateResult']>>
  /** Spy behind `scope.unset`; resolves immediately. */
  unset: ReturnType<typeof vi.fn>
  /** @returns how many listeners are currently subscribed (disposal assertions). */
  listenerCount(): number
  /**
   * Replace part of the snapshot and notify subscribers, as a Host
   * acceptance would.
   * @param next - snapshot fields to replace.
   */
  publish(next: Partial<ConfigFormSnapshot<T>>): void
}

/**
 * Build an in-memory settings scope for service specs: starts in the host
 * loading state, records writes, and lets the test publish Host acceptances.
 * @returns the stub handle.
 */
export function stubConfigForm<T>(): StubConfigForm<T> {
  let snapshot: ConfigFormSnapshot<T> = {
    status: 'loading',
    value: undefined,
    base: undefined,
    user: undefined,
    revision: undefined,
    writable: false,
    mode: 'host',
  }
  const listeners = new Set<() => void>()
  const set = vi.fn(() => Promise.resolve(true))
  const mutate = vi.fn<ConfigForm<T>['mutate']>(() => Promise.resolve(true))
  const mutateResult = vi.fn<ConfigForm<T>['mutateResult']>(() => Promise.resolve(undefined))
  const unset = vi.fn(() => Promise.resolve(true))
  return {
    scope: {
      getSnapshot: () => snapshot,
      subscribe: (listener) => {
        listeners.add(listener)
        return () => {
          listeners.delete(listener)
        }
      },
      mutate,
      mutateResult,
      set,
      unset,
    },
    set,
    mutate,
    mutateResult,
    unset,
    listenerCount: () => listeners.size,
    publish: (next) => {
      snapshot = { ...snapshot, ...next }
      for (const listener of [...listeners]) listener()
    },
  }
}
