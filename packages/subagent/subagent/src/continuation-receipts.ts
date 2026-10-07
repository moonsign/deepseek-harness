/**
 * Host continuation admission receipts recovered from child-owned events and
 * maintained from committed events during each Session's residency.
 *
 * @module @deepseek-ai/dsh-subagent/continuation-receipts
 */

import type { Context } from '@deepseek-ai/cordis'
import { brandString } from '@deepseek-ai/dsh-brand'
import type { MessageId, MessageSource } from '@deepseek-ai/dsh-llm'
import type { Session, SessionEvent } from '@deepseek-ai/dsh-session'
import type { SessionObservation } from '@deepseek-ai/dsh-session-query'
import type { SubagentPromptRequestId } from './control-types.ts'

type Receipts = ReadonlyMap<SubagentPromptRequestId, MessageId>

/** Message sources are merge-extensible; this package does not own their RPC variant. */
function promptRequestId(source: MessageSource | undefined): SubagentPromptRequestId | undefined {
  return source?.kind === 'user' && 'rpcId' in source && typeof source.rpcId === 'string'
    ? brandString<SubagentPromptRequestId>(source.rpcId)
    : undefined
}

/** Inbox insertion retains admission even after claim, removal, or execution refusal. */
function recordReceipt(receipts: Map<SubagentPromptRequestId, MessageId>, event: SessionEvent): void {
  const messages =
    event.type === 'user/message' ? [event.data] : event.type === 'agent/inbox/spliced' ? event.data.inserted : []
  for (const message of messages) {
    const requestId = promptRequestId(message.source)
    if (requestId === undefined || receipts.has(requestId)) continue
    receipts.set(requestId, message.id)
  }
}

/** Process-local receipt projection; no optional projection service is required. */
export class ContinuationPromptReceipts {
  private readonly resident = new WeakMap<Session, Map<SubagentPromptRequestId, MessageId>>()
  private readonly recovered = new WeakMap<readonly SessionEvent[], Receipts>()

  /**
   * Maintain receipts from committed child events on the manager's owning context.
   * @param ctx - continuation owner whose disposal releases the subscription.
   */
  constructor(ctx: Context) {
    ctx.on('session/event', (session, event) => {
      if (session.header.parentSession === undefined || event.seq < session.inheritedEventCount) return
      let receipts = this.resident.get(session)
      if (receipts === undefined) {
        receipts = new Map()
        this.resident.set(session, receipts)
      }
      recordReceipt(receipts, event)
    })
  }

  /**
   * Recover receipts once per immutable observation already read for cold reconstruction.
   * @param observation - exact child cut including its fork-inherited prefix length.
   * @returns first accepted message per request identity in the child's own events.
   */
  recover(observation: SessionObservation): Receipts {
    const events = observation.events
    let receipts = this.recovered.get(events)
    if (receipts === undefined) {
      const folded = new Map<SubagentPromptRequestId, MessageId>()
      for (const event of events) {
        if (event.seq >= observation.inheritedEventCount) recordReceipt(folded, event)
      }
      receipts = folded
      this.recovered.set(events, receipts)
    }
    return receipts
  }

  /**
   * Seed a resumed Session without discarding events committed during materialization.
   * @param session - the newly resident child Session.
   * @param recovered - receipts from the earlier cold cut; these identities win collisions.
   */
  restore(session: Session, recovered: Receipts): void {
    const receipts = this.resident.get(session) ?? new Map<SubagentPromptRequestId, MessageId>()
    for (const [requestId, messageId] of recovered) receipts.set(requestId, messageId)
    this.resident.set(session, receipts)
  }

  /**
   * Read the maintained receipt for one resident child without reading historical events.
   * @param session - exact resident child Session.
   * @param source - requested message attribution; only identified human prompts deduplicate.
   * @returns original admitted message id, or undefined before admission.
   */
  accepted(session: Session, source: MessageSource | undefined): MessageId | undefined {
    return this.find(this.resident.get(session), source)
  }

  /**
   * Find one recovered receipt before a cold child needs an Activation.
   * @param receipts - receipts recovered from the authorized child's own events.
   * @param source - requested human prompt attribution.
   * @returns original admitted message id, or undefined before admission.
   */
  find(receipts: Receipts | undefined, source: MessageSource | undefined): MessageId | undefined {
    const requestId = promptRequestId(source)
    return requestId === undefined ? undefined : receipts?.get(requestId)
  }
}
