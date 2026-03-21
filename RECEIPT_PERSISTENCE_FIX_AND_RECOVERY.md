# Receipt Persistence Fix And Recovery Notes

## TL;DR

This branch contains the minimal code fix for the rectification receipt persistence bug in `koinos-chain`.

The bug was that rectified block receipts were computed, but the original unrectified receipt was the one persisted to `block_store` and broadcast through `koinos.block.accept`.

That means:

- the fix in this branch prevents future bad receipts from being written,
- but it does not retroactively repair block stores that already contain corrupted historical receipts.

For already-affected nodes, `chain.verify-blocks=true` is still required during reindex so that the node re-executes blocks instead of trusting persisted deltas.

This branch also bumps the chain version to `1.5.1`.

## Version

This branch changes:

- `koinos-chain` `1.4.1 -> 1.5.1`

The bump is justified as a patch release because the behavior change is a correctness fix in persisted receipt handling, not a protocol or API redesign.

## Bug Summary

The failure happened inside `controller_impl::apply_block()`.

The code first copied the current receipt from the block application context into `res.receipt`, then passed that copy through `maybe_rectify_state()`.

Relevant code:

- [src/koinos/chain/controller.cpp#L419](/Users/pgarcgo/code/koinos_code/.worktrees/koinos-chain-receipt-fix/src/koinos/chain/controller.cpp#L419)
- [src/koinos/chain/controller.cpp#L421](/Users/pgarcgo/code/koinos_code/.worktrees/koinos-chain-receipt-fix/src/koinos/chain/controller.cpp#L421)

Conceptually:

```cpp
res.receipt = std::get< protocol::block_receipt >( ctx.receipt() );
maybe_rectify_state( ctx, block, *res.receipt );
```

So far that is correct: rectification updates the copied receipt held in `res.receipt`.

The bug appeared afterward, because the writes still used `ctx.receipt()` instead of `*res.receipt`.

## Root Cause

Two persistence paths were wrong.

### 1. Receipt written to block_store

Before the fix, the `block_store.add_block` request wrote the original receipt:

- previous behavior used `std::get< protocol::block_receipt >( ctx.receipt() )`

After the fix, it writes the rectified receipt:

- [src/koinos/chain/controller.cpp#L427](/Users/pgarcgo/code/koinos_code/.worktrees/koinos-chain-receipt-fix/src/koinos/chain/controller.cpp#L427)

### 2. Receipt sent in `koinos.block.accept`

Before the fix, the accepted-block broadcast also used the original receipt.

After the fix, it uses the rectified receipt:

- [src/koinos/chain/controller.cpp#L530](/Users/pgarcgo/code/koinos_code/.worktrees/koinos-chain-receipt-fix/src/koinos/chain/controller.cpp#L530)

## Exact Code Change

This branch is intentionally small.

The code fix is only two behavioral substitutions in `controller.cpp`:

```cpp
req.mutable_add_block()->mutable_receipt_to_add()->CopyFrom( *res.receipt );
```

and:

```cpp
*ba.mutable_receipt() = *res.receipt;
```

instead of using `ctx.receipt()`.

That is enough to ensure the receipt persisted and broadcast after rectification is the same receipt that was actually produced by the block application path.

## Why This Bug Matters

When reindexing, `chain` can rebuild state by replaying persisted deltas rather than re-executing the full block.

That fast path is:

- [src/koinos/chain/indexer.cpp#L234](/Users/pgarcgo/code/koinos_code/.worktrees/koinos-chain-receipt-fix/src/koinos/chain/indexer.cpp#L234)
- [src/koinos/chain/indexer.cpp#L241](/Users/pgarcgo/code/koinos_code/.worktrees/koinos-chain-receipt-fix/src/koinos/chain/indexer.cpp#L241)

When `_verify_blocks` is `false`, reindex uses:

```cpp
_controller.apply_block_delta( block_item.block(), block_item.receipt(), _target_head.height() );
```

If the stored receipt is missing rectification deltas, reindex reconstructs the wrong state. That can later surface as a merkle mismatch when the node tries to continue syncing from peers.

## Why `verify-blocks=true` Is Still Required For Affected Nodes

This is the operationally important point.

The code fix prevents future corruption. It does not rewrite history inside an already-corrupted `block_store`.

If a node has already persisted bad receipts, upgrading to this fixed binary alone is not enough. On restart, the node may still read those historical receipts and rebuild the wrong state if it stays on the delta replay path.

That is why recovery for an already-affected node still requires:

```yaml
chain:
  verify-blocks: true
```

or the equivalent CLI/runtime setting.

`verify-blocks` is parsed from config/CLI in:

- [src/koinos_chain.cpp#L121](/Users/pgarcgo/code/koinos_code/.worktrees/koinos-chain-receipt-fix/src/koinos_chain.cpp#L121)
- [src/koinos_chain.cpp#L178](/Users/pgarcgo/code/koinos_code/.worktrees/koinos-chain-receipt-fix/src/koinos_chain.cpp#L178)

When it is enabled, the indexer switches from receipt replay to full block verification:

```cpp
rpc::chain::submit_block_request submit_block;
*submit_block.mutable_block() = block_item.block();
_controller.submit_block( submit_block, _target_head.height() );
```

This matters because:

- `apply_block_delta()` trusts the stored receipt,
- `submit_block()` re-executes the block from the block body and reconstructs state from execution.

So for already-corrupted nodes, `verify-blocks=true` is not a workaround for the code fix. It is the required recovery path to escape corrupted persisted receipts already written before the fix existed.

## Practical Rollout Guidance

### Case 1: New deployments or unaffected nodes

If a node never persisted the bad receipts, this branch is enough. It prevents future corruption and normal indexing behavior can remain unchanged.

### Case 2: Existing affected nodes

If a node already synced through the affected window and persisted bad receipts, the recommended recovery path is:

1. Upgrade to a binary containing this fix.
2. Restart with `chain.verify-blocks=true`.
3. Let the node reindex by re-executing blocks.
4. Once the node has rebuilt state from execution and is healthy again, decide whether to keep or remove `verify-blocks` depending on operational preference.

The key point is that step 2 is needed because the node's historical receipt data is already wrong.

## What This Branch Does Not Attempt To Solve

This branch is intentionally scoped.

It does not:

- mutate old receipts already stored in `block_store`,
- introduce a fallback path inside `apply_block_delta()`,
- automatically detect old corrupted receipts and self-heal,
- change network protocol behavior.

It only fixes the persistence bug at the point where the wrong receipt was being written.

## Branch Intent

This branch is meant to stay clean and isolated relative to `master`:

- rectification receipt persistence fix,
- patch version bump to `1.5.1`,
- documentation for release and recovery context.

That makes it suitable as the base for a focused PR against the main branch later.
