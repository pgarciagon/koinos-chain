# State Persistence in Koinos Chain

This document explains how state is saved to the database in the Koinos chain implementation.

## "Block previous state merkle mismatch" — Parent node and check

This error is thrown when the **incoming block’s** `header.previous_state_merkle_root` does not match the **parent state node’s** merkle root as stored by this node.

### How the parent node is obtained

**Location**: `src/koinos/chain/controller.cpp` (around 316–320 and 391–394).

1. **Parent block ID**  
   The parent is identified by the block’s header:
   ```cpp
   auto parent_id = util::converter::to< crypto::multihash >( block.header().previous() );
   ```
   So the parent is whatever block ID is in `block.header().previous()`.

2. **Parent state node**  
   The parent’s state node is loaded from the state DB by that ID:
   ```cpp
   auto parent_node = _db.get_node( parent_id, db_lock );
   ```
   So:
   - **Parent node** = state node for the block whose ID is `block.header().previous()`.
   - It is the same node used to create the new block node:  
     `_db.create_writable_node( parent_id, block_id, ... )` (line 351).

3. **Merkle check**  
   The mismatch is asserted here:
   ```cpp
   KOINOS_ASSERT( block.header().previous_state_merkle_root()
                    == util::converter::as< std::string >( parent_node->merkle_root() ),
                  state_merkle_mismatch_exception,
                  "block previous state merkle mismatch" );
   ```
   So:
   - **Left side**: `block.header().previous_state_merkle_root()` — value in the block you’re applying.
   - **Right side**: `parent_node->merkle_root()` — merkle root of the state node for `block.header().previous()` as known to this process (from `_db`).

### When the error happens

- **Producer side**: The block was built on a different view of the chain (e.g. different head or different state after the parent), so the producer’s “previous state merkle root” does not match this node’s parent state.
- **Sync / fork**: You’re applying a block whose `previous` points to a block you have, but the block header says the previous state root was X while your `parent_node->merkle_root()` is Y (e.g. different fork or reorg).
- **Head / LIB**: The block’s `previous` might point at your current head, but your head’s state node might not be finalized yet, or you might be comparing against a different head (e.g. after a reorg). Then `parent_node` is correct for `previous`, but its merkle root doesn’t match what the block claims.

So the parent node is always obtained by: **take `block.header().previous()` as the block ID, then `_db.get_node(parent_id, db_lock)`**. The error means the block’s claimed previous state root and this node’s parent state root disagree.

## Overview

State persistence in Koinos chain uses a hierarchical node-based system managed by the `state_db` library. State changes are organized in a tree structure where:

- **Root nodes** represent committed state
- **Block nodes** represent state at a specific block
- **Transaction nodes** (anonymous nodes) represent state changes within a transaction
- **Anonymous nodes** can be created for temporary read-only operations

## Key Components

### 1. State Database (`state_db`)

The `state_db::database` class (`_db` in `controller_impl`) manages the persistent storage. It's opened with a filesystem path:

```cpp
_db.open(p, [&](state_db::state_node_ptr root) {
    // Initialize genesis state
});
```

**Location**: `src/koinos/chain/controller.cpp:177`

### 2. State Nodes

State nodes represent snapshots of state at different points in time:

- **`state_node_ptr`**: Represents a block-level state node
- **`anonymous_state_node_ptr`**: Represents temporary state nodes (transactions, read-only operations)
- **`abstract_state_node_ptr`**: Base type for both

**Key Methods**:
- `put_object(space, key, value)`: Write state
- `get_object(space, key)`: Read state
- `remove_object(space, key)`: Delete state
- `create_anonymous_node()`: Create a child node
- `commit()`: Commit changes to parent node
- `merkle_root()`: Get merkle root hash

## State Write Flow

### 1. Writing State Objects

State is written through the `put_object` system call:

**Location**: `src/koinos/chain/system_calls.cpp:1043-1056`

```cpp
THUNK_DEFINE(void, put_object, 
    (const object_space&)space, 
    (const std::string&)key, 
    (const std::string&)obj)
{
    // Check read-only mode
    KOINOS_ASSERT(!context.read_only(), ...);
    
    // Check permissions
    state::assert_permissions(context, space);
    
    // Get current state node
    auto state = context.get_state_node();
    
    // Convert and write
    auto val = util::converter::as<state_db::object_value>(obj);
    context.resource_meter().use_disk_storage(
        state->put_object(space, key, &val)
    );
}
```

### 2. State Organization: Spaces and Keys

State is organized into **spaces** (namespaces) and **keys**:

**Spaces** (`src/koinos/chain/state.hpp:23-31`):
- `contract_bytecode()`: Contract WASM bytecode
- `contract_metadata()`: Contract metadata
- `system_call_dispatch()`: System call routing
- `metadata()`: Chain metadata (head block, chain ID, etc.)
- `transaction_nonce()`: Transaction nonces

**Keys** (`src/koinos/chain/state.hpp:33-52`):
- Predefined keys like `head_block`, `chain_id`, `genesis_key`, etc.
- Keys are hashed strings for consistency

### 3. Transaction-Level State Changes

When a transaction executes:

**Location**: `src/koinos/chain/system_calls.cpp:513-539`

1. An anonymous transaction node is created from the block node:
   ```cpp
   auto block_node = context.get_state_node();
   auto trx_node = block_node->create_anonymous_node();
   context.set_state_node(trx_node, block_node->parent());
   ```

2. Transaction operations execute and write to `trx_node`

3. On success, the transaction node commits to the block node:
   ```cpp
   trx_node->commit();  // Merges changes into block_node
   ```

4. On failure, the transaction node is discarded (changes are lost)

### 4. Block-Level State Persistence

When a block is applied:

**Location**: `src/koinos/chain/controller.cpp:351-502`

1. **Create Block Node**: A writable block node is created:
   ```cpp
   block_node = _db.create_writable_node(parent_id, block_id, block.header(), db_lock);
   ```

2. **Execute Block**: Transactions execute and commit to `block_node`

3. **Finalize Node**: After all transactions, the node is finalized:
   ```cpp
   _db.finalize_node(block_id, unique_db_lock);
   ```
   This calculates the merkle root and makes the node immutable.

4. **Commit to Database**: If this block becomes the Last Irreversible Block (LIB), it's committed:
   ```cpp
   if (lib > _db.get_root(unique_db_lock)->revision()) {
       auto lib_id = _db.get_node_at_revision(lib, block_id, unique_db_lock)->id();
       _db.commit_node(lib_id, unique_db_lock);  // Persists to disk
   }
   ```

## State Hierarchy Example

```
Root Node (committed state)
  └── Block Node #100 (finalized)
       └── Transaction Node A (committed to block)
       └── Transaction Node B (committed to block)
  └── Block Node #101 (finalized)
       └── Transaction Node C (committed to block)
```

## Key Persistence Points

### 1. Genesis State

**Location**: `src/koinos/chain/controller.cpp:181-188`

Genesis objects are written directly to the root node when the database is first opened:

```cpp
for (const auto& entry: data.entries()) {
    root->put_object(entry.space(), entry.key(), &entry.value());
}
```

### 2. Transaction Commit

**Location**: `src/koinos/chain/system_calls.cpp:539`

Transaction nodes commit to their parent block node:
```cpp
trx_node->commit();  // Merges into block_node
```

### 3. Block Finalization

**Location**: `src/koinos/chain/controller.cpp:486`

Block nodes are finalized after all transactions:
```cpp
_db.finalize_node(block_id, unique_db_lock);
```

### 4. LIB Commit

**Location**: `src/koinos/chain/controller.cpp:498-502`

Only blocks that become the Last Irreversible Block are committed to persistent storage:
```cpp
_db.commit_node(lib_id, unique_db_lock);  // Actually writes to disk
```

## State Access Patterns

### Read-Only Operations

For read-only operations (like `read_contract`), anonymous nodes are created:

**Location**: `src/koinos/chain/controller.cpp:1014`

```cpp
ctx.set_state_node(_db.get_head(db_lock)->create_anonymous_node());
```

These nodes are never committed and are discarded after use.

### Write Operations

Write operations require:
1. A writable state node (not read-only)
2. Proper permissions (checked via `state::assert_permissions`)
3. Resource metering (disk storage is tracked)

## Error Handling

If a block application fails before finalization:

**Location**: `src/koinos/chain/controller.cpp:571-573`

```cpp
if (block_node && !block_node->is_finalized()) {
    _db.discard_node(block_node->id(), db_lock);
}
```

The node is discarded and all changes are lost.

## Database Location

The database is stored at the filesystem path provided to `controller::open()`:

```cpp
controller.open(path, genesis_data, algo, reset);
```

The `state_db` library handles the actual file I/O and persistence format.

## Summary

1. **State writes** happen through `put_object()` system calls
2. **Transaction nodes** accumulate changes and commit to block nodes
3. **Block nodes** are finalized after all transactions complete
4. **Only LIB blocks** are committed to persistent storage
5. **The `state_db` library** handles the actual disk persistence

This design allows for:
- Efficient fork handling (multiple block nodes can exist)
- Transaction rollback (discard transaction nodes on failure)
- Incremental persistence (only commit irreversible blocks)
- Fast reads (read from any node in the tree)
