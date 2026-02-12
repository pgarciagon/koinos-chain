# Debug Merkle Mismatch Feature

## Overview

This feature automatically saves detailed debugging information to a JSON file when a "block previous state merkle mismatch" error is detected. The debug information includes:

- The block being applied (ID, height, previous block, claimed previous state merkle root, etc.)
- Parent chain information (up to 5 blocks back) with their state merkle roots
- Current head block information
- Last Irreversible Block (LIB) information
- Detailed mismatch comparison

## Implementation

### Files Modified

1. **`src/koinos/chain/controller.cpp`**:
   - Added `#include <nlohmann/json.hpp>` and `#include <fstream>`
   - Added `debug_merkle_mismatch()` method declaration in `controller_impl` class
   - Implemented `debug_merkle_mismatch()` function that collects and saves debug info
   - Added call to `debug_merkle_mismatch()` before the merkle root assertion

2. **`src/CMakeLists.txt`**:
   - Added `nlohmann_json::nlohmann_json` to the chain library's `target_link_libraries`

## How It Works

When a block is being applied and the merkle root check fails:

1. **Detection**: Before the assertion throws, the code checks if there's a mismatch:
   ```cpp
   if( block.header().previous_state_merkle_root()
       != util::converter::as< std::string >( parent_node->merkle_root() ) )
   {
     debug_merkle_mismatch( block, parent_node, db_lock );
   }
   ```

2. **Data Collection**: The `debug_merkle_mismatch()` function collects:
   - Block being applied details
   - Parent chain (traverses up to 5 parent blocks)
   - **State data** for the immediate parent node (all key-value pairs in all spaces)
   - Current head information
   - Root/LIB information
   - Mismatch details (what the block claims vs. what the parent node actually has)

   The `collect_state_data()` helper function iterates through all state spaces:
   - `metadata`: Chain metadata (head block, chain ID, etc.)
   - `contract_bytecode`: Contract WASM bytecode
   - `contract_metadata`: Contract metadata
   - `system_call_dispatch`: System call routing
   - `transaction_nonce`: Transaction nonces
   
   For each space, it collects up to 1000 entries (configurable) to avoid creating huge files. It also collects delta entries (changes made in this node).

3. **File Output**: Saves to a JSON file named:
   ```
   merkle_mismatch_debug_<block_id>.json
   ```
   Where `<block_id>` is the hex-encoded block ID.

## JSON Output Structure

```json
{
  "block_being_applied": {
    "id": "...",
    "height": 12345,
    "previous": "...",
    "previous_state_merkle_root": "...",
    "timestamp": 1234567890,
    "transaction_merkle_root": "...",
    "transaction_count": 5
  },
  "parent_chain": [
    {
      "id": "...",
      "revision": 12344,
      "parent_id": "...",
      "merkle_root": "...",
      "is_finalized": true,
      "head_info": {
        "height": 12344,
        "id": "...",
        "previous": "...",
        "last_irreversible_block": 12300,
        "head_block_time": 1234567890
      },
      "state": {
        "metadata": {
          "entries": [
            {
              "key": "...",
              "value": "...",
              "value_size": 123
            }
          ],
          "total_entries_found": 10,
          "truncated": false
        },
        "contract_bytecode": { ... },
        "contract_metadata": { ... },
        "system_call_dispatch": { ... },
        "transaction_nonce": { ... },
        "delta_entries": [
          {
            "space_system": true,
            "space_zone": "",
            "space_id": 0,
            "key": "...",
            "value": "...",
            "value_size": 123
          }
        ],
        "delta_entry_count": 5
      }
    },
    ...
  ],
  "current_head": {
    "id": "...",
    "revision": 12345,
    "parent_id": "...",
    "merkle_root": "...",
    "is_finalized": true
  },
  "root_lib": {
    "id": "...",
    "revision": 12300,
    "parent_id": "...",
    "merkle_root": "..."
  },
  "mismatch_details": {
    "block_claims_previous_state_merkle_root": "...",
    "parent_node_actual_merkle_root": "...",
    "match": false
  }
}
```

## Usage

When the error occurs, check the log output for:
```
Merkle mismatch debug info saved to: merkle_mismatch_debug_<block_id>.json
```

The JSON file will be created in the current working directory of the process.

## Troubleshooting

If the file cannot be written, the debug information will be logged to the error log instead:
```
Failed to save debug info to file: merkle_mismatch_debug_<block_id>.json
Debug info JSON: {...}
```

## Analyzing the Debug Output

Use the JSON file to:

1. **Compare merkle roots**: Check `mismatch_details` to see what the block claims vs. what the parent node has
2. **Trace parent chain**: Follow the `parent_chain` array to see the state of previous blocks
3. **Inspect state data**: The `state` field in the immediate parent contains all key-value pairs, allowing you to:
   - Verify what state the parent node actually has
   - Compare state entries to understand why merkle roots differ
   - Check delta entries to see what changes were made in that node
   - Identify missing or unexpected state entries
4. **Check head alignment**: Compare `current_head` with the block being applied to see if there's a fork
5. **Verify LIB**: Check `root_lib` to see what the Last Irreversible Block is

This information helps diagnose:
- Fork detection issues
- State synchronization problems
- Block production issues
- Chain reorganization scenarios
- State corruption or inconsistency issues
- Missing or incorrect state entries

### State Data Details

The state data includes:
- **Entries per space**: All key-value pairs in each state space (up to 1000 entries per space)
- **Delta entries**: Changes made in this specific node (useful for understanding what changed)
- **Truncation flag**: Indicates if entries were truncated due to the limit
- **Entry details**: Each entry includes the hex-encoded key, hex-encoded value, and value size

**Note**: State data is only collected for the immediate parent node (the one causing the mismatch) to keep file sizes manageable. If you need state data for other parent nodes, you can modify the `max_entries_per_space` parameter or remove the `parent_count == 0` condition.
