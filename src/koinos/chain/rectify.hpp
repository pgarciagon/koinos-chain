#pragma once

#include <koinos/chain/execution_context.hpp>

#include <koinos/protocol/protocol.pb.h>

namespace koinos::chain {

void maybe_rectify_state( execution_context&, const protocol::block&, protocol::block_receipt& );

/**
 * Returns true when a block header's previous_state_merkle_root is a known
 * historically-signed rectified value for the given parent block.
 *
 * Covers consensus scars where the root signed into the chain differs from the
 * root an honest application of the parent block produces. The parent block's
 * state and receipt remain the honest ones; only the comparison against the
 * signed header value is relaxed, and only for the exact (parent id, computed
 * root, claimed root) triple recorded here.
 */
bool acceptable_rectified_previous_root( const std::string& parent_block_id,
                                         const std::string& computed_parent_root,
                                         const std::string& claimed_previous_root );

} // namespace koinos::chain
