#include <koinos/block_store/block_store.pb.h>
#include <koinos/broadcast/broadcast.pb.h>

#include <koinos/chain/constants.hpp>
#include <koinos/chain/controller.hpp>
#include <koinos/chain/exceptions.hpp>
#include <koinos/chain/execution_context.hpp>
#include <koinos/chain/host_api.hpp>
#include <koinos/chain/rectify.hpp>
#include <koinos/chain/state.hpp>
#include <koinos/chain/system_calls.hpp>

#include <koinos/exception.hpp>

#include <koinos/protocol/protocol.pb.h>

#include <koinos/rpc/block_store/block_store_rpc.pb.h>
#include <koinos/rpc/chain/chain_rpc.pb.h>
#include <koinos/rpc/mempool/mempool_rpc.pb.h>

#include <koinos/state_db/state_db.hpp>

#include <koinos/util/base58.hpp>
#include <koinos/util/base64.hpp>
#include <koinos/util/conversion.hpp>
#include <koinos/util/hex.hpp>
#include <koinos/util/services.hpp>

#include <koinos/vm_manager/vm_backend.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <list>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <thread>
#include <filesystem>

#include <boost/interprocess/streams/vectorstream.hpp>
#include <nlohmann/json.hpp>

namespace koinos::chain {

using namespace std::string_literals;
using namespace std::chrono_literals;

using fork_data = std::pair< std::vector< block_topology >, block_topology >;

namespace detail {

std::string format_time( int64_t time )
{
  std::stringstream ss;

  auto seconds  = time % 60;
  time         /= 60;
  auto minutes  = time % 60;
  time         /= 60;
  auto hours    = time % 24;
  time         /= 24;
  auto days     = time % 365;
  auto years    = time / 365;

  if( years )
  {
    ss << years << "y, " << days << "d, ";
  }
  else if( days )
  {
    ss << days << "d, ";
  }

  ss << std::setw( 2 ) << std::setfill( '0' ) << hours;
  ss << std::setw( 1 ) << "h, ";
  ss << std::setw( 2 ) << std::setfill( '0' ) << minutes;
  ss << std::setw( 1 ) << "m, ";
  ss << std::setw( 2 ) << std::setfill( '0' ) << seconds;
  ss << std::setw( 1 ) << "s";
  return ss.str();
}

// Encode binary data as base64 (ASCII-only output, safe for JSON).
std::string base64_encode( const std::string& data )
{
  static const char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  out.reserve( ( ( data.size() + 2 ) / 3 ) * 4 );
  const unsigned char* p = reinterpret_cast< const unsigned char* >( data.data() );
  size_t i = 0;
  for( ; i + 3 <= data.size(); i += 3, p += 3 )
  {
    out += table[ ( p[ 0 ] >> 2 ) & 0x3F ];
    out += table[ ( ( p[ 0 ] << 4 ) | ( p[ 1 ] >> 4 ) ) & 0x3F ];
    out += table[ ( ( p[ 1 ] << 2 ) | ( p[ 2 ] >> 6 ) ) & 0x3F ];
    out += table[ p[ 2 ] & 0x3F ];
  }
  if( i + 1 == data.size() )
  {
    out += table[ ( p[ 0 ] >> 2 ) & 0x3F ];
    out += table[ ( ( p[ 0 ] << 4 ) ) & 0x3F ];
    out += "==";
  }
  else if( i + 2 == data.size() )
  {
    out += table[ ( p[ 0 ] >> 2 ) & 0x3F ];
    out += table[ ( ( p[ 0 ] << 4 ) | ( p[ 1 ] >> 4 ) ) & 0x3F ];
    out += table[ ( ( p[ 1 ] << 2 ) ) & 0x3F ];
    out += "=";
  }
  return out;
}

struct apply_block_options
{
  uint64_t index_to;
  std::chrono::system_clock::time_point application_time;
  bool propose_block;
};

struct apply_block_result
{
  std::optional< protocol::block_receipt > receipt;
  std::vector< uint32_t > failed_transaction_indices;
};

class controller_impl final
{
public:
  controller_impl( uint64_t read_compute_bandwith_limit,
                   uint32_t syscall_bufsize,
                   std::optional< uint64_t > pending_transaction_limit );
  ~controller_impl();

  void open( const std::filesystem::path& p, const genesis_data& data, fork_resolution_algorithm algo, bool reset );
  void close();
  void set_client( std::shared_ptr< mq::client > c );
  void set_log_directory( const std::filesystem::path& log_dir );

  apply_block_result apply_block( const protocol::block& block, const apply_block_options& opts );
  void apply_block_delta( const protocol::block&, const protocol::block_receipt&, uint64_t );

  rpc::chain::submit_transaction_response submit_transaction( const rpc::chain::submit_transaction_request& );
  rpc::chain::get_head_info_response get_head_info( const rpc::chain::get_head_info_request& );
  rpc::chain::get_chain_id_response get_chain_id( const rpc::chain::get_chain_id_request& );
  rpc::chain::get_fork_heads_response get_fork_heads( const rpc::chain::get_fork_heads_request& );
  rpc::chain::read_contract_response read_contract( const rpc::chain::read_contract_request& );
  rpc::chain::get_account_nonce_response get_account_nonce( const rpc::chain::get_account_nonce_request& );
  rpc::chain::get_account_rc_response get_account_rc( const rpc::chain::get_account_rc_request& );
  rpc::chain::get_resource_limits_response get_resource_limits( const rpc::chain::get_resource_limits_request& );
  rpc::chain::invoke_system_call_response invoke_system_call( const rpc::chain::invoke_system_call_request& );

private:
  state_db::database _db;
  std::shared_ptr< vm_manager::vm_backend > _vm_backend;
  std::shared_ptr< mq::client > _client;
  uint64_t _read_compute_bandwidth_limit;
  uint32_t _syscall_bufsize;
  std::optional< uint64_t > _pending_transaction_limit;
  std::shared_mutex _cached_head_block_mutex;
  std::shared_ptr< const protocol::block > _cached_head_block;
  std::filesystem::path _log_directory;

  void validate_block( const protocol::block& b );
  void validate_transaction( const protocol::transaction& t );

  fork_data get_fork_data( state_db::shared_lock_ptr db_lock );

  void debug_merkle_mismatch( const protocol::block& block,
                               state_db::state_node_ptr parent_node,
                               state_db::shared_lock_ptr db_lock );

  nlohmann::json collect_state_data( state_db::state_node_ptr node, int max_entries_per_space = 1000 );
};

controller_impl::controller_impl( uint64_t read_compute_bandwidth_limit,
                                  uint32_t syscall_bufsize,
                                  std::optional< uint64_t > pending_transaction_limit ):
    _read_compute_bandwidth_limit( read_compute_bandwidth_limit ),
    _syscall_bufsize( syscall_bufsize ),
    _pending_transaction_limit( pending_transaction_limit )
{
  _vm_backend = vm_manager::get_vm_backend(); // Default is fizzy
  KOINOS_ASSERT( _vm_backend, unknown_backend_exception, "could not get vm backend" );

  _cached_head_block = std::make_shared< const protocol::block >( protocol::block() );

  _vm_backend->initialize();
  LOG( info ) << "Initialized " << _vm_backend->backend_name() << " VM backend";
}

controller_impl::~controller_impl()
{
  close();
}

void controller_impl::open( const std::filesystem::path& p,
                            const chain::genesis_data& data,
                            fork_resolution_algorithm algo,
                            bool reset )
{
  state_db::state_node_comparator_function comp;

  switch( algo )
  {
    case fork_resolution_algorithm::block_time:
      comp = &state_db::block_time_comparator;
      break;
    case fork_resolution_algorithm::pob:
      comp = &state_db::pob_comparator;
      break;
    case fork_resolution_algorithm::fifo:
      [[fallthrough]];
    default:
      comp = &state_db::fifo_comparator;
  }

  _db.open(
    p,
    [ & ]( state_db::state_node_ptr root )
    {
      // Write genesis objects into the database
      for( const auto& entry: data.entries() )
      {
        KOINOS_ASSERT( !root->get_object( entry.space(), entry.key() ),
                       unexpected_state_exception,
                       "encountered unexpected object in initial state" );

        root->put_object( entry.space(), entry.key(), &entry.value() );
      }
      LOG( info ) << "Wrote " << data.entries().size() << " genesis objects into new database";

      // Read genesis public key from the database, assert its existence at the correct location
      KOINOS_ASSERT( root->get_object( state::space::metadata(), state::key::genesis_key ),
                     unexpected_state_exception,
                     "could not find genesis public key in database" );

      // Calculate and write the chain ID into the database
      auto chain_id = crypto::hash( koinos::crypto::multicodec::sha2_256, data );
      LOG( info ) << "Calculated chain ID: " << chain_id;
      auto chain_id_str = util::converter::as< std::string >( chain_id );
      KOINOS_ASSERT( !root->get_object( chain::state::space::metadata(), chain::state::key::chain_id ),
                     unexpected_state_exception,
                     "encountered unexpected chain id in initial state" );

      root->put_object( chain::state::space::metadata(), chain::state::key::chain_id, &chain_id_str );
      LOG( info ) << "Wrote chain ID into new database";
    },
    comp,
    _db.get_unique_lock() );

  if( reset )
  {
    LOG( info ) << "Resetting database...";
    _db.reset( _db.get_unique_lock() );
  }

  auto head = _db.get_head( _db.get_shared_lock() );
  LOG( info ) << "Opened database at block - Height: " << head->revision() << ", ID: " << head->id();
}

void controller_impl::close()
{
  _db.close( _db.get_unique_lock() );
}

void controller_impl::set_client( std::shared_ptr< mq::client > c )
{
  _client = c;
}

void controller_impl::set_log_directory( const std::filesystem::path& log_dir )
{
  _log_directory = log_dir;
}

void controller_impl::validate_block( const protocol::block& b )
{
  KOINOS_ASSERT( b.id().size(),
                 missing_required_arguments_exception,
                 "missing expected field in block: ${field}",
                 ( "field", "id" ) );
  KOINOS_ASSERT( b.has_header(),
                 missing_required_arguments_exception,
                 "missing expected field in block: ${field}",
                 ( "field", "header" )( "block_id", util::to_hex( b.id() ) ) );
  KOINOS_ASSERT( b.header().previous().size(),
                 missing_required_arguments_exception,
                 "missing expected field in block header: ${field}",
                 ( "field", "previous" )( "block_id", util::to_hex( b.id() ) ) );
  KOINOS_ASSERT( b.header().height(),
                 missing_required_arguments_exception,
                 "missing expected field in block header: ${field}",
                 ( "field", "height" )( "block_id", util::to_hex( b.id() ) ) );
  KOINOS_ASSERT( b.header().timestamp(),
                 missing_required_arguments_exception,
                 "missing expected field in block header: ${field}",
                 ( "field", "timestamp" )( "block_id", util::to_hex( b.id() ) ) );
  KOINOS_ASSERT( b.header().previous_state_merkle_root().size(),
                 missing_required_arguments_exception,
                 "missing expected field in block header: ${field}",
                 ( "field", "previous_state_merkle_root" )( "block_id", util::to_hex( b.id() ) ) );
  KOINOS_ASSERT( b.header().transaction_merkle_root().size(),
                 missing_required_arguments_exception,
                 "missing expected field in block header: ${field}",
                 ( "field", "transaction_merkle_root" )( "block_id", util::to_hex( b.id() ) ) );
  KOINOS_ASSERT( b.signature().size(),
                 missing_required_arguments_exception,
                 "missing expected field in block: ${field}",
                 ( "field", "signature_data" )( "block_id", util::to_hex( b.id() ) ) );

  for( const auto& t: b.transactions() )
    validate_transaction( t );
}

void controller_impl::validate_transaction( const protocol::transaction& t )
{
  KOINOS_ASSERT( t.id().size(),
                 missing_required_arguments_exception,
                 "missing expected field in transaction: ${field}",
                 ( "field", "id" ) );
  KOINOS_ASSERT( t.has_header(),
                 missing_required_arguments_exception,
                 "missing expected field in transaction: ${field}",
                 ( "field", "header" )( "transaction_id", util::to_hex( t.id() ) ) );
  KOINOS_ASSERT( t.header().payer().size(),
                 missing_required_arguments_exception,
                 "missing expected field in transaction: ${field}",
                 ( "field", "payer" )( "transaction_id", util::to_hex( t.id() ) ) );
  KOINOS_ASSERT( t.header().rc_limit(),
                 missing_required_arguments_exception,
                 "missing expected field in transaction header: ${field}",
                 ( "field", "rc_limit" )( "transaction_id", util::to_hex( t.id() ) ) );
  KOINOS_ASSERT( t.header().operation_merkle_root().size(),
                 missing_required_arguments_exception,
                 "missing expected field in transaction header: ${field}",
                 ( "field", "operation_merkle_root" )( "transaction_id", util::to_hex( t.id() ) ) );
  KOINOS_ASSERT( t.signatures().size(),
                 missing_required_arguments_exception,
                 "missing expected field in transaction: ${field}",
                 ( "field", "signature_data" )( "transaction_id", util::to_hex( t.id() ) ) );
}

nlohmann::json controller_impl::collect_state_data( state_db::state_node_ptr node, int max_entries_per_space )
{
  nlohmann::json state_data = nlohmann::json::object();

  if( !node )
    return state_data;

  // Treat string values as raw bytes and encode as base64 (always JSON-safe ASCII).
  auto safe_json_assign = []( nlohmann::json& j, const std::string& key, const std::string& value ) {
    try
    {
      j[ key ] = detail::base64_encode( value );
    }
    catch( ... )
    {
      j[ key ] = "base64_error";
    }
  };

  try
  {
    execution_context ctx( _vm_backend, intent::read_only );
    ctx.push_frame( stack_frame{ .call_privilege = privilege::kernel_mode } );
    ctx.set_state_node( node->create_anonymous_node() );
    ctx.reset_cache();

    // List of spaces to iterate through
    std::vector< std::pair< std::string, object_space > > spaces = {
        { "metadata", state::space::metadata() },
        { "system_call_dispatch", state::space::system_call_dispatch() } };

    for( const auto& [ space_name, space ]: spaces )
    {
      nlohmann::json space_data = nlohmann::json::object();
      space_data[ "entries" ] = nlohmann::json::array();
      int entry_count = 0;

      try
      {
        // Start iteration from empty string to get first object
        std::string current_key = "";
        bool has_more = true;

        while( has_more && entry_count < max_entries_per_space )
        {
          const auto [ result, next_key ] = node->get_next_object( space, current_key );

          if( result )
          {
            try
            {
              nlohmann::json entry;
              // Encode binary key/value as base64 (JSON-safe)
              safe_json_assign( entry, "key", next_key );
              safe_json_assign( entry, "value", std::string( result->data(), result->size() ) );
              entry[ "value_size" ] = result->size();
              
              try
              {
                space_data[ "entries" ].push_back( entry );
              }
              catch( const nlohmann::json::type_error& e )
              {
                // If entry contains invalid UTF-8, create a minimal version
                nlohmann::json safe_entry;
                safe_entry[ "error" ] = "invalid_utf8_in_entry";
                safe_entry[ "key_size" ] = next_key.size();
                safe_entry[ "value_size" ] = result->size();
                try
                {
                  space_data[ "entries" ].push_back( safe_entry );
                }
                catch( ... )
                {
                  // Even the safe entry failed, skip it entirely
                }
              }
              catch( const nlohmann::json::exception& )
              {
                // If entry contains invalid UTF-8, create a minimal version
                nlohmann::json safe_entry;
                safe_entry[ "error" ] = "invalid_utf8_in_entry";
                safe_entry[ "key_size" ] = next_key.size();
                safe_entry[ "value_size" ] = result->size();
                try
                {
                  space_data[ "entries" ].push_back( safe_entry );
                }
                catch( ... )
                {
                  // Even the safe entry failed, skip it entirely
                }
              }
              entry_count++;

              // Move to next key
              current_key = next_key;
            }
            catch( const std::exception& e )
            {
              // Skip this entry if it causes JSON serialization issues
              try
              {
                nlohmann::json error_entry;
                safe_json_assign( error_entry, "error", std::string( "failed_to_serialize: " ) + e.what() );
                error_entry[ "key_size" ] = next_key.size();
                error_entry[ "value_size" ] = result->size();
                space_data[ "entries" ].push_back( error_entry );
              }
              catch( ... )
              {
                nlohmann::json error_entry;
                error_entry[ "error" ] = "failed_to_serialize";
                error_entry[ "key_size" ] = next_key.size();
                error_entry[ "value_size" ] = result->size();
                space_data[ "entries" ].push_back( error_entry );
              }
              entry_count++;
              current_key = next_key;
            }
            catch( ... )
            {
              // Skip this entry if it causes any other issues
              nlohmann::json error_entry;
              error_entry[ "error" ] = "failed_to_serialize_unknown";
              error_entry[ "key_size" ] = next_key.size();
              error_entry[ "value_size" ] = result->size();
              space_data[ "entries" ].push_back( error_entry );
              entry_count++;
              current_key = next_key;
            }
          }
          else
          {
            has_more = false;
          }
        }

        space_data[ "total_entries_found" ] = entry_count;
        space_data[ "truncated" ] = ( entry_count >= max_entries_per_space );
      }
      catch( const std::exception& e )
      {
        try
        {
          safe_json_assign( space_data, "error", e.what() );
        }
        catch( ... )
        {
          space_data[ "error" ] = "unknown_error";
        }
      }
      catch( ... )
      {
        space_data[ "error" ] = "unknown_error";
      }

      // Safely assign space_data to state_data, handling any UTF-8 issues
      try
      {
        state_data[ space_name ] = space_data;
      }
      catch( const nlohmann::json::type_error& )
      {
        // If space_data contains invalid UTF-8, create a minimal version
        nlohmann::json safe_space_data;
        safe_space_data[ "error" ] = "invalid_utf8_in_space_data";
        safe_space_data[ "space_name" ] = space_name;
        if( space_data.contains( "total_entries_found" ) )
        {
          safe_space_data[ "total_entries_found" ] = space_data[ "total_entries_found" ];
        }
        state_data[ space_name ] = safe_space_data;
      }
      catch( const nlohmann::json::exception& )
      {
        // If space_data contains invalid UTF-8, create a minimal version
        nlohmann::json safe_space_data;
        safe_space_data[ "error" ] = "invalid_utf8_in_space_data";
        safe_space_data[ "space_name" ] = space_name;
        if( space_data.contains( "total_entries_found" ) )
        {
          safe_space_data[ "total_entries_found" ] = space_data[ "total_entries_found" ];
        }
        state_data[ space_name ] = safe_space_data;
      }
    }

    // Also collect delta entries if available
    try
    {
      auto delta_entries = node->get_delta_entries();
      nlohmann::json delta_data = nlohmann::json::array();
      for( const auto& entry: delta_entries )
      {
        try
        {
          nlohmann::json delta_entry;
          delta_entry[ "space_system" ] = entry.object_space().system();
          safe_json_assign( delta_entry, "space_zone", entry.object_space().zone() );
          delta_entry[ "space_id" ] = entry.object_space().id();
          safe_json_assign( delta_entry, "key", entry.key() );
          safe_json_assign( delta_entry, "value", entry.value() );
          delta_entry[ "value_size" ] = entry.value().size();
          try
          {
            delta_data.push_back( delta_entry );
          }
          catch( const nlohmann::json::exception& )
          {
            // If delta_entry contains invalid UTF-8, create a minimal version
            nlohmann::json safe_entry;
            safe_entry[ "error" ] = "invalid_utf8_in_delta_entry";
            safe_entry[ "space_system" ] = entry.object_space().system();
            safe_entry[ "space_id" ] = entry.object_space().id();
            safe_entry[ "key_size" ] = entry.key().size();
            safe_entry[ "value_size" ] = entry.value().size();
            delta_data.push_back( safe_entry );
          }
        }
        catch( const std::exception& e )
        {
          // Skip this entry if it causes JSON serialization issues
          try
          {
            nlohmann::json error_entry;
            safe_json_assign( error_entry, "error", std::string( "failed_to_serialize: " ) + e.what() );
            error_entry[ "space_system" ] = entry.object_space().system();
            error_entry[ "space_id" ] = entry.object_space().id();
            error_entry[ "key_size" ] = entry.key().size();
            error_entry[ "value_size" ] = entry.value().size();
            delta_data.push_back( error_entry );
          }
          catch( ... )
          {
            nlohmann::json error_entry;
            error_entry[ "error" ] = "failed_to_serialize";
            error_entry[ "space_system" ] = entry.object_space().system();
            error_entry[ "space_id" ] = entry.object_space().id();
            error_entry[ "key_size" ] = entry.key().size();
            error_entry[ "value_size" ] = entry.value().size();
            delta_data.push_back( error_entry );
          }
        }
        catch( ... )
        {
          // Skip this entry if it causes any other issues
          nlohmann::json error_entry;
          error_entry[ "error" ] = "failed_to_serialize_unknown";
          error_entry[ "space_system" ] = entry.object_space().system();
          error_entry[ "space_id" ] = entry.object_space().id();
          error_entry[ "key_size" ] = entry.key().size();
          error_entry[ "value_size" ] = entry.value().size();
          delta_data.push_back( error_entry );
        }
      }
      state_data[ "delta_entries" ] = delta_data;
      state_data[ "delta_entry_count" ] = delta_entries.size();
    }
    catch( ... )
    {
      state_data[ "delta_entries" ] = "error_retrieving_delta";
    }

    // Query koinos_fund_state for specific space and keys
    try
    {
      nlohmann::json koinos_fund_data = nlohmann::json::object();
      koinos_fund_data[ "entries" ] = nlohmann::json::array();

      // Create the object space: zone="AGODyCuhi5XqbJWB30tP4BYEWmCH6mq2zg==", id=2
      object_space fund_space;
      fund_space.set_zone( util::from_base64< std::string >( "AGODyCuhi5XqbJWB30tP4BYEWmCH6mq2zg==" ) );
      fund_space.set_id( 2 );
      fund_space.set_system( true );

      // Keys to query (base64-encoded)
      std::vector< std::string > fund_keys = {
        "MDIyNDU1Mjk3NDQ5Njk2NDA5OTk5OTY=",
        "MDIyNDU1MzM3MDQzNDQ0MTA5OTk5OTY=",
        "MDIyNDU1MzM4NTkzMjUwODA5OTk5OTY="
      };

      for( const auto& key_b64 : fund_keys )
      {
        try
        {
          std::string key = util::from_base64< std::string >( key_b64 );
          auto result = node->get_object( fund_space, key );

          nlohmann::json entry;
          entry[ "key" ] = key_b64; // Store base64 key as-is (already base64-encoded)
          if( result )
          {
            safe_json_assign( entry, "value", std::string( result->data(), result->size() ) );
            entry[ "value_size" ] = result->size();
            entry[ "found" ] = true;
          }
          else
          {
            entry[ "found" ] = false;
            entry[ "value" ] = nullptr;
          }
          koinos_fund_data[ "entries" ].push_back( entry );
        }
        catch( const std::exception& e )
        {
          nlohmann::json error_entry;
          error_entry[ "key" ] = key_b64; // Store base64 key as-is
          safe_json_assign( error_entry, "error", std::string( "failed_to_query: " ) + e.what() );
          error_entry[ "found" ] = false;
          koinos_fund_data[ "entries" ].push_back( error_entry );
        }
        catch( ... )
        {
          nlohmann::json error_entry;
          error_entry[ "key" ] = key_b64; // Store base64 key as-is
          error_entry[ "error" ] = "failed_to_query_unknown";
          error_entry[ "found" ] = false;
          koinos_fund_data[ "entries" ].push_back( error_entry );
        }
      }

      state_data[ "koinos_fund_state" ] = koinos_fund_data;
    }
    catch( const std::exception& e )
    {
      nlohmann::json error_data;
      error_data[ "error" ] = "failed_to_query_koinos_fund_state";
      try
      {
        safe_json_assign( error_data, "error_message", e.what() );
      }
      catch( ... )
      {
        error_data[ "error_message" ] = "unknown_error";
      }
      state_data[ "koinos_fund_state" ] = error_data;
    }
    catch( ... )
    {
      state_data[ "koinos_fund_state" ] = nlohmann::json::object();
      state_data[ "koinos_fund_state" ][ "error" ] = "unknown_error_querying_koinos_fund_state";
    }

    // All string values are base64-encoded, so no UTF-8 validation needed

    // Validate the entire JSON structure before returning
    try
    {
      // Try to serialize to validate UTF-8
      std::string test_serialize = state_data.dump();
      // If successful, return the validated JSON
      return state_data;
    }
    catch( const nlohmann::json::type_error& e )
    {
      // If validation fails, return a minimal safe version
      nlohmann::json safe_state_data;
      safe_state_data[ "error" ] = "json_validation_failed";
      safe_state_data[ "error_message" ] = "invalid_utf8_in_state_data";
      safe_json_assign( safe_state_data, "validation_error", e.what() );
      return safe_state_data;
    }
    catch( const nlohmann::json::exception& e )
    {
      // If validation fails, return a minimal safe version
      nlohmann::json safe_state_data;
      safe_state_data[ "error" ] = "json_validation_failed";
      safe_state_data[ "error_message" ] = "json_exception_during_validation";
      try
      {
        safe_json_assign( safe_state_data, "validation_error", e.what() );
      }
      catch( ... )
      {
        safe_state_data[ "validation_error" ] = "unknown_error";
      }
      return safe_state_data;
    }
  }
  catch( const std::exception& e )
  {
    nlohmann::json safe_state_data;
    safe_state_data[ "error" ] = "exception_during_collection";
    try
    {
      safe_json_assign( safe_state_data, "error_message", e.what() );
    }
    catch( ... )
    {
      safe_state_data[ "error_message" ] = "unknown_error";
    }
    return safe_state_data;
  }
  catch( ... )
  {
    nlohmann::json safe_state_data;
    safe_state_data[ "error" ] = "unknown_error_creating_context";
    return safe_state_data;
  }
}

void controller_impl::debug_merkle_mismatch( const protocol::block& block,
                                              state_db::state_node_ptr parent_node,
                                              state_db::shared_lock_ptr db_lock )
{
  // #region agent log
  try
  {
    const std::filesystem::path log_path( "/home/julian/koinos/.cursor/debug.log" );
    if( !log_path.parent_path().empty() && !std::filesystem::exists( log_path.parent_path() ) )
    {
      std::filesystem::create_directories( log_path.parent_path() );
    }
    std::ofstream log_file( "/home/julian/koinos/.cursor/debug.log", std::ios::app );
    if( log_file.is_open() )
    {
      log_file << "{\"runId\":\"pre-fix\",\"hypothesisId\":\"H0\",\"location\":\"controller.cpp:debug_merkle_mismatch(raw_entry)\",\"message\":\"Entered debug_merkle_mismatch\",\"data\":{\"height\":"
               << block.header().height()
               << "},\"timestamp\":"
               << std::chrono::duration_cast< std::chrono::milliseconds >( std::chrono::system_clock::now().time_since_epoch() ).count()
               << "}\n";
    }
  }
  catch( ... )
  {}
  // #endregion

  // #region agent log
  auto append_debug_log = []( const std::string& run_id,
                              const std::string& hypothesis_id,
                              const std::string& location,
                              const std::string& message,
                              const nlohmann::json& data ) {
    try
    {
      nlohmann::json log_entry;
      log_entry[ "runId" ] = run_id;
      log_entry[ "hypothesisId" ] = hypothesis_id;
      log_entry[ "location" ] = location;
      log_entry[ "message" ] = message;
      log_entry[ "data" ] = data;
      log_entry[ "timestamp" ] = std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::system_clock::now().time_since_epoch() ).count();

      const std::filesystem::path log_path( "/home/julian/koinos/.cursor/debug.log" );
      if( !log_path.parent_path().empty() && !std::filesystem::exists( log_path.parent_path() ) )
      {
        std::filesystem::create_directories( log_path.parent_path() );
      }

      std::ofstream log_file( "/home/julian/koinos/.cursor/debug.log", std::ios::app );
      if( log_file.is_open() )
      {
        log_file << log_entry.dump() << '\n';
      }
    }
    catch( ... )
    {}
  };
  // #endregion

  // Treat string values as raw bytes and encode as base64 (always JSON-safe ASCII).
  auto safe_base64_assign = []( nlohmann::json& j, const std::string& key, const std::string& value ) {
    try
    {
      j[ key ] = detail::base64_encode( value );
    }
    catch( ... )
    {
      j[ key ] = "base64_error";
    }
  };

  try
  {
    // #region agent log
    {
      nlohmann::json mismatch_input;
      mismatch_input[ "height" ] = block.header().height();
      mismatch_input[ "tx_count" ] = block.transactions_size();
      safe_base64_assign( mismatch_input, "block_id_b64", block.id() );
      safe_base64_assign( mismatch_input, "producer_b64", block.header().signer() );
      safe_base64_assign( mismatch_input, "claimed_prev_merkle_root_b64", block.header().previous_state_merkle_root() );
      safe_base64_assign( mismatch_input, "parent_merkle_root_b64", util::converter::as< std::string >( parent_node->merkle_root() ) );
      append_debug_log( "pre-fix",
                        "H4",
                        "controller.cpp:debug_merkle_mismatch(entry)",
                        "State merkle mismatch detected before assert",
                        mismatch_input );
    }
    // #endregion

    nlohmann::json debug_info;

    // Block being applied
    debug_info[ "block_being_applied" ] = nlohmann::json::object();
    safe_base64_assign( debug_info[ "block_being_applied" ], "id", block.id() );
    debug_info[ "block_being_applied" ][ "height" ] = block.header().height();
    safe_base64_assign( debug_info[ "block_being_applied" ], "previous", block.header().previous() );
    safe_base64_assign( debug_info[ "block_being_applied" ], "previous_state_merkle_root",
                        block.header().previous_state_merkle_root() );
    debug_info[ "block_being_applied" ][ "timestamp" ] = block.header().timestamp();
    safe_base64_assign( debug_info[ "block_being_applied" ], "transaction_merkle_root",
                        block.header().transaction_merkle_root() );
    debug_info[ "block_being_applied" ][ "transaction_count" ] = block.transactions_size();

    // Parent chain (up to 5 blocks)
    debug_info[ "parent_chain" ] = nlohmann::json::array();

    state_db::state_node_ptr current_node = parent_node;
    int parent_count = 0;
    const int max_parents = 5;

    while( current_node && parent_count < max_parents )
    {
      nlohmann::json parent_info;
      try
      {
        safe_base64_assign( parent_info, "id", util::converter::as< std::string >( current_node->id() ) );
        parent_info[ "revision" ] = current_node->revision();
        safe_base64_assign( parent_info, "parent_id", util::converter::as< std::string >( current_node->parent_id() ) );
        safe_base64_assign( parent_info, "merkle_root", util::converter::as< std::string >( current_node->merkle_root() ) );
        parent_info[ "is_finalized" ] = current_node->is_finalized();
      }
      catch( ... )
      {
        parent_info[ "error" ] = "failed_to_serialize_node_info";
        parent_info[ "revision" ] = current_node ? current_node->revision() : 0;
      }

      // Try to get head info from this node
      try
      {
        execution_context parent_ctx( _vm_backend, intent::read_only );
        parent_ctx.push_frame( stack_frame{ .call_privilege = privilege::kernel_mode } );
        parent_ctx.set_state_node( current_node->create_anonymous_node() );
        parent_ctx.reset_cache();
        auto head_info = system_call::get_head_info( parent_ctx );
        try
        {
          parent_info[ "head_info" ] = nlohmann::json::object();
          parent_info[ "head_info" ][ "height" ] = head_info.head_topology().height();
          safe_base64_assign( parent_info[ "head_info" ], "id", head_info.head_topology().id() );
          safe_base64_assign( parent_info[ "head_info" ], "previous", head_info.head_topology().previous() );
          parent_info[ "head_info" ][ "last_irreversible_block" ] = head_info.last_irreversible_block();
          parent_info[ "head_info" ][ "head_block_time" ] = head_info.head_block_time();
        }
        catch( const nlohmann::json::exception& )
        {
          // If head_info contains invalid UTF-8, create a minimal version
          parent_info[ "head_info" ] = nlohmann::json::object();
          parent_info[ "head_info" ][ "error" ] = "invalid_utf8_in_head_info";
          parent_info[ "head_info" ][ "height" ] = head_info.head_topology().height();
        }
      }
      catch( ... )
      {
        parent_info[ "head_info" ] = "error_retrieving_head_info";
      }

      // Collect state data for parent node (only for the immediate parent to avoid huge files)
      if( parent_count == 0 )
      {
        try
        {
          auto state_data_result = collect_state_data( current_node, 1000 );
          try
          {
            // Try to serialize to string first to validate UTF-8
            std::string test_serialize = state_data_result.dump();
            // If that succeeds, assign it
            parent_info[ "state" ] = state_data_result;
          }
          catch( const nlohmann::json::exception& e )
          {
            // If state data contains invalid UTF-8, just mark it as an error
            parent_info[ "state" ] = "error_serializing_state_data";
            try
            {
              safe_base64_assign( parent_info, "state_error", std::string( "contains_invalid_utf8: " ) + e.what() );
            }
            catch( ... )
            {
              parent_info[ "state_error" ] = "contains_invalid_utf8";
            }
          }
        }
        catch( ... )
        {
          parent_info[ "state" ] = "error_collecting_state";
        }
      }

      try
      {
        debug_info[ "parent_chain" ].push_back( parent_info );
      }
      catch( const nlohmann::json::exception& e )
      {
        // If parent_info contains invalid UTF-8, create a minimal version
        nlohmann::json safe_parent_info;
        safe_parent_info[ "error" ] = "failed_to_serialize_parent_info";
        safe_parent_info[ "revision" ] = current_node ? current_node->revision() : 0;
        try
        {
          safe_parent_info[ "id" ] = util::to_hex( util::converter::as< std::string >( current_node->id() ) );
        }
        catch( ... )
        {
          safe_parent_info[ "id" ] = "error";
        }
        debug_info[ "parent_chain" ].push_back( safe_parent_info );
      }

      // Move to parent
      if( !current_node->parent_id().is_zero() )
      {
        current_node = _db.get_node( current_node->parent_id(), db_lock );
      }
      else
      {
        current_node.reset();
      }
      parent_count++;
    }

    // Current head information
    try
    {
      auto head = _db.get_head( db_lock );
      debug_info[ "current_head" ] = nlohmann::json::object();
      safe_base64_assign( debug_info[ "current_head" ], "id", util::converter::as< std::string >( head->id() ) );
      debug_info[ "current_head" ][ "revision" ] = head->revision();
      safe_base64_assign( debug_info[ "current_head" ], "parent_id", util::converter::as< std::string >( head->parent_id() ) );
      safe_base64_assign( debug_info[ "current_head" ], "merkle_root", util::converter::as< std::string >( head->merkle_root() ) );
      debug_info[ "current_head" ][ "is_finalized" ] = head->is_finalized();
    }
    catch( ... )
    {
      debug_info[ "current_head" ] = "error_retrieving_head";
    }

    // Root/LIB information
    try
    {
      auto root = _db.get_root( db_lock );
      debug_info[ "root_lib" ] = nlohmann::json::object();
      safe_base64_assign( debug_info[ "root_lib" ], "id", util::converter::as< std::string >( root->id() ) );
      debug_info[ "root_lib" ][ "revision" ] = root->revision();
      safe_base64_assign( debug_info[ "root_lib" ], "parent_id", util::converter::as< std::string >( root->parent_id() ) );
      safe_base64_assign( debug_info[ "root_lib" ], "merkle_root", util::converter::as< std::string >( root->merkle_root() ) );
    }
    catch( ... )
    {
      debug_info[ "root_lib" ] = "error_retrieving_root";
    }

    // Mismatch details
    debug_info[ "mismatch_details" ] = nlohmann::json::object();
    safe_base64_assign( debug_info[ "mismatch_details" ], "block_claims_previous_state_merkle_root",
                     block.header().previous_state_merkle_root() );
    safe_base64_assign( debug_info[ "mismatch_details" ], "parent_node_actual_merkle_root",
                     util::converter::as< std::string >( parent_node->merkle_root() ) );
    debug_info[ "mismatch_details" ][ "match" ] =
        ( block.header().previous_state_merkle_root()
          == util::converter::as< std::string >( parent_node->merkle_root() ) );

    // #region agent log
    try
    {
      nlohmann::json fund_votes_keys;
      fund_votes_keys[ "space" ] = "fund.active_projects_by_votes";
      fund_votes_keys[ "entries" ] = nlohmann::json::array();
      object_space fund_space;
      fund_space.set_zone( util::from_base64< std::string >( "AGODyCuhi5XqbJWB30tP4BYEWmCH6mq2zg==" ) );
      fund_space.set_id( 2 );
      fund_space.set_system( true );

      std::vector< std::string > key_b64s = {
        "MDIyNDU1Mjk3NDQ5Njk2NDA5OTk5OTY=",
        "MDIyNDU1MzM3MDQzNDQ0MTA5OTk5OTY=",
        "MDIyNDU1MzM4NTkzMjUwODA5OTk5OTY="
      };

      for( const auto& key_b64 : key_b64s )
      {
        nlohmann::json key_entry;
        key_entry[ "key_b64" ] = key_b64;
        auto key = util::from_base64< std::string >( key_b64 );
        auto value = parent_node->get_object( fund_space, key );
        key_entry[ "found" ] = !!value;
        if( value )
        {
          key_entry[ "value_size" ] = value->size();
        }
        fund_votes_keys[ "entries" ].push_back( key_entry );
      }

      append_debug_log( "pre-fix",
                        "H2",
                        "controller.cpp:debug_merkle_mismatch(fund_votes_space)",
                        "Fund active_projects_by_votes key existence snapshot",
                        fund_votes_keys );
    }
    catch( ... )
    {}
    // #endregion

    // Save to file in log directory if available, otherwise current directory
    std::filesystem::path filepath;
    std::string filename;
    try
    {
      filename = "merkle_mismatch_debug_" + util::to_hex( block.id() ) + ".json";
    }
    catch( ... )
    {
      filename = "merkle_mismatch_debug_error.json";
    }

    if( !_log_directory.empty() && std::filesystem::exists( _log_directory ) )
    {
      filepath = _log_directory / filename;
    }
    else
    {
      filepath = filename;
    }

    // Ensure directory exists
    if( !filepath.parent_path().empty() && !std::filesystem::exists( filepath.parent_path() ) )
    {
      std::filesystem::create_directories( filepath.parent_path() );
    }

    std::ofstream file( filepath );
    if( file.is_open() )
    {
      file << debug_info.dump( 2 );
      file.close();
      LOG( error ) << "Merkle mismatch debug info saved to: " << filepath.string();
    }
    else
    {
      LOG( error ) << "Failed to save debug info to file: " << filepath.string();
      LOG( error ) << "Debug info JSON: " << debug_info.dump( 2 );
    }
  }
  catch( const nlohmann::json::exception& e )
  {
    LOG( error ) << "Error generating debug info (JSON exception): " << e.what();
    // Try to save a minimal debug file
    try
    {
      nlohmann::json minimal_info;
      minimal_info[ "error" ] = "json_serialization_failed";
      minimal_info[ "error_message" ] = e.what();
      minimal_info[ "block_id" ] = "0x" + util::to_hex( block.id() );
      minimal_info[ "block_height" ] = block.header().height();
        std::filesystem::path error_filepath;
        std::string error_filename = "merkle_mismatch_debug_" + util::to_hex( block.id() ) + "_error.json";
        if( !_log_directory.empty() && std::filesystem::exists( _log_directory ) )
        {
          error_filepath = _log_directory / error_filename;
        }
        else
        {
          error_filepath = error_filename;
        }
        if( !error_filepath.parent_path().empty() && !std::filesystem::exists( error_filepath.parent_path() ) )
        {
          std::filesystem::create_directories( error_filepath.parent_path() );
        }
        std::ofstream file( error_filepath );
        if( file.is_open() )
        {
          file << minimal_info.dump( 2 );
          file.close();
          LOG( error ) << "Minimal debug info saved to: " << error_filepath.string();
        }
    }
    catch( ... )
    {
      LOG( error ) << "Failed to save minimal debug info";
    }
  }
  catch( const std::exception& e )
  {
    LOG( error ) << "Error generating debug info: " << e.what();
  }
}

apply_block_result controller_impl::apply_block( const protocol::block& block, const apply_block_options& opts )
{
  validate_block( block );

  apply_block_result res;

  static constexpr uint64_t index_message_interval = 1'000;
  static constexpr std::chrono::seconds time_delta = std::chrono::seconds( 5 );
  static constexpr std::chrono::seconds live_delta = std::chrono::seconds( 60 );

  auto time_lower_bound = uint64_t( 0 );
  auto time_upper_bound =
    std::chrono::duration_cast< std::chrono::milliseconds >( ( opts.application_time + time_delta ).time_since_epoch() )
      .count();
  uint64_t parent_height = 0;

  auto db_lock = _db.get_shared_lock();

  auto block_id     = util::converter::to< crypto::multihash >( block.id() );
  auto block_height = block.header().height();
  auto parent_id    = util::converter::to< crypto::multihash >( block.header().previous() );
  auto block_node   = _db.get_node( block_id, db_lock );
  auto parent_node  = _db.get_node( parent_id, db_lock );

  bool new_head = false;

  if( block_node )
    return {}; // Block has been applied

  // This prevents returning "unknown previous block" when the pushed block is the LIB
  if( !parent_node )
  {
    auto root = _db.get_root( db_lock );
    KOINOS_ASSERT( block_height >= root->revision(),
                   pre_irreversibility_block_exception,
                   "block is prior to irreversibility" );
    KOINOS_ASSERT( block_id == root->id(), unknown_previous_block_exception, "unknown previous block" );
    return {}; // Block is current LIB
  }
  else
  {
    KOINOS_ASSERT( parent_node->is_finalized(), unknown_previous_block_exception, "unknown previous block" );
  }

  bool live = block.header().timestamp() > std::chrono::duration_cast< std::chrono::milliseconds >(
                                             ( opts.application_time - live_delta ).time_since_epoch() )
                                             .count();

  if( !opts.index_to && live )
  {
    LOG( debug ) << "Pushing block - Height: " << block_height << ", ID: " << block_id;
  }

  block_node = _db.create_writable_node( parent_id, block_id, block.header(), db_lock );

  // If this is not the genesis case, we must ensure that the proposed block timestamp is greater
  // than the parent block timestamp.
  if( block_node && !parent_id.is_zero() )
  {
    execution_context parent_ctx( _vm_backend, intent::read_only );

    parent_ctx.push_frame( stack_frame{ .call_privilege = privilege::kernel_mode } );

    parent_ctx.set_state_node( parent_node );
    parent_ctx.reset_cache();
    auto head_info   = system_call::get_head_info( parent_ctx );
    parent_height    = head_info.head_topology().height();
    time_lower_bound = head_info.head_block_time();
  }

  execution_context ctx( _vm_backend, opts.propose_block ? intent::block_proposal : intent::block_application );

  try
  {
    // Genesis case, when the first block is submitted the previous must be the zero hash
    if( parent_id.is_zero() )
    {
      KOINOS_ASSERT( block_height == 1, unexpected_height_exception, "first block must have height of 1" );
    }

    KOINOS_ASSERT( block_node, block_state_error_exception, "could not create new block state node" );

    KOINOS_ASSERT( block_height == parent_height + 1,
                   unexpected_height_exception,
                   "expected block height of ${a}, was ${b}",
                   ( "a", parent_height + 1 )( "b", block_height ) );

    KOINOS_ASSERT( block.header().timestamp() <= time_upper_bound,
                   timestamp_out_of_bounds_exception,
                   "block timestamp is too far in the future" );
    KOINOS_ASSERT( block.header().timestamp() > time_lower_bound,
                   timestamp_out_of_bounds_exception,
                   "block timestamp is too old" );

    // Debug: Save block and parent information before checking merkle root
    if( block.header().previous_state_merkle_root()
        != util::converter::as< std::string >( parent_node->merkle_root() ) )
    {
      debug_merkle_mismatch( block, parent_node, db_lock );
    }

    KOINOS_ASSERT( block.header().previous_state_merkle_root()
                     == util::converter::as< std::string >( parent_node->merkle_root() ),
                   state_merkle_mismatch_exception,
                   "block previous state merkle mismatch" );

    ctx.push_frame( stack_frame{ .call_privilege = privilege::kernel_mode } );

    ctx.set_state_node( block_node );
    ctx.reset_cache();

    system_call::apply_block( ctx, block );

    res.failed_transaction_indices = ctx.get_failed_transaction_indices();

    if( opts.propose_block && res.failed_transaction_indices.size() )
    {
      // Icky, but the transaction failure code is in a catch block
      // so use the current flow of control

      KOINOS_THROW( failure_exception,
                    "${n} transactions failed in the block",
                    ( "n", res.failed_transaction_indices.size() ) );
    }

    KOINOS_ASSERT( std::holds_alternative< protocol::block_receipt >( ctx.receipt() ),
                   unexpected_receipt_exception,
                   "expected block receipt" );
    res.receipt = std::get< protocol::block_receipt >( ctx.receipt() );

    maybe_rectify_state( ctx, block, *res.receipt );

    if( _client )
    {
      rpc::block_store::block_store_request req;
      req.mutable_add_block()->mutable_block_to_add()->CopyFrom( block );
      req.mutable_add_block()->mutable_receipt_to_add()->CopyFrom(
        std::get< protocol::block_receipt >( ctx.receipt() ) );

      auto future = _client->rpc( util::service::block_store,
                                  util::converter::as< std::string >( req ),
                                  1'500ms,
                                  mq::retry_policy::none );

      rpc::block_store::block_store_response resp;
      resp.ParseFromString( future.get() );

      KOINOS_ASSERT( !resp.has_error(),
                     rpc_failure_exception,
                     "received error from block store: ${e}",
                     ( "e", resp.error() ) );
      KOINOS_ASSERT( resp.has_add_block(),
                     rpc_failure_exception,
                     "unexpected response when submitting block: ${r}",
                     ( "r", resp ) );
    }

    if( !opts.index_to && live )
    {
      auto num_transactions = block.transactions_size();

      LOG( info ) << "Block applied - Height: " << block_height << ", ID: " << block_id << " (" << num_transactions
                  << ( num_transactions == 1 ? " transaction)" : " transactions)" );
    }
    else if( block_height % index_message_interval == 0 )
    {
      if( opts.index_to )
      {
        auto progress = block_height / static_cast< double >( opts.index_to ) * 100;
        LOG( info ) << "Indexing chain (" << progress << "%) - Height: " << block_height << ", ID: " << block_id;
      }
      else
      {
        auto to_go =
          std::chrono::duration_cast< std::chrono::seconds >(
            opts.application_time.time_since_epoch() - std::chrono::milliseconds( block.header().timestamp() ) )
            .count();
        LOG( info ) << "Sync progress - Height: " << block_height << ", ID: " << block_id << " ("
                    << format_time( to_go ) << " block time remaining)";
      }
    }

    auto lib = system_call::get_last_irreversible_block( ctx );

    try
    {
      // We need to finalize our node, checking if it is the new head block, update the cached head block,
      // and advancing LIB as an atomic action or else we risk _db.get_head(), _cached_head_block, and
      // LIB desyncing from each other
      db_lock.reset();
      block_node.reset();
      parent_node.reset();
      ctx.clear_state_node();

      auto unique_db_lock = _db.get_unique_lock();
      _db.finalize_node( block_id, unique_db_lock );

      res.receipt->set_state_merkle_root(
        util::converter::as< std::string >( _db.get_node( block_id, unique_db_lock )->merkle_root() ) );

      if( block_id == _db.get_head( unique_db_lock )->id() )
      {
        std::unique_lock< std::shared_mutex > head_lock( _cached_head_block_mutex );
        new_head           = true;
        _cached_head_block = std::make_shared< protocol::block >( block );
      }

      if( lib > _db.get_root( unique_db_lock )->revision() )
      {
        auto lib_id = _db.get_node_at_revision( lib, block_id, unique_db_lock )->id();
        _db.commit_node( lib_id, unique_db_lock );
      }

      unique_db_lock.reset();
      db_lock    = _db.get_shared_lock();
      block_node = _db.get_node( block_id, db_lock );
      ctx.set_state_node( block_node );

    }
    catch( ... )
    {
      // If any exception is thrown, reset to the expected local state and then rethrow.
      db_lock    = _db.get_shared_lock();
      block_node = _db.get_node( block_id, db_lock );
      ctx.set_state_node( block_node );
      throw;
    }

    // It is NOT safe to use block_node after this point without checking it against null

    if( _client )
    {
      const auto [ fork_heads, last_irreversible_block ] = get_fork_data( db_lock );

      broadcast::block_irreversible bc;
      bc.mutable_topology()->CopyFrom( last_irreversible_block );

      _client->broadcast( "koinos.block.irreversible", util::converter::as< std::string >( bc ) );

      broadcast::block_accepted ba;
      *ba.mutable_block()   = block;
      *ba.mutable_receipt() = std::get< protocol::block_receipt >( ctx.receipt() );
      ba.set_live( live );
      ba.set_head( new_head );

      _client->broadcast( "koinos.block.accept", util::converter::as< std::string >( ba ) );

      broadcast::fork_heads fh;
      fh.set_allocated_last_irreversible_block( bc.release_topology() );

      for( const auto& fork_head: fork_heads )
      {
        auto* head = fh.add_heads();
        *head      = fork_head;
      }

      _client->broadcast( "koinos.block.forks", util::converter::as< std::string >( fh ) );

      for( const auto& [ transaction_id, event ]: ctx.chronicler().events() )
      {
        broadcast::event_parcel ep;
        ep.set_block_id( block.id() );
        ep.set_height( block.header().height() );
        *ep.mutable_event() = event;

        if( transaction_id )
          ep.set_transaction_id( *transaction_id );

        _client->broadcast( "koinos.event." + util::to_base58( event.source() ) + "." + event.name(),
                            ep.SerializeAsString() );
      }
    }
  }
  catch( const block_state_error_exception& e )
  {
    LOG( warning ) << "Block application failed - Height: " << block_height << " ID: " << block_id
                   << ", with reason: " << e.what();
    throw;
  }
  catch( koinos::exception& e )
  {
    if( block_node && !block_node->is_finalized() )
    {
      _db.discard_node( block_node->id(), db_lock );
      LOG( warning ) << "Block application failed - Height: " << block_height << " ID: " << block_id
                     << ", with reason: " << e.what();
    }
    else
    {
      LOG( error ) << "Block application failed after finalization - Height: " << block_height << " ID: " << block_id
                   << ", with reason: " << e.what();
    }

    if( std::holds_alternative< protocol::block_receipt >( ctx.receipt() ) )
      e.add_json( "logs", std::get< protocol::block_receipt >( ctx.receipt() ).logs() );

    if( opts.propose_block && res.failed_transaction_indices.size() )
    {
      if( _client )
      {
        broadcast::transaction_failed trx_failed;

        for( auto i: res.failed_transaction_indices )
        {
          trx_failed.set_id( block.transactions( i ).id() );
          _client->broadcast( "koinos.transaction.fail", util::converter::as< std::string >( trx_failed ) );
        }
      }

      return res;
    }
    else if( _client )
    {
      const auto& exception_data = e.get_json();

      if( exception_data.count( "transaction_id" ) )
      {
        broadcast::transaction_failed ptf;
        ptf.set_id( util::from_hex< std::string >( exception_data[ "transaction_id" ] ) );
        _client->broadcast( "koinos.transaction.fail", util::converter::as< std::string >( ptf ) );
      }
    }

    throw;
  }
  catch( ... )
  {
    if( block_node && !block_node->is_finalized() )
    {
      _db.discard_node( block_node->id(), db_lock );
      LOG( warning ) << "Block application failed - Height: " << block_height << ", ID: " << block_id
                     << ", for an unknown reason";
    }
    else
    {
      LOG( error ) << "Block application failed after finalization - Height: " << block_height << ", ID: " << block_id
                   << ", for an unknown reason";
    }

    throw;
  }

  return res;
}

void controller_impl::apply_block_delta( const protocol::block& block,
                                         const protocol::block_receipt& receipt,
                                         uint64_t index_to )
{
  // #region agent log
  auto append_debug_log = []( const std::string& run_id,
                              const std::string& hypothesis_id,
                              const std::string& location,
                              const std::string& message,
                              const nlohmann::json& data ) {
    try
    {
      nlohmann::json log_entry;
      log_entry[ "runId" ] = run_id;
      log_entry[ "hypothesisId" ] = hypothesis_id;
      log_entry[ "location" ] = location;
      log_entry[ "message" ] = message;
      log_entry[ "data" ] = data;
      log_entry[ "timestamp" ] = std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::system_clock::now().time_since_epoch() ).count();

      const std::filesystem::path log_path( "/home/julian/koinos/.cursor/debug.log" );
      if( !log_path.parent_path().empty() && !std::filesystem::exists( log_path.parent_path() ) )
      {
        std::filesystem::create_directories( log_path.parent_path() );
      }

      std::ofstream log_file( "/home/julian/koinos/.cursor/debug.log", std::ios::app );
      if( log_file.is_open() )
      {
        log_file << log_entry.dump() << '\n';
      }
    }
    catch( ... )
    {}
  };
  // #endregion

  uint64_t index_message_interval                  = std::max( 10'000ull, index_to / 1'000ull );
  static constexpr std::chrono::seconds time_delta = std::chrono::seconds( 5 );

  auto db_lock = _db.get_shared_lock();

  auto block_id     = util::converter::to< crypto::multihash >( block.id() );
  auto block_height = block.header().height();
  auto parent_id    = util::converter::to< crypto::multihash >( block.header().previous() );
  auto block_node   = _db.get_node( block_id, db_lock );
  auto parent_node  = _db.get_node( parent_id, db_lock );

  if( block_node )
  {
    block_node.reset();
    _db.discard_node( block_id, db_lock );
  }

  // This prevents returning "unknown previous block" when the pushed block is the LIB
  if( !parent_node )
  {
    auto root = _db.get_root( db_lock );
    KOINOS_ASSERT( block_height >= root->revision(),
                   pre_irreversibility_block_exception,
                   "block is prior to irreversibility" );
    KOINOS_ASSERT( block_id == root->id(), unknown_previous_block_exception, "unknown previous block" );
    return; // Block is current LIB
  }

  block_node = _db.create_writable_node( parent_id, block_id, block.header(), db_lock );

  execution_context ctx( _vm_backend, intent::block_application );

  try
  {
    ctx.push_frame( stack_frame{ .call_privilege = privilege::kernel_mode } );

    ctx.set_state_node( block_node );
    ctx.reset_cache();

    for( const auto& delta_entry: receipt.state_delta_entries() )
    {
      // #region agent log
      if( block_height == 32983500
          && delta_entry.object_space().system()
          && delta_entry.object_space().id() == 2
          && delta_entry.object_space().zone()
               == util::from_base64< std::string >( "AGODyCuhi5XqbJWB30tP4BYEWmCH6mq2zg==" ) )
      {
        nlohmann::json replay_entry;
        replay_entry[ "height" ] = block_height;
        replay_entry[ "has_value" ] = delta_entry.has_value();
        replay_entry[ "value_size" ] = delta_entry.value().size();
        replay_entry[ "key_b64" ] = detail::base64_encode( delta_entry.key() );
        append_debug_log( "pre-fix",
                          "H5",
                          "controller.cpp:apply_block_delta(state_delta_loop)",
                          "Replaying fund delta entry from receipt",
                          replay_entry );
      }
      // #endregion

      chain::object_space object_space;
      object_space.set_system( delta_entry.object_space().system() );
      object_space.set_zone( delta_entry.object_space().zone() );
      object_space.set_id( delta_entry.object_space().id() );

      if( delta_entry.has_value() )
        block_node->put_object( object_space, delta_entry.key(), &delta_entry.value() );
      else
        block_node->remove_object( object_space, delta_entry.key() );
    }

    if( block_height % index_message_interval == 0 )
    {
      auto progress = block_height / static_cast< double >( index_to ) * 100;
      LOG( info ) << "Indexing chain (" << progress << "%) - Height: " << block_height << ", ID: " << block_id;
    }

    auto lib = system_call::get_last_irreversible_block( ctx );

    try
    {
      // We need to finalize our node, checking if it is the new head block, update the cached head block,
      // and advancing LIB as an atomic action or else we risk _db.get_head(), _cached_head_block, and
      // LIB desyncing from each other
      db_lock.reset();
      block_node.reset();
      parent_node.reset();
      ctx.clear_state_node();

      auto unique_db_lock = _db.get_unique_lock();
      _db.finalize_node( block_id, unique_db_lock );

      if( block_id == _db.get_head( unique_db_lock )->id() )
      {
        std::unique_lock< std::shared_mutex > head_lock( _cached_head_block_mutex );
        _cached_head_block = std::make_shared< protocol::block >( block );
      }

      if( lib > _db.get_root( unique_db_lock )->revision() )
      {
        auto lib_id = _db.get_node_at_revision( lib, block_id, unique_db_lock )->id();
        _db.commit_node( lib_id, unique_db_lock );
      }

      unique_db_lock.reset();
      db_lock    = _db.get_shared_lock();
      block_node = _db.get_node( block_id, db_lock );
      ctx.set_state_node( block_node );

      // #region agent log
      if( block_height == index_to && block_node )
      {
        nlohmann::json replay_root_check;
        replay_root_check[ "height" ] = block_height;
        replay_root_check[ "index_to" ] = index_to;
        replay_root_check[ "state_delta_count" ] = receipt.state_delta_entries_size();
        replay_root_check[ "receipt_state_merkle_root_b64" ] = detail::base64_encode( receipt.state_merkle_root() );
        replay_root_check[ "finalized_node_merkle_root_b64" ] =
          detail::base64_encode( util::converter::as< std::string >( block_node->merkle_root() ) );
        replay_root_check[ "merkle_match" ] =
          ( receipt.state_merkle_root() == util::converter::as< std::string >( block_node->merkle_root() ) );
        append_debug_log( "pre-fix",
                          "H6",
                          "controller.cpp:apply_block_delta(finalized_merkle_check)",
                          "Compare receipt merkle root with finalized node merkle root",
                          replay_root_check );
      }
      // #endregion
    }
    catch( ... )
    {
      // If any exception is thrown, reset to the expected local state and then rethrow.
      db_lock    = _db.get_shared_lock();
      block_node = _db.get_node( block_id, db_lock );
      ctx.set_state_node( block_node );
      throw;
    }

    // It is NOT safe to use block_node after this point without checking it against null
  }
  catch( const block_state_error_exception& e )
  {
    LOG( warning ) << "Block application failed - Height: " << block_height << " ID: " << block_id
                   << ", with reason: " << e.what();
    throw;
  }
  catch( koinos::exception& e )
  {
    if( block_node && !block_node->is_finalized() )
    {
      _db.discard_node( block_node->id(), db_lock );
      LOG( warning ) << "Block application failed - Height: " << block_height << " ID: " << block_id
                     << ", with reason: " << e.what();
    }
    else
    {
      LOG( error ) << "Block application failed after finalization - Height: " << block_height << " ID: " << block_id
                   << ", with reason: " << e.what();
    }

    if( std::holds_alternative< protocol::block_receipt >( ctx.receipt() ) )
      e.add_json( "logs", std::get< protocol::block_receipt >( ctx.receipt() ).logs() );

    throw;
  }
  catch( ... )
  {
    if( block_node && !block_node->is_finalized() )
    {
      _db.discard_node( block_node->id(), db_lock );
      LOG( warning ) << "Block application failed - Height: " << block_height << ", ID: " << block_id
                     << ", for an unknown reason";
    }
    else
    {
      LOG( error ) << "Block application failed after finalization - Height: " << block_height << ", ID: " << block_id
                   << ", for an unknown reason";
    }

    throw;
  }
}

rpc::chain::submit_transaction_response
controller_impl::submit_transaction( const rpc::chain::submit_transaction_request& request )
{
  validate_transaction( request.transaction() );

  rpc::chain::submit_transaction_response resp;

  std::string payer, payee, nonce;
  uint64_t max_payer_rc;
  uint64_t trx_rc_limit;
  chain::value_type mempool_nonce;

  auto transaction    = request.transaction();
  auto transaction_id = util::to_hex( transaction.id() );

  LOG( debug ) << "Pushing transaction - ID: " << transaction_id;

  auto db_lock = _db.get_shared_lock();
  state_node_ptr head;
  execution_context ctx( _vm_backend, intent::transaction_application );
  std::shared_ptr< const protocol::block > head_block_ptr;

  {
    std::shared_lock< std::shared_mutex > head_lock( _cached_head_block_mutex );
    head_block_ptr = _cached_head_block;
    KOINOS_ASSERT( head_block_ptr, internal_error_exception, "error retrieving head block" );

    head = _db.get_head( db_lock );
  }

  ctx.set_block( *head_block_ptr );
  ctx.set_state_node( head->create_anonymous_node() );

  ctx.push_frame( stack_frame{ .call_privilege = privilege::kernel_mode } );

  try
  {
    ctx.reset_cache();

    payer        = transaction.header().payer();
    payee        = transaction.header().payee();
    nonce        = transaction.header().nonce();
    max_payer_rc = system_call::get_account_rc( ctx, payer );
    trx_rc_limit = transaction.header().rc_limit();

    if( request.broadcast() && _client )
    {
      rpc::mempool::mempool_request req1, req2, req3, req4;
      auto* check_pending = req1.mutable_check_pending_account_resources();

      check_pending->set_payer( payer );
      check_pending->set_max_payer_rc( max_payer_rc );
      check_pending->set_rc_limit( trx_rc_limit );

      auto* check_nonce = req2.mutable_check_account_nonce();

      check_nonce->set_payee( payee.empty() ? payer : payee );
      check_nonce->set_nonce( nonce );

      auto* pending_nonce = req3.mutable_get_pending_nonce();

      pending_nonce->set_payee( payee.empty() ? payer : payee );

      if( _pending_transaction_limit )
      {
        auto* pending_transaction_count = req4.mutable_get_pending_transaction_count();
        pending_transaction_count->set_payee( payee.empty() ? payer : payee );
      }

      auto future1 = _client->rpc( util::service::mempool,
                                   util::converter::as< std::string >( req1 ),
                                   750ms,
                                   mq::retry_policy::none );

      auto future2 = _client->rpc( util::service::mempool,
                                   util::converter::as< std::string >( req2 ),
                                   750ms,
                                   mq::retry_policy::none );

      auto future3 = _client->rpc( util::service::mempool,
                                   util::converter::as< std::string >( req3 ),
                                   750ms,
                                   mq::retry_policy::none );

      std::shared_future< std::string > future4;

      if( _pending_transaction_limit )
      {
        future4 = _client->rpc( util::service::mempool,
                                util::converter::as< std::string >( req4 ),
                                750ms,
                                mq::retry_policy::none );
      }

      rpc::mempool::mempool_response resp;
      resp.ParseFromString( future1.get() );

      KOINOS_ASSERT( !resp.has_error(),
                     rpc_failure_exception,
                     "received error from mempool: ${e}",
                     ( "e", resp.error() ) );
      KOINOS_ASSERT( resp.has_check_pending_account_resources(),
                     rpc_failure_exception,
                     "received unexpected response from mempool" );
      KOINOS_ASSERT( resp.check_pending_account_resources().success(),
                     insufficient_rc_exception,
                     "insufficient pending account resources" );

      resp.ParseFromString( future2.get() );

      KOINOS_ASSERT( !resp.has_error(),
                     rpc_failure_exception,
                     "received error from mempool: ${e}",
                     ( "e", resp.error() ) );
      KOINOS_ASSERT( resp.has_check_account_nonce(),
                     rpc_failure_exception,
                     "received unexpected response from mempool" );
      KOINOS_ASSERT( resp.check_account_nonce().success(), invalid_nonce_exception, "invalid account nonce" );

      resp.ParseFromString( future3.get() );
      KOINOS_ASSERT( !resp.has_error(),
                     rpc_failure_exception,
                     "received error from mempool: ${e}",
                     ( "e", resp.error() ) );
      KOINOS_ASSERT( resp.has_get_pending_nonce(), rpc_failure_exception, "received unexpected response from mempool" );
      mempool_nonce = util::converter::to< chain::value_type >( resp.get_pending_nonce().nonce() );

      if( mempool_nonce.has_uint64_value() )
        ctx.set_mempool_nonce( mempool_nonce );

      if( _pending_transaction_limit )
      {
        resp.ParseFromString( future4.get() );
        KOINOS_ASSERT( !resp.has_error(),
                       rpc_failure_exception,
                       "received error from mempool: ${e}",
                       ( "e", resp.error() ) );
        KOINOS_ASSERT( resp.has_get_pending_transaction_count(),
                       rpc_failure_exception,
                       "received unexpected response from mempool" );
        KOINOS_ASSERT( resp.get_pending_transaction_count().count() < _pending_transaction_limit,
                       pending_transaction_limit_exceeded_exception,
                       "pending transaction limit exceeded" );
      }
    }

    ctx.resource_meter().set_resource_limit_data( system_call::get_resource_limits( ctx ) );
    system_call::apply_transaction( ctx, transaction );

    LOG( debug ) << "Transaction applied - ID: " << transaction_id;

    KOINOS_ASSERT( std::holds_alternative< protocol::transaction_receipt >( ctx.receipt() ),
                   unexpected_receipt_exception,
                   "expected transaction receipt" );
    *resp.mutable_receipt() = std::get< protocol::transaction_receipt >( ctx.receipt() );

    if( request.broadcast() && _client )
    {
      broadcast::transaction_accepted ta;
      *ta.mutable_transaction() = transaction;
      *ta.mutable_receipt()     = std::get< protocol::transaction_receipt >( ctx.receipt() );
      ta.set_height( ctx.get_state_node()->revision() );
      ta.set_system_disk_storage_used( ctx.resource_meter().disk_storage_used() - ta.receipt().disk_storage_used() );
      ta.set_system_network_bandwidth_used( ctx.resource_meter().network_bandwidth_used()
                                            - ta.receipt().network_bandwidth_used() );
      ta.set_system_compute_bandwidth_used( ctx.resource_meter().compute_bandwidth_used()
                                            - ta.receipt().compute_bandwidth_used() );

      _client->broadcast( "koinos.transaction.accept", util::converter::as< std::string >( ta ) );
    }
  }
  catch( koinos::exception& e )
  {
    LOG( debug ) << "Transaction application failed - ID: " << transaction_id << ", with reason: " << e.what();

    if( std::holds_alternative< protocol::transaction_receipt >( ctx.receipt() ) )
      e.add_json( "logs", std::get< protocol::transaction_receipt >( ctx.receipt() ).logs() );

    throw;
  }
  catch( ... )
  {
    LOG( debug ) << "Transaction application failed - ID: " << transaction_id << ", for an unknown reason";
    throw;
  }

  return resp;
}

rpc::chain::get_head_info_response controller_impl::get_head_info( const rpc::chain::get_head_info_request& )
{
  execution_context ctx( _vm_backend );
  ctx.push_frame( stack_frame{ .call_privilege = privilege::kernel_mode } );

  auto db_lock = _db.get_shared_lock();
  state_node_ptr head;
  std::shared_ptr< const protocol::block > head_block_ptr;

  {
    std::shared_lock< std::shared_mutex > head_lock( _cached_head_block_mutex );
    head_block_ptr = _cached_head_block;

    KOINOS_ASSERT( head_block_ptr, internal_error_exception, "error retrieving head block" );

    head = _db.get_head( db_lock );
  }

  ctx.set_state_node( head->create_anonymous_node() );

  KOINOS_ASSERT( head_block_ptr, internal_error_exception, "error retrieving head block" );

  ctx.set_block( *head_block_ptr );
  ctx.reset_cache();

  auto head_info      = system_call::get_head_info( ctx );
  block_topology topo = head_info.head_topology();

  rpc::chain::get_head_info_response resp;
  *resp.mutable_head_topology() = topo;
  resp.set_last_irreversible_block( head_info.last_irreversible_block() );
  resp.set_head_state_merkle_root( util::converter::as< std::string >( head->merkle_root() ) );
  resp.set_head_block_time( head_info.head_block_time() );

  return resp;
}

rpc::chain::get_chain_id_response controller_impl::get_chain_id( const rpc::chain::get_chain_id_request& )
{
  execution_context ctx( _vm_backend );
  ctx.push_frame( stack_frame{ .call_privilege = privilege::kernel_mode } );

  ctx.set_state_node( _db.get_head( _db.get_shared_lock() )->create_anonymous_node() );
  ctx.reset_cache();

  rpc::chain::get_chain_id_response resp;
  resp.set_chain_id( system_call::get_chain_id( ctx ) );

  return resp;
}

fork_data controller_impl::get_fork_data( state_db::shared_lock_ptr db_lock )
{
  fork_data fdata;
  execution_context ctx( _vm_backend );

  ctx.push_frame( koinos::chain::stack_frame{ .call_privilege = privilege::kernel_mode } );

  std::vector< state_db::state_node_ptr > fork_heads;

  ctx.set_state_node( _db.get_root( db_lock )->create_anonymous_node() );
  ctx.reset_cache();
  fork_heads = _db.get_fork_heads( db_lock );

  auto head_info = system_call::get_head_info( ctx );
  fdata.second   = head_info.head_topology();

  for( auto& fork: fork_heads )
  {
    ctx.set_state_node( fork->create_anonymous_node() );
    ctx.reset_cache();
    auto head_info = system_call::get_head_info( ctx );
    fdata.first.emplace_back( std::move( head_info.head_topology() ) );
  }

  // Sort all fork heads by height
  std::sort( fdata.first.begin(),
             fdata.first.end(),
             []( const block_topology& a, const block_topology& b )
             {
               return a.height() > b.height();
             } );

  // If there is a tie for highest block, ensure the head block is first
  auto fork_itr = fdata.first.begin();
  while( fork_itr != fdata.first.begin() && fork_itr->id() != head_info.head_topology().id() )
  {
    ++fork_itr;
  }

  if( fork_itr != fdata.first.begin() )
  {
    std::iter_swap( fork_itr, fdata.first.begin() );
  }

  return fdata;
}

rpc::chain::get_resource_limits_response
controller_impl::get_resource_limits( const rpc::chain::get_resource_limits_request& )
{
  execution_context ctx( _vm_backend );
  ctx.push_frame( stack_frame{ .call_privilege = privilege::kernel_mode } );

  ctx.set_state_node( _db.get_head( _db.get_shared_lock() )->create_anonymous_node() );
  ctx.reset_cache();

  auto value = system_call::get_resource_limits( ctx );

  rpc::chain::get_resource_limits_response resp;
  *resp.mutable_resource_limit_data() = value;

  return resp;
}

rpc::chain::get_account_rc_response controller_impl::get_account_rc( const rpc::chain::get_account_rc_request& request )
{
  KOINOS_ASSERT( request.account().size(),
                 missing_required_arguments_exception,
                 "missing expected field: ${f}",
                 ( "f", "payer" ) );

  execution_context ctx( _vm_backend );
  ctx.push_frame( stack_frame{ .call_privilege = privilege::kernel_mode } );

  ctx.set_state_node( _db.get_head( _db.get_shared_lock() )->create_anonymous_node() );
  ctx.reset_cache();

  auto value = system_call::get_account_rc( ctx, request.account() );

  rpc::chain::get_account_rc_response resp;
  resp.set_rc( value );

  return resp;
}

rpc::chain::get_fork_heads_response controller_impl::get_fork_heads( const rpc::chain::get_fork_heads_request& )
{
  rpc::chain::get_fork_heads_response resp;

  const auto [ fork_heads, last_irreversible_block ] = get_fork_data( _db.get_shared_lock() );
  auto topo                                          = resp.mutable_last_irreversible_block();
  *topo                                              = std::move( last_irreversible_block );

  for( const auto& fork_head: fork_heads )
  {
    auto* head = resp.add_fork_heads();
    *head      = fork_head;
  }

  return resp;
}

rpc::chain::read_contract_response controller_impl::read_contract( const rpc::chain::read_contract_request& request )
{
  KOINOS_ASSERT( request.contract_id().size(),
                 missing_required_arguments_exception,
                 "missing expected field: ${f}",
                 ( "f", "contract_id" ) );

  auto db_lock = _db.get_shared_lock();

  execution_context ctx( _vm_backend, intent::read_only );
  ctx.push_frame( stack_frame{
    .call_privilege = privilege::user_mode,
  } );

  std::shared_ptr< const protocol::block > head_block_ptr;

  {
    std::shared_lock< std::shared_mutex > head_lock( _cached_head_block_mutex );
    head_block_ptr = _cached_head_block;
    KOINOS_ASSERT( head_block_ptr, internal_error_exception, "error retrieving head block" );

    ctx.set_state_node( _db.get_head( db_lock )->create_anonymous_node() );
  }

  ctx.set_block( *head_block_ptr );
  ctx.reset_cache();

  resource_limit_data rl;
  rl.set_compute_bandwidth_limit( _read_compute_bandwidth_limit );

  ctx.resource_meter().set_resource_limit_data( rl );

  rpc::chain::read_contract_response resp;

  try
  {
    resp.set_result( system_call::call( ctx, request.contract_id(), request.entry_point(), request.args() ) );
  }
  catch( koinos::exception& e )
  {
    e.add_json( "logs", ctx.chronicler().logs() );
    throw e;
  }

  for( const auto& message: ctx.chronicler().logs() )
    *resp.add_logs() = message;

  return resp;
}

rpc::chain::get_account_nonce_response
controller_impl::get_account_nonce( const rpc::chain::get_account_nonce_request& request )
{
  KOINOS_ASSERT( request.account().size(),
                 missing_required_arguments_exception,
                 "missing expected field: ${f}",
                 ( "f", "account" ) );

  execution_context ctx( _vm_backend );

  ctx.push_frame( koinos::chain::stack_frame{ .call_privilege = privilege::kernel_mode } );

  ctx.set_state_node( _db.get_head( _db.get_shared_lock() )->create_anonymous_node() );
  ctx.reset_cache();

  auto nonce = system_call::get_account_nonce( ctx, request.account() );

  rpc::chain::get_account_nonce_response resp;
  resp.set_nonce( nonce );

  return resp;
}

rpc::chain::invoke_system_call_response
controller_impl::invoke_system_call( const rpc::chain::invoke_system_call_request& request )
{
  KOINOS_ASSERT( request.has_id() || request.has_name(),
                 missing_required_arguments_exception,
                 "missing expected field: ${f1} or ${f2}",
                 ( "f1", "id" )( "f2", "name" ) );

  execution_context ctx( _vm_backend, intent::read_only );

  stack_frame sframe;

  if( request.has_caller_data() )
  {
    sframe.contract_id    = request.caller_data().caller();
    sframe.call_privilege = request.caller_data().caller_privilege();
  }
  else
  {
    sframe.call_privilege = privilege::kernel_mode;
  }

  ctx.push_frame( std::move( sframe ) );

  ctx.set_state_node( _db.get_head( _db.get_shared_lock() )->create_anonymous_node() );
  ctx.reset_cache();

  resource_limit_data rl;
  rl.set_compute_bandwidth_limit( _read_compute_bandwidth_limit );

  ctx.resource_meter().set_resource_limit_data( rl );

  system_call_id syscall_id;

  if( request.has_id() )
  {
    syscall_id = system_call_id( request.id() );
  }
  else
  {
    if( !system_call_id_Parse( request.name(), &syscall_id ) )
      KOINOS_THROW( unknown_system_call_exception, "unknown system call name" );
  }

  koinos::chain::host_api hapi( ctx );

  std::vector< char > buffer( _syscall_bufsize, 0 );
  uint32_t bytes_written;
  rpc::chain::invoke_system_call_response resp;

  hapi.call( syscall_id,
             &buffer[ 0 ],
             _syscall_bufsize,
             request.args().c_str(),
             uint32_t( request.args().size() ),
             &bytes_written );

  resp.set_value( std::string( &buffer[ 0 ], bytes_written ) );

  return resp;
}

} // namespace detail

controller::controller( uint64_t read_compute_bandwith_limit,
                        uint32_t syscall_bufsize,
                        std::optional< uint64_t > pending_transaction_limit ):
    _my( std::make_unique< detail::controller_impl >( read_compute_bandwith_limit,
                                                      syscall_bufsize,
                                                      pending_transaction_limit ) )
{}

controller::~controller() = default;

void controller::open( const std::filesystem::path& p,
                       const chain::genesis_data& data,
                       fork_resolution_algorithm algo,
                       bool reset )
{
  _my->open( p, data, algo, reset );
}

void controller::close()
{
  _my->close();
}

void controller::set_client( std::shared_ptr< mq::client > c )
{
  _my->set_client( c );
}

void controller::set_log_directory( const std::filesystem::path& log_dir )
{
  _my->set_log_directory( log_dir );
}

rpc::chain::submit_block_response controller::submit_block( const rpc::chain::submit_block_request& request,
                                                            uint64_t index_to,
                                                            std::chrono::system_clock::time_point now )
{
  rpc::chain::submit_block_response resp;

  auto res = _my->apply_block( request.block(), detail::apply_block_options{ index_to, now, false } );

  if( res.receipt )
    *resp.mutable_receipt() = res.receipt.value();

  return resp;
}

void controller::apply_block_delta( const protocol::block& block,
                                    const protocol::block_receipt& receipt,
                                    uint64_t index_to )
{
  _my->apply_block_delta( block, receipt, index_to );
}

rpc::chain::propose_block_response controller::propose_block( const rpc::chain::propose_block_request& request,
                                                              uint64_t index_to,
                                                              std::chrono::system_clock::time_point now )
{
  rpc::chain::propose_block_response resp;

  auto res = _my->apply_block( request.block(), detail::apply_block_options{ index_to, now, true } );

  if( res.receipt )
    *resp.mutable_receipt() = res.receipt.value();
  else
  {
    for( auto i: res.failed_transaction_indices )
    {
      resp.add_failed_transaction_indices( i );
    }
  }

  return resp;
}

rpc::chain::submit_transaction_response
controller::submit_transaction( const rpc::chain::submit_transaction_request& request )
{
  return _my->submit_transaction( request );
}

rpc::chain::get_head_info_response controller::get_head_info( const rpc::chain::get_head_info_request& request )
{
  return _my->get_head_info( request );
}

rpc::chain::get_chain_id_response controller::get_chain_id( const rpc::chain::get_chain_id_request& request )
{
  return _my->get_chain_id( request );
}

rpc::chain::get_fork_heads_response controller::get_fork_heads( const rpc::chain::get_fork_heads_request& request )
{
  return _my->get_fork_heads( request );
}

rpc::chain::read_contract_response controller::read_contract( const rpc::chain::read_contract_request& request )
{
  return _my->read_contract( request );
}

rpc::chain::get_account_nonce_response
controller::get_account_nonce( const rpc::chain::get_account_nonce_request& request )
{
  return _my->get_account_nonce( request );
}

rpc::chain::get_account_rc_response controller::get_account_rc( const rpc::chain::get_account_rc_request& request )
{
  return _my->get_account_rc( request );
}

rpc::chain::get_resource_limits_response
controller::get_resource_limits( const rpc::chain::get_resource_limits_request& request )
{
  return _my->get_resource_limits( request );
}

rpc::chain::invoke_system_call_response
controller::invoke_system_call( const rpc::chain::invoke_system_call_request& request )
{
  return _my->invoke_system_call( request );
}

} // namespace koinos::chain
