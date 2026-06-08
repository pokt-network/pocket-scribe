# Network configs

> Each PocketScribe deployment is pointed at exactly ONE Pocket Network deployment
> (mainnet, beta, or localnet). The config file in this directory tells
> `ps sync-upgrades` and `ps reconciler` how to reach the chain.

## Why one network per deployment

Each Pocket Network deployment is its own chain — its own genesis, its own
upgrade history, its own validators. You can't meaningfully index multiple
networks into the same Postgres because:
- The `block` table's `(height, time)` keys clash.
- The `upgrades` table would conflict (mainnet's v0.1.33 at 703870 vs beta's
  v0.1.33 at 153479 — both with name `"v0.1.33"`).
- All entity history would mix.

If you need to index multiple networks: **run multiple PocketScribe deployments**,
one per network, each with its own Postgres + NATS.

## Available configs

| File | Network | Description |
|---|---|---|
| `mainnet.yaml` | Pocket Shannon Mainnet | Production. Genesis 2025-03-28. ~764k blocks. |
| `beta.yaml` | Pocket Shannon Beta Testnet (a.k.a. Lego) | Public testnet, `chain_id: pocket-lego-testnet`. |
| `localnet.yaml` | Local devnet (via Tilt) | Your machine. Reset on each `tilt up`. |

## Selecting a network

```bash
# Mainnet
export PS_NETWORK_CONFIG=configs/networks/mainnet.yaml
ps sync-upgrades
ps consumer supplier

# Beta
export PS_NETWORK_CONFIG=configs/networks/beta.yaml
ps sync-upgrades
ps consumer supplier

# Or pass the flag directly
ps sync-upgrades --config configs/networks/mainnet.yaml
```

## Bootstrap sequence per deployment

```bash
# 1. Migrate the database schema (one-time per fresh DB)
ps migrate up

# 2. Populate `upgrades` table from the chain
ps sync-upgrades --config configs/networks/<network>.yaml

# 3. Verify
ps inspect upgrades

# 4. Start consumers
ps consumer supplier
```

The reconciler periodically re-runs sync-upgrades to catch new chain upgrades.

## Adding a custom network (operator-defined, e.g. forks)

Copy `mainnet.yaml` to `configs/networks/<your-name>.yaml`, adjust:
- `network.id` — unique logical name (recorded in Postgres for sanity-check).
- `network.chain_id` — verify with `curl <rpc>/status | jq .result.node_info.network`.
- `network.genesis_time` — from `curl <rpc>/status | jq .result.sync_info.earliest_block_time`.
- `network.genesis_decoder_version` — which poktroll binary was running at height 1.
- `endpoints.rpc/lcd/grpc` — your nodes.

Then run `ps sync-upgrades --config configs/networks/<your-name>.yaml`.

## What `ps sync-upgrades` does

1. Reads the config to know which chain to query.
2. Calls `/cosmos/upgrade/v1beta1/applied_plan/{name}` for each plan name in a list:
   - Pre-known names (genesis + every poktroll tag known to the running PocketScribe binary).
   - Speculative names (e.g., next 5 versions ahead) to catch upgrades not in the binary yet.
3. UPSERTs the `upgrades` table with chain-authoritative heights.
4. Logs any new upgrades detected.
5. Optionally fails if an unknown upgrade is found without a corresponding decoder package (use `--allow-missing-decoders` to override).

## What happens if you point one PocketScribe at multiple networks accidentally?

The first thing `ps consumer ...` does at startup is read `upgrades` and verify the
chain_id + genesis_time match the configured network. If they don't, the consumer
refuses to start with an explanatory error. This protects you from "wrong cluster" mistakes.
