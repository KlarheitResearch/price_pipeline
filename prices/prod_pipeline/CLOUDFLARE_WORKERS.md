# Cloudflare Worker Setup (4-Worker Plan)

This is the current production trigger setup for `backend/.github/workflows/gecko_prod_*`.

Why 4 workers:
- Cloudflare Free plan has strict cron-trigger limits.
- This design multiplexes multiple GitHub workflow dispatches inside each worker run.

## Worker Inventory

1. `prod-core-5m`
- Cron: `*/5 * * * *`
- Dispatch:
  - always: `gecko_prod_live_tier1_5m.yml`
  - if `minute % 10 == 0`: `gecko_prod_tier1_10m.yml`
  - if `minute == 5`: `gecko_prod_tier2_hourly.yml`
  - if `minute == 15 && hour % 4 == 0`: `gecko_prod_tier3_4h.yml`

2. `prod-repair-hourly`
- Cron: `37 * * * *`
- Dispatch:
  - always: `gecko_prod_repair_tier1_hourly.yml`
  - if `hour % 4 == 0`: `gecko_prod_repair_tier2_4h.yml`

3. `prod-repair-daily`
- Cron: `55 2 * * *`
- Dispatch:
  - `gecko_prod_repair_tier3_daily.yml`

4. `prod-bootstrap-daily`
- Cron: `23 3 * * *`
- Dispatch:
  - `gecko_prod_bootstrap_new_entrants_1y.yml`

## Required Worker Vars/Secrets (all 4 workers)

Plaintext vars:
- `GH_OWNER` (e.g. `KlarheitResearch`)
- `GH_REPO` (e.g. `price_pipeline`)
- `REF` (usually `main`)

Secret:
- `GH_TOKEN` (GitHub token with workflow dispatch permission)

Optional:
- `SKIP_IF_RUNNING`
  - `0` = do not skip dispatch even if same workflow currently running
  - `1` = skip dispatch if same workflow already in progress

Recommended:
- `prod-core-5m`: `SKIP_IF_RUNNING=0`
- other 3 workers: `SKIP_IF_RUNNING=1`

Important:
- Do **not** use `WORKFLOW_FILE` or `INPUTS_JSON` with these scripts.
- These scripts contain explicit workflow filenames + inputs.

## Common Worker Script Helpers

Each script below is self-contained for copy/paste.

### 1) `prod-core-5m` script

```js
const GH_API_VERSION = "2022-11-28";
const USER_AGENT = "cf-gh-dispatch/prod-core-5m";

export default {
  async fetch() {
    return new Response("prod-core-5m alive", { status: 200 });
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(run(event, env));
  },
};

async function run(event, env) {
  requireEnv(env, ["GH_OWNER", "GH_REPO", "GH_TOKEN"]);

  const dt = new Date(event.scheduledTime);
  const minute = dt.getUTCMinutes();
  const hour = dt.getUTCHours();

  const jobs = [
    {
      workflow: "gecko_prod_live_tier1_5m.yml",
      inputs: { run_live: "true", VERBOSE_MODE: "0" },
    },
  ];

  if (minute % 10 === 0) {
    jobs.push({
      workflow: "gecko_prod_tier1_10m.yml",
      inputs: {
        run_live: "false",
        run_10m: "true",
        run_derivatives: "true",
        run_hourly: "true",
        run_daily: "true",
        run_monthly: "false",
        VERBOSE_MODE: "0",
      },
    });
  }

  if (minute === 5) {
    jobs.push({
      workflow: "gecko_prod_tier2_hourly.yml",
      inputs: {
        run_live: "true",
        run_10m: "true",
        run_mcap: "true",
        run_derivatives: "true",
        run_hourly: "true",
        run_daily: "true",
        run_monthly: "true",
        VERBOSE_MODE: "0",
      },
    });
  }

  if (minute === 15 && hour % 4 === 0) {
    jobs.push({
      workflow: "gecko_prod_tier3_4h.yml",
      inputs: {
        run_live: "true",
        run_10m: "true",
        run_derivatives: "true",
        run_hourly: "true",
        run_daily: "true",
        run_monthly: "true",
        VERBOSE_MODE: "0",
      },
    });
  }

  for (const job of jobs) {
    await dispatchWorkflow(env, job.workflow, job.inputs, "0");
  }
}

function requireEnv(env, keys) {
  const missing = keys.filter((k) => !env[k]);
  if (missing.length) throw new Error(`Missing required env: ${missing.join(", ")}`);
}

function ghHeaders(env) {
  return {
    Authorization: `Bearer ${env.GH_TOKEN}`,
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": GH_API_VERSION,
    "User-Agent": USER_AGENT,
    "Content-Type": "application/json",
  };
}

async function isInProgress(env, workflowFile) {
  const url =
    `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}` +
    `/actions/workflows/${workflowFile}/runs?status=in_progress&per_page=1`;
  const res = await fetch(url, { headers: ghHeaders(env) });
  if (!res.ok) throw new Error(`Failed run check for ${workflowFile}: ${res.status} ${await res.text()}`);
  const data = await res.json();
  return (data.total_count || 0) > 0;
}

async function dispatchWorkflow(env, workflowFile, inputs, defaultSkipIfRunning) {
  const skipIfRunning = (env.SKIP_IF_RUNNING || defaultSkipIfRunning) !== "0";
  if (skipIfRunning && (await isInProgress(env, workflowFile))) {
    console.log(`skip ${workflowFile}: already in progress`);
    return;
  }

  const url =
    `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}` +
    `/actions/workflows/${workflowFile}/dispatches`;

  const body = { ref: env.REF || "main", inputs };
  const res = await fetch(url, {
    method: "POST",
    headers: ghHeaders(env),
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    throw new Error(`Dispatch failed ${workflowFile}: ${res.status} ${res.statusText} :: ${await res.text()}`);
  }

  console.log(`dispatched ${workflowFile}`);
}
```

### 2) `prod-repair-hourly` script

```js
const GH_API_VERSION = "2022-11-28";
const USER_AGENT = "cf-gh-dispatch/prod-repair-hourly";

export default {
  async fetch() {
    return new Response("prod-repair-hourly alive", { status: 200 });
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(run(event, env));
  },
};

async function run(event, env) {
  requireEnv(env, ["GH_OWNER", "GH_REPO", "GH_TOKEN"]);

  const hour = new Date(event.scheduledTime).getUTCHours();
  const jobs = [
    {
      workflow: "gecko_prod_repair_tier1_hourly.yml",
      inputs: {
        run_live_refresh: "true",
        run_repair: "true",
        VERBOSE_MODE: "0",
      },
    },
  ];

  if (hour % 4 === 0) {
    jobs.push({
      workflow: "gecko_prod_repair_tier2_4h.yml",
      inputs: {
        run_live_refresh: "true",
        run_repair: "true",
        VERBOSE_MODE: "0",
      },
    });
  }

  for (const job of jobs) {
    await dispatchWorkflow(env, job.workflow, job.inputs, "1");
  }
}

function requireEnv(env, keys) {
  const missing = keys.filter((k) => !env[k]);
  if (missing.length) throw new Error(`Missing required env: ${missing.join(", ")}`);
}

function ghHeaders(env) {
  return {
    Authorization: `Bearer ${env.GH_TOKEN}`,
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": GH_API_VERSION,
    "User-Agent": USER_AGENT,
    "Content-Type": "application/json",
  };
}

async function isInProgress(env, workflowFile) {
  const url =
    `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}` +
    `/actions/workflows/${workflowFile}/runs?status=in_progress&per_page=1`;
  const res = await fetch(url, { headers: ghHeaders(env) });
  if (!res.ok) throw new Error(`Failed run check for ${workflowFile}: ${res.status} ${await res.text()}`);
  const data = await res.json();
  return (data.total_count || 0) > 0;
}

async function dispatchWorkflow(env, workflowFile, inputs, defaultSkipIfRunning) {
  const skipIfRunning = (env.SKIP_IF_RUNNING || defaultSkipIfRunning) !== "0";
  if (skipIfRunning && (await isInProgress(env, workflowFile))) {
    console.log(`skip ${workflowFile}: already in progress`);
    return;
  }

  const url =
    `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}` +
    `/actions/workflows/${workflowFile}/dispatches`;

  const body = { ref: env.REF || "main", inputs };
  const res = await fetch(url, {
    method: "POST",
    headers: ghHeaders(env),
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    throw new Error(`Dispatch failed ${workflowFile}: ${res.status} ${res.statusText} :: ${await res.text()}`);
  }

  console.log(`dispatched ${workflowFile}`);
}
```

### 3) `prod-repair-daily` script

```js
const GH_API_VERSION = "2022-11-28";
const USER_AGENT = "cf-gh-dispatch/prod-repair-daily";

export default {
  async fetch() {
    return new Response("prod-repair-daily alive", { status: 200 });
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(run(event, env));
  },
};

async function run(_event, env) {
  requireEnv(env, ["GH_OWNER", "GH_REPO", "GH_TOKEN"]);
  await dispatchWorkflow(
    env,
    "gecko_prod_repair_tier3_daily.yml",
    {
      run_live_refresh: "true",
      run_repair: "true",
      run_availability: "true",
      run_audit: "true",
      VERBOSE_MODE: "0",
    },
    "1"
  );
}

function requireEnv(env, keys) {
  const missing = keys.filter((k) => !env[k]);
  if (missing.length) throw new Error(`Missing required env: ${missing.join(", ")}`);
}

function ghHeaders(env) {
  return {
    Authorization: `Bearer ${env.GH_TOKEN}`,
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": GH_API_VERSION,
    "User-Agent": USER_AGENT,
    "Content-Type": "application/json",
  };
}

async function isInProgress(env, workflowFile) {
  const url =
    `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}` +
    `/actions/workflows/${workflowFile}/runs?status=in_progress&per_page=1`;
  const res = await fetch(url, { headers: ghHeaders(env) });
  if (!res.ok) throw new Error(`Failed run check for ${workflowFile}: ${res.status} ${await res.text()}`);
  const data = await res.json();
  return (data.total_count || 0) > 0;
}

async function dispatchWorkflow(env, workflowFile, inputs, defaultSkipIfRunning) {
  const skipIfRunning = (env.SKIP_IF_RUNNING || defaultSkipIfRunning) !== "0";
  if (skipIfRunning && (await isInProgress(env, workflowFile))) {
    console.log(`skip ${workflowFile}: already in progress`);
    return;
  }

  const url =
    `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}` +
    `/actions/workflows/${workflowFile}/dispatches`;

  const body = { ref: env.REF || "main", inputs };
  const res = await fetch(url, {
    method: "POST",
    headers: ghHeaders(env),
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    throw new Error(`Dispatch failed ${workflowFile}: ${res.status} ${res.statusText} :: ${await res.text()}`);
  }

  console.log(`dispatched ${workflowFile}`);
}
```

### 4) `prod-bootstrap-daily` script

```js
const GH_API_VERSION = "2022-11-28";
const USER_AGENT = "cf-gh-dispatch/prod-bootstrap-daily";

export default {
  async fetch() {
    return new Response("prod-bootstrap-daily alive", { status: 200 });
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(run(event, env));
  },
};

async function run(_event, env) {
  requireEnv(env, ["GH_OWNER", "GH_REPO", "GH_TOKEN"]);
  await dispatchWorkflow(
    env,
    "gecko_prod_bootstrap_new_entrants_1y.yml",
    {
      run_bootstrap: "true",
      run_monthly_backfill: "false",
      PP_BOOTSTRAP_MAX_COINS: "20",
      PP_BOOTSTRAP_DAYS: "365",
      VERBOSE_MODE: "0",
    },
    "1"
  );
}

function requireEnv(env, keys) {
  const missing = keys.filter((k) => !env[k]);
  if (missing.length) throw new Error(`Missing required env: ${missing.join(", ")}`);
}

function ghHeaders(env) {
  return {
    Authorization: `Bearer ${env.GH_TOKEN}`,
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": GH_API_VERSION,
    "User-Agent": USER_AGENT,
    "Content-Type": "application/json",
  };
}

async function isInProgress(env, workflowFile) {
  const url =
    `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}` +
    `/actions/workflows/${workflowFile}/runs?status=in_progress&per_page=1`;
  const res = await fetch(url, { headers: ghHeaders(env) });
  if (!res.ok) throw new Error(`Failed run check for ${workflowFile}: ${res.status} ${await res.text()}`);
  const data = await res.json();
  return (data.total_count || 0) > 0;
}

async function dispatchWorkflow(env, workflowFile, inputs, defaultSkipIfRunning) {
  const skipIfRunning = (env.SKIP_IF_RUNNING || defaultSkipIfRunning) !== "0";
  if (skipIfRunning && (await isInProgress(env, workflowFile))) {
    console.log(`skip ${workflowFile}: already in progress`);
    return;
  }

  const url =
    `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}` +
    `/actions/workflows/${workflowFile}/dispatches`;

  const body = { ref: env.REF || "main", inputs };
  const res = await fetch(url, {
    method: "POST",
    headers: ghHeaders(env),
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    throw new Error(`Dispatch failed ${workflowFile}: ${res.status} ${res.statusText} :: ${await res.text()}`);
  }

  console.log(`dispatched ${workflowFile}`);
}
```

## Troubleshooting

If worker logs show:
- `Missing required env: GH_OWNER, GH_REPO, GH_TOKEN`

check:
- vars/secrets are set on the same worker service that owns the cron trigger
- they are configured in the deployed environment (not only preview)
- worker was redeployed after variable updates

If GitHub runs appear as `Scheduled` unexpectedly:
- verify workflow `schedule:` blocks are still commented out
- verify repo variable `ENABLE_GH_FALLBACK_SCHEDULE` is `0`

