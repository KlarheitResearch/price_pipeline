const GH_API_VERSION = "2022-11-28";
const USER_AGENT = "cf-gh-dispatch/legacy-core-5m";

export default {
  async fetch() {
    return new Response("legacy-core-5m worker alive", { status: 200 });
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
  const day = dt.getUTCDate();

  const jobs = [];

  // Main legacy cycle: every 10 minutes.
  if (minute % 10 === 0) {
    jobs.push({
      workflow: "gecko_legacy_core.yml",
      inputs: {
        run_live: "true",
        run_10m: "true",
        run_hourly: minute === 0 ? "true" : "false",
        run_daily_from_10m: hour === 0 && minute === 40 ? "true" : "false",
        run_monthly: "false",
        TOP_N: "1000",
        RANK_TOP_N: "1000",
        CURRENT_ONLY: "1",
        VERBOSE_MODE: "0",
      },
    });
  }

  // Daily true close API (top 100) after UTC midnight.
  if (hour === 0 && minute === 20) {
    jobs.push({
      workflow: "gecko_legacy_daily_api_close.yml",
      inputs: {
        run_daily_api_close: "true",
        TOP_N_API_DAILY: "100",
        TOP_N_AGG_DAILY: "1000",
        VERBOSE_MODE: "0",
      },
    });
  }

  // Monthly rollup once a month.
  if (day === 1 && hour === 0 && minute === 35) {
    jobs.push({
      workflow: "gecko_legacy_core.yml",
      inputs: {
        run_live: "false",
        run_10m: "false",
        run_hourly: "false",
        run_daily_from_10m: "false",
        run_monthly: "true",
        TOP_N: "1000",
        RANK_TOP_N: "1000",
        CURRENT_ONLY: "1",
        VERBOSE_MODE: "0",
      },
    });
  }

  for (const job of jobs) {
    await dispatchWorkflow(env, job.workflow, job.inputs);
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
  if (!res.ok)
    throw new Error(`Failed run check for ${workflowFile}: ${res.status} ${await res.text()}`);
  const data = await res.json();
  return (data.total_count || 0) > 0;
}

async function dispatchWorkflow(env, workflowFile, inputs) {
  const skipIfRunning = (env.SKIP_IF_RUNNING || "1") !== "0";
  if (skipIfRunning && (await isInProgress(env, workflowFile))) {
    console.log(`skip ${workflowFile}: already in progress`);
    return;
  }

  const url =
    `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}` +
    `/actions/workflows/${workflowFile}/dispatches`;

  const res = await fetch(url, {
    method: "POST",
    headers: ghHeaders(env),
    body: JSON.stringify({ ref: env.REF || "main", inputs }),
  });

  if (!res.ok) {
    throw new Error(`Dispatch failed ${workflowFile}: ${res.status} ${res.statusText} :: ${await res.text()}`);
  }

  console.log(`dispatched ${workflowFile}`);
}
