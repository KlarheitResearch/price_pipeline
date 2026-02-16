const GH_API_VERSION = "2022-11-28";
const USER_AGENT = "cf-gh-dispatch/legacy-maintenance-daily";

export default {
  async fetch() {
    return new Response("legacy-maintenance-daily worker alive", { status: 200 });
  },
  async scheduled(_event, env, ctx) {
    ctx.waitUntil(run(env));
  },
};

async function run(env) {
  requireEnv(env, ["GH_OWNER", "GH_REPO", "GH_TOKEN"]);

  await dispatchWorkflow(env, "gecko_legacy_maintenance.yml", {
    run_availability: "true",
    run_audit: "true",
    VERBOSE_MODE: "0",
  });
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
