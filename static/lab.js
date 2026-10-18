// Shared helpers for the producer and consumer pages.

async function api(path, body) {
  const opts = body === undefined ? {} : {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
  const res = await fetch(path, opts);
  let data = {};
  try { data = await res.json(); } catch (e) { /* empty */ }
  if (!res.ok && !data.message) data.message = `Request failed (${res.status})`;
  return data;
}

function fmt(n) {
  if (n === null || n === undefined) return "–";
  return Number(n).toLocaleString();
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"]/g, ch => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]));
}

// verb: "Writing to" for the producer, "Reading from" for the consumer
function renderRoute(st, verb) {
  const route = document.getElementById("route");
  route.dataset.active = st.active;
  for (const name of ["primary", "backup"]) {
    const site = route.querySelector(`[data-site="${name}"]`);
    const up = st.health[name];
    site.classList.toggle("active", st.active === name);
    site.classList.toggle("is-down", up === false);
    site.querySelector(".host").textContent = st.bootstrap[name];
    const dot = site.querySelector(".dot");
    dot.className = "dot" + (up === true ? " up" : up === false ? " down" : "");
    site.querySelector(".state").textContent = up === true ? "Answering health checks" : up === false ? "Not responding" : "Checking…";
    site.querySelector(".tag").textContent = `${verb} this cluster`;
  }

  const say = document.getElementById("say");
  let text, cls = "";
  if (st.active === "primary") {
    if (st.failures === 0) {
      text = `${verb} the primary.`;
    } else if (st.auto_failover) {
      text = `Primary failed ${st.failures} of ${st.fail_threshold} checks. The app moves to the backup at ${st.fail_threshold}.`;
      cls = "warn";
    } else {
      text = `Primary failed ${st.failures} checks. Automatic failover is off, so the app stays on the primary.`;
      cls = "warn";
    }
  } else {
    text = `Failed over to the backup at ${st.switched_at}.`;
    cls = "alert";
    if (st.health.primary) text += " The primary is back. Switch back when you're ready.";
  }
  say.textContent = text;
  say.className = "say " + cls;

  document.getElementById("btn-failover").disabled = st.active !== "primary";
  document.getElementById("btn-failback").disabled = st.active !== "backup";
  const auto = document.getElementById("auto");
  if (document.activeElement !== auto) auto.checked = st.auto_failover;
}

function showOffline() {
  const say = document.getElementById("say");
  say.textContent = "This app is not responding. Check that it is still running (./run.sh status).";
  say.className = "say warn";
}

function renderEvents(events) {
  const ul = document.getElementById("events");
  ul.innerHTML = events.map(e =>
    `<li class="${e.level}"><span class="t">${esc(e.time)}</span><span class="m">${esc(e.msg)}</span></li>`
  ).join("");
}

function wireRouteControls(onDone) {
  document.getElementById("btn-failover").onclick = async () => {
    await api("/api/switch", { to: "backup" }); onDone();
  };
  document.getElementById("btn-failback").onclick = async () => {
    await api("/api/switch", { to: "primary" }); onDone();
  };
  document.getElementById("auto").onchange = async (e) => {
    await api("/api/auto-failover", { enabled: e.target.checked }); onDone();
  };
}

async function loadTopicList(listId) {
  const data = await api("/api/topics");
  const dl = document.getElementById(listId);
  dl.innerHTML = (data.topics || []).map(t => `<option value="${esc(t)}">`).join("");
}
