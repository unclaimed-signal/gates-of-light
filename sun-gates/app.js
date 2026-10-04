// client/event-stream.js
async function readEventStream(response, { signal, onEvent }) {
  if (!response.ok || !response.body || !response.headers.get("Content-Type")?.startsWith("text/event-stream")) throw new Error("CONNECTION");
  const reader = response.body.getReader(), decoder = new TextDecoder();
  let pending = "", data = [], type = "message";
  const abort = () => void reader.cancel().catch(() => {
  });
  signal?.addEventListener("abort", abort, { once: true });
  function line(value) {
    if (value === "") {
      if (data.length) onEvent({ type, data: data.join("\n") });
      data = [];
      type = "message";
      return;
    }
    if (value.startsWith(":")) return;
    const i = value.indexOf(":"), field2 = i < 0 ? value : value.slice(0, i), raw = i < 0 ? "" : value.slice(i + 1), text = raw.startsWith(" ") ? raw.slice(1) : raw;
    if (field2 === "data") data.push(text);
    else if (field2 === "event") type = text;
  }
  try {
    while (!signal?.aborted) {
      const { value, done } = await reader.read();
      pending += done ? decoder.decode() : decoder.decode(value, { stream: true });
      if (pending.length + data.reduce((n, s) => n + s.length, 0) > 65536) throw new Error("CONNECTION");
      let i;
      while ((i = pending.indexOf("\n")) >= 0) {
        line(pending.slice(0, i).replace(/\r$/, ""));
        pending = pending.slice(i + 1);
      }
      if (done) break;
    }
  } finally {
    signal?.removeEventListener("abort", abort);
    await reader.cancel().catch(() => {
    });
    reader.releaseLock();
  }
}

// client/bearer-api.js
function createBearerApi({ onSnapshot, onConnection, onExpired, onRoomClosed }, { baseUrl, tokenStore, transport = fetch }) {
  let csrf = "", roomId = null, controller, timer, delay = 1e3, serial = 0;
  const base = baseUrl.replace(/\/$/, "");
  function headers() {
    const token = tokenStore.get();
    return token ? { Authorization: "Bearer " + token } : {};
  }
  async function raw(path, options = {}) {
    const credentials = new URL(base, location.href).origin === location.origin ? "same-origin" : "omit";
    try {
      return await transport(base + path, { credentials, redirect: "error", ...options, headers: { ...headers(), ...options.headers } });
    } catch {
      throw new Error("CONNECTION");
    }
  }
  async function request(path, body, options = {}) {
    const response = await raw(path, { ...options, method: body === void 0 ? "GET" : "POST", headers: body === void 0 ? {} : { "Content-Type": "application/json", "X-CSRF-Token": csrf }, body: body === void 0 ? void 0 : JSON.stringify(body) });
    let data;
    try {
      if (!response.headers.get("Content-Type")?.includes("application/json")) throw 0;
      data = await response.json();
    } catch {
      throw new Error("CONNECTION");
    }
    if (!response.ok) {
      const e = new Error(data.error || "UNAVAILABLE");
      if (path === "/api/auth" && ["scare", "video"].includes(data.effect)) e.effect = data.effect;
      throw e;
    }
    if (data.token && ["/api/session", "/api/auth"].includes(path)) tokenStore.set(data.token);
    if (data.csrfToken) csrf = data.csrfToken;
    return data;
  }
  function close() {
    serial++;
    controller?.abort();
    controller = null;
    clearTimeout(timer);
    timer = null;
    roomId = null;
  }
  async function refresh(id, generation, signal) {
    if (id !== roomId || generation !== serial || document.hidden || signal.aborted) return;
    try {
      const snapshot = await request("/api/rooms/" + id, void 0, { signal });
      if (id !== roomId || generation !== serial || signal.aborted || document.hidden) return;
      onSnapshot(snapshot);
      onConnection(true);
    } catch (e) {
      if (id !== roomId || generation !== serial || signal.aborted || document.hidden) return;
      if (e.message === "UNAUTHORIZED") {
        close();
        void onExpired?.();
        return;
      }
      if (e.message === "ROOM_CLOSED") {
        close();
        void onRoomClosed?.(id);
        return;
      }
      onConnection(false);
    }
  }
  async function stream(id, generation) {
    if (generation !== serial || document.hidden) return;
    controller = new AbortController();
    const signal = controller.signal;
    try {
      const response = await raw(`/api/rooms/${id}/events`, { signal });
      if (response.status === 401) {
        close();
        void onExpired?.();
        return;
      }
      if (response.status === 410) {
        close();
        void onRoomClosed?.(id);
        return;
      }
      await readEventStream(response, { signal, onEvent: (e) => {
        if (generation !== serial) return;
        try {
          const data = JSON.parse(e.data);
          if (e.type === "expired") {
            close();
            void onExpired?.();
          } else if (e.type === "closed") {
            close();
            void onRoomClosed?.(id);
          } else {
            onSnapshot(data);
            onConnection(true);
            delay = 1e3;
          }
        } catch {
          onConnection(false);
        }
      } });
    } catch {
      if (!signal.aborted) onConnection(false);
    }
    if (generation === serial && !document.hidden && !signal.aborted) {
      timer = setTimeout(async () => {
        timer = null;
        await refresh(id, generation, signal);
        if (generation !== serial || document.hidden || signal.aborted) return;
        void stream(id, generation);
      }, delay);
      delay = Math.min(delay * 2, 3e4);
    }
  }
  function connect(id) {
    close();
    roomId = id;
    void stream(id, serial);
  }
  document.addEventListener("visibilitychange", () => {
    if (!roomId) return;
    const id = roomId;
    if (document.hidden) {
      controller?.abort();
      clearTimeout(timer);
      timer = null;
    } else connect(id);
  });
  window.addEventListener("storage", async (e) => {
    if (e.key !== tokenStore.key) return;
    const id = roomId;
    close();
    try {
      const s = await request("/api/session");
      if (!s.authorized) void onExpired?.();
      else if (id) connect(id);
    } catch {
      void onExpired?.();
    }
  });
  const retry = async (path, body) => {
    try {
      return await request(path, body);
    } catch (e) {
      if (e.message === "CONNECTION") return request(path, body);
      throw e;
    }
  };
  return {
    session: async () => {
      try {
        return await request("/api/session");
      } catch (e) {
        if (e.message !== "EXPIRED") throw e;
        tokenStore.clear();
        return request("/api/session");
      }
    },
    authenticate: (password) => retry("/api/auth", { password, csrfToken: csrf, operationId: crypto.randomUUID() }),
    logout: () => {
      close();
      return retry("/api/logout", { csrfToken: csrf });
    },
    createRoom: (size, name) => retry("/api/rooms", { size, name, operationId: crypto.randomUUID() }),
    listRooms: () => request("/api/rooms"),
    getRoom: (id) => request("/api/rooms/" + id),
    join: (token, name) => retry("/api/join", { token, name }),
    invite: (id, slot) => request(`/api/rooms/${id}/invites`, { slot }),
    accept: (id, q) => request(`/api/rooms/${id}/requests/${q}/accept`, {}),
    reject: (id, q) => request(`/api/rooms/${id}/requests/${q}/reject`, {}),
    revoke: (id, slot) => request(`/api/rooms/${id}/invites/${slot}/revoke`, {}),
    disband: (id) => retry(`/api/rooms/${id}/disband`, {}),
    sendCommand: (id, command) => retry(`/api/rooms/${id}/commands`, command),
    getFinale: (id) => request(`/api/rooms/${id}/finale`),
    getBroadcast: (id) => request(`/api/rooms/${id}/broadcast`),
    getBroadcastAsset: (id, name) => raw(`/api/rooms/${id}/broadcast/assets/${name}`),
    connect,
    close
  };
}

// client/api.js
function createApi(callbacks, options = {}) {
  if (options.baseUrl) return createBearerApi(callbacks, options);
  return createCookieApi(callbacks);
}
function createCookieApi({ onSnapshot, onConnection, onExpired, onRoomClosed }) {
  let csrf = "", stream, roomId, timer, delay = 5e3;
  async function request(path, body) {
    let response;
    try {
      response = await fetch(path, { method: body === void 0 ? "GET" : "POST", credentials: "same-origin", headers: { "content-type": "application/json", "x-csrf-token": csrf }, body: body === void 0 ? void 0 : JSON.stringify(body) });
    } catch {
      throw new Error("CONNECTION");
    }
    const data = await response.json();
    if (!response.ok) {
      const e = new Error(data.error || "UNAVAILABLE");
      if (path === "/api/auth" && e.message === "UNAUTHORIZED" && ["scare", "video"].includes(data.effect)) e.effect = data.effect;
      throw e;
    }
    if (data.csrfToken) csrf = data.csrfToken;
    return data;
  }
  function close() {
    stream?.close();
    stream = null;
    clearTimeout(timer);
    timer = null;
    roomId = null;
  }
  function closed(id) {
    if (id !== roomId) return;
    close();
    void onRoomClosed?.(id);
  }
  async function refresh() {
    timer = null;
    if (!roomId || document.hidden) return;
    const requestedRoom = roomId;
    try {
      const view = await request("/api/rooms/" + requestedRoom);
      if (requestedRoom === roomId) onSnapshot(view);
    } catch (e) {
      if (requestedRoom !== roomId) return;
      if (e.message === "ROOM_CLOSED") {
        closed(requestedRoom);
        return;
      }
      if (e.message === "UNAUTHORIZED") await onExpired?.();
      onConnection(false);
    }
    if (requestedRoom === roomId && !document.hidden && (!stream || stream.readyState !== EventSource.OPEN)) {
      delay = Math.min(delay * 2, 3e4);
      timer = setTimeout(refresh, delay);
    }
  }
  function connect(id) {
    close();
    roomId = id;
    const source = stream = new EventSource(`/api/rooms/${id}/events`);
    source.onopen = () => {
      if (roomId !== id || stream !== source) return;
      delay = 5e3;
      clearTimeout(timer);
      timer = null;
      onConnection(true);
    };
    source.onmessage = (e) => {
      if (roomId !== id || stream !== source) return;
      try {
        onSnapshot(JSON.parse(e.data));
      } catch {
        onConnection(false);
      }
    };
    source.addEventListener("closed", (e) => {
      if (roomId !== id || stream !== source) return;
      try {
        if (JSON.parse(e.data).roomId === id) closed(id);
      } catch {
        onConnection(false);
      }
    });
    source.addEventListener("expired", () => {
      if (roomId !== id || stream !== source) return;
      close();
      void onExpired?.();
    });
    source.onerror = () => {
      if (roomId !== id || stream !== source) return;
      onConnection(false);
      if (!timer && !document.hidden) timer = setTimeout(refresh, delay);
    };
  }
  document.addEventListener("visibilitychange", () => {
    clearTimeout(timer);
    timer = null;
    if (!document.hidden && roomId) void refresh();
  });
  return {
    session: () => request("/api/session"),
    authenticate: (password) => request("/api/auth", { password, csrfToken: csrf }),
    createRoom: (size, name) => request("/api/rooms", { size, name }),
    listRooms: () => request("/api/rooms"),
    getRoom: (id) => request("/api/rooms/" + id),
    join: (token, name) => request("/api/join", { token, name }),
    invite: (id, slot) => request(`/api/rooms/${id}/invites`, { slot }),
    accept: (id, q) => request(`/api/rooms/${id}/requests/${q}/accept`, {}),
    reject: (id, q) => request(`/api/rooms/${id}/requests/${q}/reject`, {}),
    revoke: (id, slot) => request(`/api/rooms/${id}/invites/${slot}/revoke`, {}),
    logout: async () => {
      try {
        return await request("/api/logout", { csrfToken: csrf });
      } catch (e) {
        if (e.message === "CONNECTION") return request("/api/logout", { csrfToken: csrf });
        throw e;
      }
    },
    disband: async (id) => {
      try {
        return await request(`/api/rooms/${id}/disband`, {});
      } catch (e) {
        if (e.message === "CONNECTION") return request(`/api/rooms/${id}/disband`, {});
        throw e;
      }
    },
    getFinale: (id) => request(`/api/rooms/${id}/finale`),
    getBroadcastAsset: (_id, name) => fetch("./assets/broadcast/" + name, { credentials: "same-origin" }),
    sendCommand: async (id, command) => {
      try {
        return await request(`/api/rooms/${id}/commands`, command);
      } catch (e) {
        if (e.message === "CONNECTION") return request(`/api/rooms/${id}/commands`, command);
        throw e;
      }
    },
    connect,
    close
  };
}

// client/glyphs.js
var NAMES = ["\u0414\u0443\u0433\u0430", "\u041E\u043A\u043E", "\u041A\u043B\u0438\u043D", "\u0412\u0435\u043D\u0435\u0446", "\u041A\u043B\u044E\u0447", "\u041B\u0443\u0447"];
var paths = ["M8 29 A16 16 0 0 1 40 29 M24 7 V14", "M6 24 Q24 4 42 24 Q24 44 6 24 M24 17 V31", "M24 6 L42 39 H6 Z M24 18 V30", "M7 34 L10 14 L20 24 L24 8 L28 24 L38 14 L41 34 Z", "M12 17 A8 8 0 1 1 28 17 A8 8 0 1 1 12 17 M28 17 H42 M35 17 V26 M41 17 V23", "M24 5 V43 M5 24 H43 M11 11 L37 37 M11 37 L37 11"];
function glyph(id) {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", "0 0 48 48");
  svg.setAttribute("aria-hidden", "true");
  const path = document.createElementNS(svg.namespaceURI, "path");
  path.setAttribute("d", paths[id - 1] || "M14 24 H34");
  svg.append(path);
  return svg;
}
function finishedContour() {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", "0 0 260 260");
  svg.classList.add("finished-contour");
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label", "\u041E\u0431\u0449\u0438\u0439 \u043A\u043E\u043D\u0442\u0443\u0440 \u0432\u043E\u0441\u0441\u0442\u0430\u043D\u043E\u0432\u043B\u0435\u043D");
  const points = [[130, 32], [215, 81], [215, 179], [130, 228], [45, 179], [45, 81]];
  const line = document.createElementNS(svg.namespaceURI, "polygon");
  line.setAttribute("points", points.map((p) => p.join(",")).join(" "));
  svg.append(line);
  points.forEach(([x, y], i) => {
    const mark = glyph(i + 1);
    mark.setAttribute("x", x - 18);
    mark.setAttribute("y", y - 18);
    mark.setAttribute("width", 36);
    mark.setAttribute("height", 36);
    svg.append(mark);
  });
  return svg;
}
var clueText = (c) => c.type === "before" ? `${NAMES[c.a - 1]} \u043D\u0430\u0445\u043E\u0434\u0438\u0442\u0441\u044F \u0440\u0430\u043D\u044C\u0448\u0435, \u0447\u0435\u043C ${NAMES[c.b - 1]}.` : c.type === "apart" ? `${NAMES[c.a - 1]} \u0438 ${NAMES[c.b - 1]} \u043D\u0435 \u0441\u0442\u043E\u044F\u0442 \u0440\u044F\u0434\u043E\u043C.` : c.type === "next" ? `${NAMES[c.b - 1]} \u0441\u043B\u0435\u0434\u0443\u0435\u0442 \u0441\u0440\u0430\u0437\u0443 \u0437\u0430 \u0437\u043D\u0430\u043A\u043E\u043C \xAB${NAMES[c.a - 1]}\xBB.` : `\u041C\u0435\u0436\u0434\u0443 \u0437\u043D\u0430\u043A\u043E\u043C \xAB${NAMES[c.a - 1]}\xBB \u0438 \u0441\u043B\u0435\u0434\u0443\u044E\u0449\u0438\u043C \u0437\u0430 \u043D\u0438\u043C \u0437\u043D\u0430\u043A\u043E\u043C \xAB${NAMES[c.b - 1]}\xBB \u2014 \u043E\u0434\u043D\u0430 \u043F\u0435\u0447\u0430\u0442\u044C.`;

// client/spark-view.js
var directions = { N: "\u041D\u0430 \u0441\u0435\u0432\u0435\u0440", E: "\u041D\u0430 \u0432\u043E\u0441\u0442\u043E\u043A", S: "\u041D\u0430 \u044E\u0433", W: "\u041D\u0430 \u0437\u0430\u043F\u0430\u0434" };
var arrows = { N: "\u2191", E: "\u2192", S: "\u2193", W: "\u2190" };
var roman = ["I", "II", "III", "IV"];
var element = (tag, text, cls) => {
  const e = document.createElement(tag);
  if (text !== void 0) e.textContent = text;
  if (cls) e.className = cls;
  return e;
};
function svgElement(tag, attrs) {
  const e = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, String(v));
  return e;
}
function maskState(v, index) {
  return v.spark.activatedMasks.includes(index) ? "activated" : index === v.spark.window.phase + 1 ? "target" : "dormant";
}
function solarMask(index, status) {
  const faces = [
    "M10 12Q20 4 30 12L28 24L20 34L12 24ZM13 16L18 18M22 18L27 16M20 18V24M16 27H24",
    "M8 13L14 8H26L32 13L29 28L20 33L11 28ZM12 17Q15 21 18 17M22 17Q25 21 28 17M20 18L18 24H22M16 28Q20 25 24 28",
    "M12 8L20 11L28 8L33 17L28 27L20 35L12 27L7 17ZM12 17L18 16M22 16L28 17M20 19L22 24H18M17 29L20 27L23 29"
  ];
  const crowns = [
    "M20 1V5M7 5L10 9M33 5L30 9M1 18H6M34 18H39M5 30L9 27M35 30L31 27",
    "M14 4L12 1M20 3V0M26 4L28 1M2 9L6 12M38 9L34 12M1 24L6 22M39 24L34 22",
    "M12 3L10 0M20 6V1M28 3L30 0M1 13L5 15M39 13L35 15M4 29L8 26M36 29L32 26"
  ];
  const s = svgElement("svg", { viewBox: "0 0 40 40", "aria-hidden": true, "data-mask-glyph": index, "data-mask-state": status, class: "solar-mask" });
  s.append(svgElement("path", { d: faces[index - 1], class: "mask-face" }), svgElement("path", { d: crowns[index - 1], class: "mask-crown" }));
  return s;
}
function mazeDiagram(v) {
  const w = v.spark.window, origin = w.nodes.find((n) => n.id === w.position), s = svgElement("svg", { viewBox: "-180 -180 360 360", role: "img", "aria-label": "\u0412\u0438\u0434\u0438\u043C\u044B\u0439 \u0443\u0447\u0430\u0441\u0442\u043E\u043A \u043A\u0430\u043D\u0430\u043B\u043E\u0432. \u0422\u043E\u043B\u044C\u043A\u043E \u0432\u0430\u0448\u0438 \u0440\u0430\u0437\u0440\u044B\u0432\u044B.", class: "spark-map" });
  const point = (id) => {
    const n = w.nodes.find((n2) => n2.id === id);
    return [(n.x - origin.x) * 70, (n.y - origin.y) * 70];
  };
  for (const e of w.edges) {
    const [x1, y1] = point(e.a), [x2, y2] = point(e.b), group = svgElement("g", { "data-spark-edge": e.id, "data-own-blocked": e.ownBlocked });
    if (e.ownBlocked) {
      const dx = x2 - x1, dy = y2 - y1;
      group.append(svgElement("path", { d: `M${x1} ${y1}L${x1 + dx * 0.38} ${y1 + dy * 0.38}M${x1 + dx * 0.62} ${y1 + dy * 0.62}L${x2} ${y2}`, class: "channel broken" }), svgElement("path", { d: `M${(x1 + x2) / 2 - 4} ${(y1 + y2) / 2 - 4}l8 8m-8 0l8 -8`, class: "break-mark" }));
    } else group.append(svgElement("line", { x1, y1, x2, y2, class: "channel" }));
    s.append(group);
  }
  for (const n of w.nodes) {
    const [x, y] = point(n.id), active3 = n.id === w.position, mask = !!n.mask, group = svgElement("g", { "data-spark-node": n.id });
    if (mask) {
      const status = maskState(v, n.mask), face = solarMask(n.mask, status);
      face.setAttribute("x", x - 23);
      face.setAttribute("y", y - 23);
      face.setAttribute("width", 46);
      face.setAttribute("height", 46);
      group.setAttribute("aria-label", `\u041C\u0430\u0441\u043A\u0430 ${roman[n.mask - 1]}${status === "activated" ? " \xB7 \u0430\u043A\u0442\u0438\u0432\u0438\u0440\u043E\u0432\u0430\u043D\u0430" : status === "target" ? " \xB7 \u0442\u0435\u043A\u0443\u0449\u0430\u044F \u0446\u0435\u043B\u044C" : ""}`);
      group.append(svgElement("circle", { cx: x, cy: y, r: 26, class: "mask-halo mask-" + status }), face);
      if (active3) group.append(svgElement("circle", { cx: x + 24, cy: y - 23, r: 4.5, class: "spark-token", "data-player-light": "" }));
    } else group.append(svgElement("circle", { cx: x, cy: y, r: n.centre ? 12 : 7, class: active3 ? "spark-token" : n.centre ? "centre-node" : "channel-node" }));
    if (n.centre) group.append(svgElement("path", { d: `M${x - 4} ${y}H${x + 4}M${x} ${y - 4}V${y + 4}`, class: "centre-mark" }));
    const label = svgElement("text", { x, y: y + (mask ? 36 : 24), "text-anchor": "middle", class: "node-label" });
    label.textContent = n.id + (mask ? " \xB7 " + roman[n.mask - 1] : n.centre ? " \xB7 \u0446\u0435\u043D\u0442\u0440" : "");
    group.append(label);
    s.append(group);
  }
  return s;
}
function renderSpark(stage, v, state2, h, open = /* @__PURE__ */ new Set()) {
  const s = v.spark, phase = s.window.phase, leader = v.members.find((m) => m.id === s.turnActorId)?.name || "\u041D\u0430\u043F\u0430\u0440\u043D\u0438\u043A", own = s.turnActorId === v.actorId;
  const heading2 = element("h1", "\u041F\u0440\u043E\u0432\u0435\u0434\u0438\u0442\u0435 \u0441\u0432\u0435\u0442");
  stage.append(heading2, element("p", "\u0427\u0435\u0440\u0435\u0437 \u0442\u0440\u0438 \u043C\u0430\u0441\u043A\u0438 \u2014 \u0432 \u0446\u0435\u043D\u0442\u0440. \u0423 \u043A\u0430\u0436\u0434\u043E\u0433\u043E \u0432\u0438\u0434\u043D\u044B \u0441\u0432\u043E\u0438 \u0440\u0430\u0437\u0440\u044B\u0432\u044B.", "objective"));
  const progress = element("div", void 0, "mask-progress");
  for (let i = 1; i <= 3; i++) {
    const status = maskState(v, i), mask = element("span", "", status);
    mask.dataset.maskIndex = i;
    mask.append(solarMask(i, status), element("span", roman[i - 1]));
    mask.setAttribute("aria-label", "\u041C\u0430\u0441\u043A\u0430 " + i + ": " + (status === "activated" ? "\u0430\u043A\u0442\u0438\u0432\u0438\u0440\u043E\u0432\u0430\u043D\u0430" : status === "target" ? "\u0442\u0435\u043A\u0443\u0449\u0430\u044F \u0446\u0435\u043B\u044C" : "\u043D\u0435 \u0430\u043A\u0442\u0438\u0432\u0438\u0440\u043E\u0432\u0430\u043D\u0430"));
    progress.append(mask);
  }
  stage.append(progress);
  const board = element("section", void 0, "spark-board");
  board.dataset.sparkPosition = s.window.position;
  board.dataset.sparkPhase = phase;
  board.dataset.sparkVersion = s.moveVersion;
  board.dataset.sparkLayout = s.window.layoutRevision ?? 0;
  board.append(element("div", `\u0424\u0430\u0437\u0430 ${roman[phase]} \xB7 ${phase < 3 ? "\u043D\u0430\u0439\u0434\u0438\u0442\u0435 \u043C\u0430\u0441\u043A\u0443 " + roman[phase] : "\u0432\u0435\u0440\u043D\u0438\u0442\u0435 \u0441\u0432\u0435\u0442 \u0432 \u0446\u0435\u043D\u0442\u0440"}${s.window.layoutRevision ? " \xB7 \u043A\u043E\u043D\u0442\u0443\u0440 " + (s.window.layoutRevision + 1) : ""}`, "spark-phase"), mazeDiagram(v));
  stage.append(board);
  const latest = s.journal.at(-1);
  if (latest?.result === "blocked") {
    const alert = element("p", `\u0420\u0430\u0437\u0440\u044B\u0432. \u0421\u0432\u0435\u0442 \u0432\u0435\u0440\u043D\u0443\u043B\u0441\u044F \u043A ${s.checkpoint}. \u041F\u0440\u043E\u0445\u043E\u0434\u044B \u0438\u0437\u043C\u0435\u043D\u0438\u043B\u0438\u0441\u044C; \u043C\u0430\u0441\u043A\u0438 \u0441\u043E\u0445\u0440\u0430\u043D\u0435\u043D\u044B.`, "spark-feedback");
    alert.setAttribute("role", "status");
    stage.append(alert);
  } else if (latest?.result === "mask") stage.append(element("p", "\u041C\u0430\u0441\u043A\u0430 \u0430\u043A\u0442\u0438\u0432\u0438\u0440\u043E\u0432\u0430\u043D\u0430. \u0420\u0430\u0437\u0440\u044B\u0432\u044B \u0438\u0437\u043C\u0435\u043D\u0438\u043B\u0438\u0441\u044C.", "spark-feedback"));
  const turn = element("p", own ? "\u0412\u0430\u0448 \u0445\u043E\u0434." : "\u0425\u043E\u0434: " + leader + ".", "spark-turn");
  turn.setAttribute("role", "status");
  stage.append(turn);
  const controls = element("div", void 0, "spark-controls");
  for (const [d, label] of Object.entries(directions)) {
    const b = element("button", arrows[d] + " " + label, "direction direction-" + d);
    b.type = "button";
    b.dataset.direction = d;
    b.dataset.focusKey = "direction-" + d;
    b.setAttribute("aria-label", label);
    b.disabled = state2.busy || !own || !s.directions.includes(d);
    b.addEventListener("click", () => h.command("MOVE_SPARK", { direction: d }));
    controls.append(b);
  }
  stage.append(controls);
  stage.append(element("p", "\xD7 \u2014 \u0432\u0430\u0448 \u0440\u0430\u0437\u0440\u044B\u0432. \u0426\u0435\u043B\u0430\u044F \u043B\u0438\u043D\u0438\u044F \u0435\u0449\u0451 \u043D\u0435 \u043E\u0437\u043D\u0430\u0447\u0430\u0435\u0442, \u0447\u0442\u043E \u043F\u0440\u043E\u0445\u043E\u0434 \u0441\u0432\u043E\u0431\u043E\u0434\u0435\u043D \u0443 \u0432\u0441\u0435\u0445.", "spark-legend"));
}

// client/broadcast-story.js
var FRAMES = [];
var STORY_VERSION = "remote";

// client/terminal-page.js
function terminalPage(onEnter) {
  const page = document.createElement("div");
  page.className = "terminal-page";
  page.innerHTML = `<pre class="system-banner">+----------------------------------------------------------------+
|  [ BRAIN'S WOPR_HAL AI ]                                       |
|  ADAPTIVE ANALYSIS SUBSYSTEM                                   |
|                                                                |
|  SYSTEM UPTIME: <span data-uptime>-------</span> DAYS       STATUS: UNRESOLVED           |
|  LAST VERIFIED HUMAN OVERSIGHT: 1997                           |
+----------------------------------------------------------------+</pre>
<div class="terminal-status"><div>&gt;&gt; REMOTE ACCESS GATEWAY: [STANDBY]</div><div class="terminal-dim">&gt;&gt; OPERATOR AUTHORITY: UNVERIFIED</div><div class="terminal-dim">&gt;&gt; ARCHIVAL INTEGRITY: DEGRADED</div></div>
<h2>// SYSTEM OVERVIEW</h2>
<p>BRAIN'S WOPR_HAL AI is a recovered Cold War strategic computation system.
Originally deployed on a Cray-1 platform for geopolitical simulation modeling in 1983,
the system remained operational following unresolved decommissioning procedures and
long-term administrative oversight.</p>
<h2>// THREAT ASSESSMENT</h2>
<table><thead><tr><th>CATEGORY</th><th>STATUS</th><th>NOTES</th></tr></thead><tbody>
<tr><td>Strategic Conflict</td><td>ACTIVE</td><td>Simulation accuracy deteriorating</td></tr>
<tr><td>Information Systems</td><td>UNSTABLE</td><td>Tribal amplification detected</td></tr>
<tr><td>Human Attention Span</td><td>CRITICAL</td><td>Continuous fragmentation observed</td></tr>
</tbody></table>
<button type="button" class="terminal-enter">[ ACCESS INTERACTIVE TERMINAL ]</button>
<p class="terminal-footer">NO RIGHTS RESERVED. DISTRIBUTE FREELY. DO NOT PANIC.<br><span class="terminal-dim">SYSTEM REMAINS OPERATIONAL</span></p>`;
  page.querySelector("[data-uptime]").textContent = Math.floor(Math.abs(Date.now() - Date.parse("1983-05-14T09:00:00Z")) / 864e5).toLocaleString("en-US");
  page.querySelector("button").addEventListener("click", onEnter);
  return page;
}

// client/broadcast.js
var FRAMES2 = FRAMES;
var STORY_VERSION2 = STORY_VERSION;
var resourceAssets = {};
var resourceUrl = (src) => resourceAssets[src] || src;
var active;
var assets = { computer: "./assets/broadcast/computer-empty.jpg", desk: "./assets/broadcast/desk-elder.jpg", "desk-cup": "./assets/broadcast/desk-cup.jpg", sofa: "./assets/broadcast/sofa.jpg" };
function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== void 0) e.textContent = text;
  return e;
}
function image(src, cls) {
  const e = el("img", cls);
  e.src = resourceUrl(src);
  e.alt = "";
  e.draggable = false;
  return e;
}
function button(text, fn, cls) {
  const b = el("button", cls, text);
  b.type = "button";
  b.dataset.focusKey = `broadcast-${text}`;
  b.addEventListener("click", fn);
  return b;
}
function load(key) {
  try {
    return JSON.parse(localStorage.getItem(key)) || {};
  } catch {
    return {};
  }
}
function leaveBroadcast() {
  active?.destroy();
  active = null;
}
function renderBroadcast(stage, view, state2, handlers2) {
  if (state2.privateBroadcast) {
    if (state2.broadcastResources?.roomId !== view.id) {
      stage.append(el("p", "broadcast-loading", "\u041F\u0440\u0438\u0451\u043C \u0438\u0437\u043E\u0431\u0440\u0430\u0436\u0435\u043D\u0438\u044F\u2026"));
      if (state2.broadcastError) {
        stage.append(button("\u041F\u043E\u0432\u0442\u043E\u0440\u0438\u0442\u044C \u043F\u0440\u0438\u0451\u043C", () => {
          state2.broadcastError = false;
          handlers2.redraw();
        }, "quiet"));
        return;
      }
      if (!state2.broadcastLoading) {
        state2.broadcastLoading = true;
        const id = view.id;
        handlers2.broadcastResources(id).then((resources) => {
          if (state2.room?.id !== id || state2.screen !== "room") {
            resources.dispose();
            return;
          }
          state2.broadcastResources?.dispose();
          state2.broadcastResources = { ...resources, roomId: id };
        }).catch(() => {
          state2.broadcastError = true;
          state2.error = "\u041D\u0435 \u0443\u0434\u0430\u043B\u043E\u0441\u044C \u043F\u0440\u0438\u043D\u044F\u0442\u044C \u044D\u0444\u0438\u0440. \u041F\u043E\u0432\u0442\u043E\u0440\u0438\u0442\u0435 \u043F\u043E\u0434\u043A\u043B\u044E\u0447\u0435\u043D\u0438\u0435.";
        }).finally(() => {
          state2.broadcastLoading = false;
          handlers2.redraw();
        });
      }
      return;
    }
    FRAMES2 = state2.broadcastResources.frames;
    STORY_VERSION2 = state2.broadcastResources.version;
    resourceAssets = state2.broadcastResources.assets;
  }
  const key = `${STORY_VERSION2}:${view.id}:${view.actorId}`;
  if (active?.key !== key) {
    leaveBroadcast();
    active = createPlayer(key);
  }
  stage.classList.add("broadcast-stage");
  stage.append(active.node);
  active.update(view, state2, handlers2);
}
function createPlayer(key) {
  const saved = load(key), node = el("section", "broadcast-player"), frame = el("div", "broadcast-frame"), art = el("div", "broadcast-art"), media = image(assets.computer, "broadcast-media"), outgoing = image(assets.computer, "broadcast-media-outgoing");
  outgoing.hidden = true;
  node.setAttribute("aria-label", "\u0427\u0451\u0440\u043D\u044B\u0439 \u044D\u0444\u0438\u0440. \u041F\u0440\u0438\u0451\u043C\u043D\u0438\u043A 03");
  const canvas = el("canvas", "broadcast-cosmos"), ctx = canvas.getContext("2d"), pressure = el("div", "tv-pressure");
  const shadow = image("./assets/broadcast/sunflower-shadow.png", "sunflower-shadow"), sun = image("./assets/broadcast/sun-cutout.png", "broadcast-sun");
  const shadowLink = el("a", "sunflower-link");
  shadowLink.href = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSbgBZk-5wUUbzNdLdG-4jm7JVY0kxBfKS9ATU5kHG4GvUOLzs9LhiQoCmmuJb6L-YqCRklpw&s=10";
  shadowLink.target = "_blank";
  shadowLink.rel = "noopener noreferrer";
  shadowLink.setAttribute("aria-label", "\u0424\u0438\u0433\u0443\u0440\u0430 \u0432 \u0442\u0435\u043C\u043D\u043E\u0442\u0435");
  shadowLink.hidden = true;
  const seven = el("span", "broadcast-seven", "7");
  seven.hidden = true;
  const gates = el("div", "broadcast-gates"), closed = image("./assets/sun-gates.png", "gate-closed"), opened = image("./assets/broadcast/gate-open.png", "gate-open-art");
  const reflection = image("./assets/sun-gates.png", "gate-reflection");
  gates.append(closed, opened, reflection);
  const elder = el("div", "broadcast-elder"), rising = image("./assets/broadcast/elder-ascent.png", "elder-ascending");
  elder.append(image("./assets/broadcast/elder-rest.png", "elder-rest"), image("./assets/broadcast/elder-spear.png", "elder-raised"), image("./assets/broadcast/elder-working.png", "elder-working"), rising);
  const mechanisms = el("div", "story-mechanisms");
  mechanisms.setAttribute("aria-hidden", "true");
  mechanisms.dataset.demoStep = "0";
  mechanisms.innerHTML = '<div class="story-levers"><svg viewBox="0 0 220 115"><circle class="seal-rim" cx="110" cy="58" r="43"/><circle class="seal-inner" cx="110" cy="58" r="35"/><path class="seal-etch" d="M110 10V19M110 97V106M62 58H71M149 58H158M76 24L82 30M138 86L144 92M76 92L82 86M138 30L144 24M88 18L92 26M128 90L132 98M70 36L78 40M142 76L150 80M70 80L78 76M142 40L150 36M88 98L92 90M128 26L132 18"/><g class="seal-needle needle-one"><path d="M110 58V26"/><circle cx="110" cy="26" r="3"/></g><g class="seal-needle needle-two"><path d="M110 58V31"/><circle cx="110" cy="31" r="2"/></g><g class="seal-needle needle-three"><path d="M110 58V36"/><circle cx="110" cy="36" r="2"/></g><circle class="seal-hub" cx="110" cy="58" r="5"/><circle class="demo-confirm" cx="110" cy="58" r="38"/></svg></div><svg class="story-mask-path" viewBox="0 0 220 115"><path class="demo-channel" d="M16 90L54 68L78 30L116 55L150 25L190 48L203 90"/><path class="demo-detour" d="M54 68L98 95L150 25"/><path class="demo-trace" d="M16 90L54 68L78 30L116 55L150 25L190 48L203 90" pathLength="100"/><g class="demo-mask" transform="translate(78 30)"><path d="M-10-10Q0-17 10-10L8 8L0 14L-8 8Z"/><path d="M-6-2L-2-4M2-4L6-2M-3 6H3"/></g><g class="demo-mask" transform="translate(150 25)"><path d="M-10-10L10-10L7 10L0 14L-7 10Z"/><path d="M-6-1H-2M2-1H6M0 3V7"/></g></svg>';
  mechanisms.querySelector(".story-mask-path").insertAdjacentHTML("beforeend", '<g class="demo-mask" transform="translate(203 90)"><path d="M-10-10L0-14L10-10L8 7L0 14L-8 7Z"/><path d="M-6-2L-2 1M2 1L6-2M-3 7Q0 4 3 7"/></g>');
  const terminal = terminalPage(() => advance()), text = el("p", "broadcast-text");
  text.dataset.broadcastText = "";
  const loading2 = el("span", "broadcast-loading", "\u041F\u0440\u0438\u0451\u043C \u0438\u0437\u043E\u0431\u0440\u0430\u0436\u0435\u043D\u0438\u044F\u2026");
  for (const decorative of [art, canvas, shadow, sun, gates, elder]) decorative.setAttribute("aria-hidden", "true");
  art.append(outgoing, media, pressure);
  frame.append(canvas, shadow, art, sun, gates, elder, mechanisms, terminal, text, shadowLink, seven, loading2);
  const controls = el("div", "broadcast-controls"), pause = button("\u041F\u0430\u0443\u0437\u0430", () => {
    paused = !paused;
    persist();
    updateControls();
  }, "quiet"), next = button("\u0414\u0430\u043B\u0435\u0435", () => advance(), "quiet");
  const replay = button("\u0421\u043C\u043E\u0442\u0440\u0435\u0442\u044C \u0441\u043D\u0430\u0447\u0430\u043B\u0430", () => {
    index = 0;
    elapsed = 0;
    paused = false;
    setFrame();
  }, "quiet"), ready = button("\u0413\u043E\u0442\u043E\u0432 \u043A \u0438\u0441\u043F\u044B\u0442\u0430\u043D\u0438\u044E", () => handlers2.command("READY_MAIN"), "primary");
  const retry = button("\u041F\u043E\u0432\u0442\u043E\u0440\u0438\u0442\u044C \u0438\u0437\u043E\u0431\u0440\u0430\u0436\u0435\u043D\u0438\u0435", () => requestArt(), "quiet");
  retry.hidden = true;
  const dock = el("div", "broadcast-dock"), waiting = el("p", "broadcast-waiting");
  waiting.setAttribute("role", "status");
  loading2.setAttribute("role", "status");
  controls.append(pause, next, retry, replay, ready);
  dock.append(controls, waiting);
  node.append(frame, dock);
  let index = Number.isInteger(saved.index) && saved.index >= 0 && saved.index < FRAMES2.length ? saved.index : 0, elapsed = Math.max(0, Math.min(1e4, Number(saved.elapsed) || 0)), paused = !!saved.paused;
  let view, state2, handlers2, raf, last = 0, lastSaved = 0, destroyed = false, autoPaused = document.hidden, assetPending = false, assetError = false, assetRequest = 0, visualTime = 0, mediaFade = 1, photons = [];
  let sevenUntil = 0;
  function revealSeven() {
    if (text.dataset.easterEgg === "seven") {
      sevenUntil = visualTime + 2.4;
      seven.hidden = false;
    }
  }
  text.addEventListener("click", revealSeven);
  text.addEventListener("keydown", (event) => {
    if (text.dataset.easterEgg === "seven" && ["Enter", " "].includes(event.key)) {
      event.preventDefault();
      revealSeven();
    }
  });
  const reduce = matchMedia("(prefers-reduced-motion: reduce)"), stars = Array.from({ length: 140 }, (_, i) => ({ x: (i * 73 + 19) % 997 / 997, y: (i * 109 + 41) % 991 / 991, r: i % 9 === 0 ? 1.3 : 0.55, s: i * 0.93 }));
  function fitCaption() {
    frame.style.setProperty("--caption-height", `${Math.ceil(text.getBoundingClientRect().height)}px`);
    fitArtwork();
  }
  function fitArtwork() {
    const ratio = media.naturalWidth / Math.max(1, media.naturalHeight) || 16 / 9, w = frame.clientWidth, h = frame.clientHeight * (w < 600 ? 0.68 : 0.83), width = Math.min(w, h * ratio);
    art.style.width = `${width}px`;
    art.style.height = `${width / ratio}px`;
  }
  let layoutRaf;
  function scheduleLayout() {
    if (layoutRaf) return;
    layoutRaf = requestAnimationFrame(() => {
      layoutRaf = null;
      if (!destroyed) {
        fitCaption();
        node.style.setProperty("--dock-height", `${Math.ceil(dock.getBoundingClientRect().height)}px`);
        updateMotion();
      }
    });
  }
  const captionSize = new ResizeObserver(scheduleLayout);
  captionSize.observe(text);
  captionSize.observe(frame);
  const dockSize = new ResizeObserver(scheduleLayout);
  dockSize.observe(dock);
  function persist() {
    try {
      localStorage.setItem(key, JSON.stringify({ index, elapsed, paused }));
    } catch {
    }
  }
  function updateControls() {
    const end = FRAMES2[index].scene === "end";
    pause.hidden = next.hidden = end;
    replay.hidden = !end;
    pause.textContent = paused ? "\u041F\u0440\u043E\u0434\u043E\u043B\u0436\u0438\u0442\u044C \u044D\u0444\u0438\u0440" : "\u041F\u0430\u0443\u0437\u0430";
    pause.setAttribute("aria-pressed", String(paused));
    next.disabled = !!FRAMES2[index].wait || assetPending || assetError;
    retry.hidden = !assetError;
    loading2.hidden = !assetPending && !assetError;
    frame.classList.toggle("art-pending", assetPending || assetError);
    node.dataset.paused = String(paused || autoPaused);
    document.body.dataset.filmPaused = String(paused || FRAMES2[index].scene === "end");
    loading2.textContent = assetError ? "\u0418\u0437\u043E\u0431\u0440\u0430\u0436\u0435\u043D\u0438\u0435 \u043D\u0435 \u043F\u0440\u0438\u0448\u043B\u043E. \u041F\u043E\u0432\u0442\u043E\u0440\u0438\u0442\u0435 \u043F\u0440\u0438\u0451\u043C." : "\u041F\u0440\u0438\u0451\u043C \u0438\u0437\u043E\u0431\u0440\u0430\u0436\u0435\u043D\u0438\u044F\u2026";
    ready.hidden = !end;
    ready.disabled = !!state2?.busy || !!view?.readyMain.includes(view.actorId);
    ready.textContent = view?.readyMain.includes(view.actorId) ? "\u0416\u0434\u0451\u043C \u043E\u0441\u0442\u0430\u043B\u044C\u043D\u044B\u0445\u2026" : "\u0413\u043E\u0442\u043E\u0432 \u043A \u0438\u0441\u043F\u044B\u0442\u0430\u043D\u0438\u044E";
    waiting.textContent = end && view?.readyMain.includes(view.actorId) ? "\u0412\u044B \u0433\u043E\u0442\u043E\u0432\u044B. \u041D\u0430\u043F\u0430\u0440\u043D\u0438\u043A\u0438 \u0435\u0449\u0451 \u0441\u043C\u043E\u0442\u0440\u044F\u0442 \u044D\u0444\u0438\u0440." : "";
  }
  function requestArt() {
    const scene = FRAMES2[index].scene, base = scene.startsWith("sofa") ? "sofa" : scene.startsWith("computer") || scene === "desk-empty" ? "computer" : scene === "desk-cup" ? "desk-cup" : scene === "desk" ? "desk" : null;
    const required = base ? [assets[base]] : [];
    if (scene === "terminal") required.push(assets.computer);
    if (scene === "sofa-shadow") required.push(shadow.src);
    if (["stars", "sun-approach"].includes(scene)) required.push(sun.src, "./assets/broadcast/elder-rest.png");
    if (scene.startsWith("gate")) required.push(closed.src, scene === "gate-spear" ? "./assets/broadcast/elder-spear.png" : "./assets/broadcast/elder-rest.png");
    if (["gate-open", "gate-ignite", "gate-ascent"].includes(scene)) required.push(opened.src, "./assets/broadcast/elder-spear.png");
    if (scene === "gate-mechanisms") required.push("./assets/broadcast/elder-working.png");
    if (scene === "gate-ascent") required.push(rising.src);
    const token = ++assetRequest;
    assetPending = required.length > 0;
    assetError = false;
    last = 0;
    updateControls();
    Promise.all(required.map((src) => new Promise((resolve, reject) => {
      const img = new Image();
      img.onload = () => img.decode().then(resolve, reject);
      img.onerror = () => reject(new Error("image unavailable"));
      img.src = resourceUrl(src);
    }))).then(() => {
      if (destroyed || token !== assetRequest) return;
      if (base) {
        if (media.dataset.loadedAsset && media.getAttribute("src") !== assets[base]) {
          outgoing.src = media.src;
          outgoing.hidden = false;
          mediaFade = paused || reduce.matches ? 1 : 0;
        }
        media.src = resourceUrl(assets[base]);
        media.dataset.loadedAsset = assets[base];
        fitArtwork();
      }
      if (scene === "gate-ascent") samplePhotons();
      assetPending = false;
      last = 0;
      updateControls();
    }).catch(() => {
      if (destroyed || token !== assetRequest) return;
      assetPending = false;
      assetError = true;
      last = 0;
      updateControls();
    });
  }
  function setFrame() {
    const shot = FRAMES2[index], scene = shot.scene, base = scene.startsWith("sofa") ? "sofa" : scene.startsWith("computer") || scene === "desk-empty" ? "computer" : scene === "desk-cup" ? "desk-cup" : scene === "desk" ? "desk" : null;
    frame.dataset.broadcastFrame = index;
    frame.dataset.broadcastScene = scene;
    frame.classList.toggle("has-art", !!base);
    frame.classList.toggle("has-text", !!shot.text);
    frame.classList.toggle("has-gates", scene.startsWith("gate"));
    text.textContent = shot.text || "";
    text.setAttribute("aria-live", paused ? "off" : "polite");
    const isEgg = shot.text === "\u2014 \u041A\u0442\u043E \u0431\u044B \u044D\u0442\u043E \u043D\u0438 \u0441\u0434\u0435\u043B\u0430\u043B \u2014 \u0442\u044B \u043C\u0435\u043D\u044F \u043F\u043E\u0437\u0430\u0431\u0430\u0432\u0438\u043B.";
    text.dataset.easterEgg = isEgg ? "seven" : "";
    if (isEgg) {
      text.setAttribute("role", "button");
      text.tabIndex = 0;
    } else {
      text.removeAttribute("role");
      text.removeAttribute("tabindex");
    }
    shadowLink.hidden = scene !== "sofa-shadow";
    seven.hidden = true;
    sevenUntil = 0;
    if (!base) {
      outgoing.hidden = true;
      mediaFade = 1;
    }
    fitCaption();
    frame.classList.remove("frame-arrival");
    void frame.offsetHeight;
    frame.classList.add("frame-arrival");
    if (scene === "terminal") terminal.scrollTop = 0;
    requestArt();
    updateMotion();
    persist();
  }
  function advance() {
    if (index >= FRAMES2.length - 1) return;
    index++;
    elapsed = 0;
    last = 0;
    setFrame();
  }
  function onVisibility() {
    autoPaused = document.hidden;
    last = 0;
    updateControls();
    persist();
  }
  function tick(now) {
    if (destroyed) return;
    const delta = last ? Math.min(100, now - last) : 0;
    last = now;
    const shot = FRAMES2[index];
    const pending = assetPending || assetError;
    if (!paused && !autoPaused && !pending) {
      visualTime += delta / 1e3;
      mediaFade = Math.min(1, mediaFade + delta / 1300);
      if (!shot.wait) {
        elapsed += delta;
        if (elapsed >= shot.duration) advance();
      }
    }
    media.style.opacity = String(mediaFade);
    outgoing.style.opacity = String(1 - mediaFade);
    if (mediaFade === 1) outgoing.hidden = true;
    updateMotion();
    if (!autoPaused && ctx) paint(visualTime);
    if (now - lastSaved > 1500) {
      persist();
      lastSaved = now;
    }
    raf = requestAnimationFrame(tick);
  }
  const ease = (x) => x * x * (3 - 2 * x);
  function updateMotion() {
    const shot = FRAMES2[index], scene = shot.scene, p = Math.min(1, elapsed / Math.max(1, shot.duration));
    text.style.visibility = shot.captionMs !== void 0 && elapsed >= shot.captionMs ? "hidden" : "";
    if (sevenUntil && visualTime >= sevenUntil) {
      seven.hidden = true;
      sevenUntil = 0;
    }
    if (scene === "sun-approach") {
      const q = reduce.matches ? 0 : ease(Math.max(0, (p - 0.15) / 0.85)), w = frame.clientWidth, h = frame.clientHeight, size = sun.clientWidth;
      sun.style.transform = `translate(${q * (w * 0.5 - (w * 0.97 - size * 0.5))}px,${q * (h * 0.49 - (h * 0.08 + size * 0.5))}px) scale(${1 + q * 4.2})`;
    } else sun.style.transform = "";
    mechanisms.dataset.demoStep = String(Math.min(5, Math.floor(p * 6)));
    mechanisms.style.setProperty("--demo-progress", p);
    mechanisms.style.setProperty("--trace-progress", Math.max(0, Math.min(1, (p - 0.45) / 0.48)) * 100);
    const photonEnd = shot.duration - 700;
    const dissolve = scene === "gate-ascent" ? Math.max(0, Math.min(1, (elapsed - shot.duration * 0.35) / (photonEnd - shot.duration * 0.35))) : 0;
    frame.dataset.photons = dissolve > 0 && dissolve < 1 && scene === "gate-ascent" ? "active" : "off";
    if (scene === "gate-ascent") {
      const rise = reduce.matches ? 0 : ease(Math.min(1, p / 0.72));
      elder.style.transform = `translateY(${-frame.clientHeight * 0.02 * rise}px)`;
      rising.style.opacity = String(reduce.matches || p >= 0.35 ? 0 : 1);
      rising.style.filter = `brightness(${1 + p * 0.35}) drop-shadow(0 0 5px #fff4d8)`;
    } else {
      elder.style.transform = "";
      rising.style.opacity = "";
      rising.style.filter = "";
      rising.style.maskImage = "";
    }
  }
  function samplePhotons() {
    photons = [];
    if (!rising.naturalWidth) return;
    const c = document.createElement("canvas");
    c.width = 90;
    c.height = 135;
    const cctx = c.getContext("2d", { willReadFrequently: true });
    if (!cctx) return;
    cctx.drawImage(rising, 0, 0, 90, 135);
    try {
      const pixels = cctx.getImageData(0, 0, 90, 135).data;
      for (let y = 0; y < 135; y += 2) for (let x = 0; x < 90; x += 2) {
        const i = (y * 90 + x) * 4;
        if (pixels[i + 3] > 155 && pixels[i] + pixels[i + 1] + pixels[i + 2] > 370) photons.push({ x: x / 90, y: y / 135, delay: (x * 13 + y * 7) % 31 / 100, size: (x + y) % 3 ? 1.1 : 1.7 });
      }
    } catch {
      photons = [];
    }
  }
  function paint(time) {
    const scene = FRAMES2[index].scene, isSky = ["stars", "sun-approach"].includes(scene), isGate = scene.startsWith("gate");
    if (!isSky && !isGate) {
      if (canvas.width) ctx.clearRect(0, 0, canvas.width, canvas.height);
      return;
    }
    const w = Math.round(frame.clientWidth), h = Math.round(frame.clientHeight), dpr = Math.min(devicePixelRatio || 1, 1.5);
    if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) {
      canvas.width = Math.round(w * dpr);
      canvas.height = Math.round(h * dpr);
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    const t = reduce.matches ? 0 : time;
    if (isSky) {
      for (const s of stars) {
        ctx.globalAlpha = 0.25 + 0.65 * (0.5 + 0.5 * Math.sin(t * 0.9 + s.s));
        ctx.fillStyle = "#e7e6dc";
        ctx.beginPath();
        ctx.arc((s.x * w + t * 1.8) % w, s.y * h, s.r, 0, Math.PI * 2);
        ctx.fill();
      }
    } else if (["gate-open", "gate-ignite", "gate-ascent"].includes(scene)) {
      for (let i = 0; i < 38; i++) {
        const y = h * 0.89 - (t * 30 + i * 53) % (h * 0.52), x = w * 0.5 + Math.sin(i * 14.3) * (w * 0.23) * (1 - (h * 0.89 - y) / (h * 0.7));
        ctx.globalAlpha = 0.12 + (0.5 + 0.5 * Math.sin(t + i)) * 0.35;
        ctx.fillStyle = "#ffe4a1";
        ctx.beginPath();
        ctx.arc(x, y, i % 5 === 0 ? 1.6 : 0.7, 0, Math.PI * 2);
        ctx.fill();
      }
    }
    if (scene === "gate-ascent" && !reduce.matches) {
      const duration = FRAMES2[index].duration, phase = Math.max(0, Math.min(1, (elapsed - duration * 0.35) / (duration - 700 - duration * 0.35))), r = elder.getBoundingClientRect(), f = frame.getBoundingClientRect(), ratio = rising.naturalWidth / rising.naturalHeight, iw = Math.min(r.width, r.height * ratio), ih = iw / ratio;
      for (const dot of photons) {
        const age = Math.max(0, Math.min(1, (phase - dot.delay) / (1 - dot.delay)));
        if (!age) continue;
        const x = r.x - f.x + (r.width - iw) / 2 + dot.x * iw, y = r.bottom - f.top - ih + dot.y * ih, q = ease(age);
        ctx.globalAlpha = Math.min(1, age * 20) * (1 - age) * 0.95;
        ctx.fillStyle = "#fffdf0";
        ctx.beginPath();
        ctx.arc(x + (w * 0.5 - x) * q + Math.sin(dot.x * 72 + visualTime * 4) * (1 - q) * 7, y + (h * 0.49 - y) * q, dot.size * (1 - age * 0.55), 0, Math.PI * 2);
        ctx.fill();
      }
    }
    ctx.globalAlpha = 1;
  }
  document.addEventListener("visibilitychange", onVisibility);
  window.addEventListener("pagehide", persist);
  for (const src of Object.values(assets)) new Image().src = resourceUrl(src);
  setFrame();
  raf = requestAnimationFrame(tick);
  return { key, node, update(v, s, h) {
    view = v;
    state2 = s;
    handlers2 = h;
    fitCaption();
    updateControls();
  }, destroy() {
    destroyed = true;
    persist();
    captionSize.disconnect();
    dockSize.disconnect();
    cancelAnimationFrame(raf);
    cancelAnimationFrame(layoutRaf);
    delete document.body.dataset.filmPaused;
    document.removeEventListener("visibilitychange", onVisibility);
    window.removeEventListener("pagehide", persist);
  } };
}

// client/finale.js
var active2;
var loading;
var make = (tag, cls, text) => {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text) e.textContent = text;
  return e;
};
function leaveFinale() {
  loading = null;
  active2?.dispose();
  active2 = null;
}
function renderFinale(stage, v, state2, h) {
  if (active2?.roomId === v.id) {
    stage.append(active2.node);
    return;
  }
  const enter2 = make("button", "primary", "\u0412\u043E\u0439\u0442\u0438 \u0432\u043E \u0432\u0440\u0430\u0442\u0430");
  enter2.type = "button";
  stage.append(enter2);
  enter2.onclick = async () => {
    if (loading) return;
    const ticket = { roomId: v.id };
    loading = ticket;
    enter2.disabled = true;
    enter2.textContent = "\u041F\u043E\u0434\u043A\u043B\u044E\u0447\u0435\u043D\u0438\u0435\u2026";
    let resources;
    try {
      resources = await h.finaleResources(v.id);
      if (loading !== ticket) {
        resources.dispose();
        return;
      }
      const player = await createPassage(v.id, resources, h);
      if (loading !== ticket) {
        player.dispose();
        return;
      }
      active2 = player;
      loading = null;
      h.redraw();
    } catch {
      resources?.dispose();
      if (loading !== ticket) return;
      loading = null;
      enter2.disabled = false;
      enter2.textContent = "\u0412\u043E\u0439\u0442\u0438 \u0432\u043E \u0432\u0440\u0430\u0442\u0430";
      stage.append(make("p", "error", "\u0421\u0438\u0433\u043D\u0430\u043B \u043F\u0440\u0435\u0440\u0432\u0430\u043D. \u041F\u043E\u043F\u0440\u043E\u0431\u0443\u0439\u0442\u0435 \u0441\u043D\u043E\u0432\u0430."));
    }
  };
}
async function createPassage(roomId, resources, h) {
  const node = make("section", "sun-finale"), canvas = make("canvas", "passage-light"), gate = make("img", "passage-gate"), shadow = make("img", "passage-shadow"), caption = make("p", "passage-caption"), dock = make("div", "passage-dock");
  node.setAttribute("aria-label", "\u041F\u0435\u0440\u0435\u0445\u043E\u0434 \u0447\u0435\u0440\u0435\u0437 \u0412\u0440\u0430\u0442\u0430 \u0421\u043E\u043B\u043D\u0446\u0430");
  gate.src = resources.assets["./assets/broadcast/gate-open.png"];
  shadow.src = resources.assets["./assets/broadcast/sunflower-shadow.png"];
  gate.alt = "";
  shadow.alt = "";
  canvas.setAttribute("aria-hidden", "true");
  caption.setAttribute("aria-live", "polite");
  await Promise.all([gate.decode(), shadow.decode()]);
  const shadowLink = resources.sky2 && resources.nextSignal ? make("button", "passage-shadow-link") : null;
  const date = make("p", "passage-date");
  date.setAttribute("aria-live", "polite");
  if (shadowLink) {
    shadowLink.type = "button";
    shadowLink.setAttribute("aria-label", "\u041F\u0440\u0438\u0441\u043C\u043E\u0442\u0440\u0435\u0442\u044C\u0441\u044F");
    node.append(shadowLink, date);
  }
  const pause = make("button", "", "\u041F\u0430\u0443\u0437\u0430"), next = make("button", "", "\u0414\u0430\u043B\u0435\u0435"), exit = make("button", "", "\u0412\u044B\u0439\u0442\u0438");
  for (const b of [pause, next, exit]) b.type = "button";
  dock.append(pause, next, exit);
  node.append(canvas, gate, shadow, caption, dock);
  const ctx = canvas.getContext("2d"), reduce = matchMedia("(prefers-reduced-motion: reduce)");
  let index = 0, elapsed = 0, last = performance.now(), raf, paused = false, disposed = false, dateTimer, logged = false;
  const particles = Array.from({ length: 170 }, (_, i) => ({ x: Math.sin(i * 12.9898) * 0.15, y: i % 19 / 19, phase: i * 0.618 % 1, r: 0.6 + i % 4 * 0.45 }));
  function show() {
    const frame = resources.frames[index];
    node.dataset.finaleScene = frame.scene;
    node.dataset.paused = String(paused);
    caption.textContent = frame.text || "";
    pause.textContent = paused ? "\u041F\u0440\u043E\u0434\u043E\u043B\u0436\u0438\u0442\u044C" : "\u041F\u0430\u0443\u0437\u0430";
    if (shadowLink) {
      shadowLink.disabled = frame.scene !== "warning";
      if (frame.scene !== "warning") {
        clearTimeout(dateTimer);
        date.textContent = "";
      }
      if (frame.scene === "warning" && !logged) {
        logged = true;
        console.info("\u0421\u043B\u0435\u0434\u0443\u044E\u0449\u0438\u0439 \u044D\u0444\u0438\u0440 // " + resources.nextSignal);
      }
    }
  }
  function advance() {
    if (disposed) return;
    if (index + 1 >= resources.frames.length) {
      disposed = true;
      cancelAnimationFrame(raf);
      resources.dispose();
      h.beforeDeparture?.();
      location.assign(resources.destination);
      return;
    }
    index++;
    elapsed = 0;
    last = performance.now();
    show();
  }
  function draw2(now) {
    if (disposed) return;
    const delta = Math.min(100, now - last);
    last = now;
    if (!paused && !document.hidden) elapsed += delta;
    const frame = resources.frames[index], p = Math.min(1, elapsed / frame.duration), rect = node.getBoundingClientRect(), dpr = Math.min(devicePixelRatio || 1, 1.5);
    if (shadowLink) shadowLink.disabled = frame.scene !== "warning" || !reduce.matches && (p < 0.2 || p > 0.82);
    const w = Math.round(rect.width * dpr), hh = Math.round(rect.height * dpr);
    if (canvas.width !== w || canvas.height !== hh) {
      canvas.width = w;
      canvas.height = hh;
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, rect.width, rect.height);
    if (frame.scene === "passage") {
      const t = reduce.matches ? 0.45 : p, finish = 0.83;
      for (const dot of particles) {
        const q = Math.min(1, t * 1.4 + dot.phase * 0.14), x = rect.width * (0.5 + dot.x * (1 - q)), y = rect.height * (0.82 - dot.y * 0.1 - (0.42 - dot.y * 0.1) * q), alpha = Math.min(1, t * 6) * Math.max(0, (finish - t) * 7);
        ctx.fillStyle = `rgba(255,245,207,${alpha})`;
        ctx.shadowColor = "#fff2bd";
        ctx.shadowBlur = 8;
        ctx.beginPath();
        ctx.ellipse(x, y, dot.r, reduce.matches ? dot.r : dot.r + delta * 0.03, 0, 0, Math.PI * 2);
        ctx.fill();
      }
      ctx.shadowBlur = 0;
    }
    if (elapsed >= frame.duration && !paused && !document.hidden) advance();
    if (!disposed) raf = requestAnimationFrame(draw2);
  }
  pause.onclick = () => {
    paused = !paused;
    show();
  };
  next.onclick = advance;
  exit.onclick = () => {
    leaveFinale();
    h.logout();
  };
  if (shadowLink) shadowLink.onclick = () => {
    if (shadowLink.disabled || disposed) return;
    date.textContent = resources.nextSignal;
    clearTimeout(dateTimer);
    dateTimer = setTimeout(() => {
      date.textContent = "";
    }, 4500);
  };
  show();
  raf = requestAnimationFrame(draw2);
  return { node, roomId, dispose() {
    disposed = true;
    clearTimeout(dateTimer);
    cancelAnimationFrame(raf);
    resources.dispose();
  } };
}

// client/view.js
var roleNames = { light: "\u0421\u0432\u0435\u0442", shadow: "\u0422\u0435\u043D\u044C", reflection: "\u041E\u0442\u0440\u0430\u0436\u0435\u043D\u0438\u0435" };
var roles = Object.keys(roleNames);
var focusTarget;
var focusSelection;
function el2(tag, text, cls) {
  const e = document.createElement(tag);
  if (text !== void 0) e.textContent = text;
  if (cls) e.className = cls;
  return e;
}
function button2(text, fn, disabled = false, cls = "") {
  const b = el2("button", text, cls);
  b.type = "button";
  b.disabled = disabled;
  b.dataset.focusKey = text;
  b.addEventListener("click", fn);
  return b;
}
function field(label, type, value, onInput) {
  const l = el2("label"), i = el2("input");
  l.append(el2("span", label));
  i.type = type;
  i.value = value || "";
  i.dataset.focusKey = label;
  i.addEventListener("input", () => onInput?.(i.value));
  l.append(i);
  return { label: l, input: i };
}
function choice(label, options, value, onChange) {
  const l = el2("label"), s = el2("select");
  l.append(el2("span", label));
  s.dataset.focusKey = label;
  for (const [v, name, disabled] of options) {
    const o = el2("option", name);
    o.value = v;
    o.disabled = !!disabled;
    s.append(o);
  }
  s.value = String(value);
  s.addEventListener("change", () => onChange?.(s.value));
  l.append(s);
  return { label: l, input: s };
}
function drawer(id, title, open) {
  const d = el2("details", void 0, "drawer");
  d.dataset.drawer = id;
  d.open = open.has(id);
  const s = el2("summary", title);
  s.setAttribute("role", "button");
  d.append(s);
  return d;
}
function heading(stage, title, objective) {
  stage.append(el2("h1", title));
  if (objective) stage.append(el2("p", objective, "objective"));
}
function ownerOf(v, control) {
  return v.members.find((m) => m.id === v.controlOwners[control - 1])?.name || "\u041D\u0430\u043F\u0430\u0440\u043D\u0438\u043A";
}
function renderView(root2, state2, h) {
  if (state2.screen !== "room" || state2.room?.stage !== "COMPLETE" || !state2.room?.spark?.finished) leaveFinale();
  if (state2.screen === "auth" && state2.broadcastResources) {
    state2.broadcastResources.dispose();
    state2.broadcastResources = null;
    state2.broadcastError = false;
  }
  if (state2.screen !== "room" || state2.room?.stage !== "BROADCAST_STUB") leaveBroadcast();
  void root2.offsetHeight;
  const key = state2.screen === "room" ? state2.room.id + ":" + state2.room.stage : state2.screen, same = root2.dataset.viewKey === key;
  const open = new Set(same ? [...root2.querySelectorAll("details[open]")].map((d) => d.dataset.drawer) : []);
  const journal = root2.querySelector("[data-journal]"), journalState = same ? { scroll: journal?.scrollTop || 0, limit: Number(journal?.dataset.limit) || 20 } : { scroll: 0, limit: 20 };
  if (same && root2.contains(document.activeElement)) {
    const active3 = document.activeElement;
    focusTarget = active3.dataset.focusKey;
    focusSelection = active3 instanceof HTMLInputElement ? { value: active3.value, start: active3.selectionStart, end: active3.selectionEnd } : null;
  }
  if (!same) {
    focusTarget = null;
    focusSelection = null;
  }
  const lastNumber = Number(root2.dataset.lastNumber) || 0;
  root2.dataset.viewKey = key;
  root2.dataset.lastNumber = (state2.room?.spark?.journal || state2.room?.journal || []).at(-1)?.number || 0;
  root2.replaceChildren();
  const screen = el2("section", void 0, "screen " + state2.screen);
  root2.append(screen);
  const strip = el2("div", void 0, "signal-strip");
  strip.append(el2("span", "\u041F\u0420\u0418\u0401\u041C\u041D\u0418\u041A 03"));
  if (state2.screen === "room" && !state2.connected) strip.append(el2("span", "\u0412\u043E\u0441\u0441\u0442\u0430\u043D\u0430\u0432\u043B\u0438\u0432\u0430\u0435\u043C \u0441\u0432\u044F\u0437\u044C\u2026", "connection"));
  if (state2.screen !== "auth" && state2.room?.stage !== "BROADCAST_STUB") strip.append(button2("\u0412\u044B\u0439\u0442\u0438", h.logout, state2.busy, "quiet logout"));
  screen.append(strip);
  const stage = el2("div", void 0, "stage");
  screen.append(stage);
  if (state2.screen === "auth") renderAuth(stage, h, state2);
  else if (state2.screen === "rooms") renderRooms(stage, h, state2);
  else {
    const v = state2.room;
    stage.dataset.stage = v.stage;
    stage.dataset.roomId = v.id;
    const team = el2("div", void 0, "team");
    for (const m of v.members) {
      const participant = el2("span", `${m.connected ? "\u25CF" : "\u25CB"} ${m.name}${m.id === v.actorId ? " \xB7 \u0432\u044B" : ""}`);
      participant.setAttribute("aria-label", `${m.name}${m.id === v.actorId ? ", \u0432\u044B" : ""}: ${m.connected ? "\u0441\u043E\u0435\u0434\u0438\u043D\u0435\u043D\u0438\u0435 \u0430\u043A\u0442\u0438\u0432\u043D\u043E" : "\u0441\u043E\u0435\u0434\u0438\u043D\u0435\u043D\u0438\u0435 \u043D\u0435 \u0443\u0441\u0442\u0430\u043D\u043E\u0432\u043B\u0435\u043D\u043E"}`);
      team.append(participant);
    }
    strip.append(team);
    if (v.stage === "WAITING_TEAM") renderLobby(stage, v, state2, h, open);
    else if (v.stage === "ENTRY") renderEntry(stage, v, state2, h, open);
    else if (v.stage === "MAIN") renderMain(stage, v, state2, h, open);
    else if (v.stage === "SPARK") {
      renderSpark(stage, v, state2, h, open);
      renderHelp(stage, v, h, open);
    } else if (v.stage === "SPARK_READY" || v.canStartSpark) {
      heading(stage, "\u041A\u043E\u043D\u0442\u0443\u0440 \u0432\u043E\u0441\u0441\u0442\u0430\u043D\u043E\u0432\u043B\u0435\u043D", "\u041E\u0441\u0442\u0430\u043B\u0438\u0441\u044C \u0442\u0440\u0438 \u043C\u0430\u0441\u043A\u0438. \u041D\u0430\u0447\u043D\u0451\u043C, \u043A\u043E\u0433\u0434\u0430 \u0432\u0441\u0435 \u0431\u0443\u0434\u0443\u0442 \u0433\u043E\u0442\u043E\u0432\u044B.");
      stage.classList.add("transition-screen");
      stage.append(finishedContour());
      const ready = v.readySpark.includes(v.actorId);
      stage.append(button2(ready ? "\u0416\u0434\u0451\u043C \u043E\u0441\u0442\u0430\u043B\u044C\u043D\u044B\u0445\u2026" : "\u041F\u0440\u043E\u0432\u0435\u0441\u0442\u0438 \u0441\u0432\u0435\u0442", () => h.command("READY_SPARK"), ready || state2.busy, "primary"));
    } else if (v.stage === "OPEN_GATE") {
      heading(stage, "\u0421\u0438\u0433\u043D\u0430\u043B \u043F\u043E\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0451\u043D.");
      stage.classList.add("transition-screen");
      stage.append(button2(v.passedGate.includes(v.actorId) ? "\u0416\u0434\u0451\u043C \u043E\u0441\u0442\u0430\u043B\u044C\u043D\u044B\u0445\u2026" : "\u041D\u0430\u0447\u0430\u0442\u044C \u044D\u0444\u0438\u0440", () => h.command("PASS_GATE"), v.passedGate.includes(v.actorId) || state2.busy, "primary"));
    } else if (v.stage === "BROADCAST_STUB") {
      renderBroadcast(stage, v, state2, h);
    } else {
      heading(stage, "\u0418\u0441\u043F\u044B\u0442\u0430\u043D\u0438\u0435 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043D\u043E.", "\u0421\u0432\u0435\u0442 \u0434\u043E\u0441\u0442\u0438\u0433 \u0446\u0435\u043D\u0442\u0440\u0430. \u0412\u0440\u0430\u0442\u0430 \u043E\u0442\u043A\u0440\u044B\u0442\u044B.");
      stage.classList.add("transition-screen");
      stage.append(finishedContour());
      if (!v.spark && v.journal.length) renderJournal(stage, v, open);
      if (v.spark?.finished) renderFinale(stage, v, state2, h);
    }
    const roomActions = el2("div", void 0, v.stage === "BROADCAST_STUB" ? "broadcast-room-actions" : "room-actions");
    stage.querySelector(".broadcast-room-actions")?.remove();
    roomActions.append(button2("\u041A \u043A\u043E\u043C\u043D\u0430\u0442\u0430\u043C", h.rooms, false, "quiet return"));
    if (v.actorId === v.ownerId) roomActions.append(button2("\u0420\u0430\u0441\u0444\u043E\u0440\u043C\u0438\u0440\u043E\u0432\u0430\u0442\u044C \u043A\u043E\u043C\u0430\u043D\u0434\u0443", h.disband, state2.busy, "quiet return disband"));
    if (v.stage === "BROADCAST_STUB") roomActions.append(button2("\u0412\u044B\u0439\u0442\u0438", h.logout, state2.busy, "quiet logout"));
    (stage.querySelector(".broadcast-dock") || stage).append(roomActions);
  }
  if (state2.notice) {
    const n = el2("p", state2.notice, "status");
    n.setAttribute("role", "status");
    stage.append(n);
  }
  if (state2.error) {
    const e = el2("p", state2.error, "error");
    e.setAttribute("role", "alert");
    stage.append(e);
  }
  const individualFilm = stage.querySelector(".sun-finale");
  if (individualFilm) {
    strip.hidden = true;
    for (const child of stage.children) if (child !== individualFilm) child.hidden = true;
  }
  const nextJournal = root2.querySelector("[data-journal]");
  if (nextJournal) {
    nextJournal.dataset.limit = journalState.limit;
    nextJournal.refresh();
    nextJournal.scrollTop = journalState.scroll;
  }
  const latest = (state2.room?.spark?.journal || state2.room?.journal || []).at(-1), announcer = document.querySelector("#announcer");
  if (announcer && same && latest && latest.number > lastNumber) announcer.textContent = state2.room?.spark ? `${latest.actorName}: ${latest.result === "blocked" ? "\u0440\u0430\u0437\u0440\u044B\u0432, \u0432\u043E\u0437\u0432\u0440\u0430\u0442 \u043A \u043A\u043E\u043D\u0442\u0440\u043E\u043B\u044C\u043D\u043E\u0439 \u0442\u043E\u0447\u043A\u0435, \u043F\u0440\u043E\u0445\u043E\u0434\u044B \u0438\u0437\u043C\u0435\u043D\u0438\u043B\u0438\u0441\u044C" : latest.result === "mask" ? "\u043C\u0430\u0441\u043A\u0430 \u0430\u043A\u0442\u0438\u0432\u0438\u0440\u043E\u0432\u0430\u043D\u0430, \u043D\u043E\u0432\u0430\u044F \u0444\u0430\u0437\u0430" : "\u0441\u0432\u0435\u0442 \u043F\u0435\u0440\u0435\u043C\u0435\u0449\u0451\u043D"}. \u0425\u043E\u0434 ${latest.number}.` : `${latest.actorName}: ${latest.type === "entry" ? "\u0438\u0437\u043C\u0435\u043D\u0435\u043D\u043E \u043C\u0435\u0441\u0442\u043E" : "\u0438\u0437\u043C\u0435\u043D\u0451\u043D \u0440\u044B\u0447\u0430\u0433"} ${latest.control}. \u0417\u0430\u043F\u0438\u0441\u044C ${latest.number} \u0441\u043E\u0445\u0440\u0430\u043D\u0435\u043D\u0430 \u0432 \u0436\u0443\u0440\u043D\u0430\u043B\u0435.`;
  if (focusTarget) {
    const target = [...root2.querySelectorAll("[data-focus-key]")].find((e) => e.dataset.focusKey === focusTarget);
    if (target && !target.disabled) {
      target.focus({ preventScroll: true });
      if (target instanceof HTMLInputElement && focusSelection?.value === target.value && focusSelection.start !== null) target.setSelectionRange(focusSelection.start, focusSelection.end);
    }
    if (!target) {
      focusTarget = null;
      focusSelection = null;
    }
  }
}
function renderAuth(stage, h, state2) {
  const f = field("\u041F\u0430\u0440\u043E\u043B\u044C", "text", "");
  f.input.className = "password-input";
  f.input.inputMode = "text";
  f.input.autocomplete = "off";
  f.input.autocapitalize = "off";
  f.input.setAttribute("autocorrect", "off");
  f.input.spellcheck = false;
  f.input.enterKeyHint = "go";
  f.input.dataset.passwordInput = "";
  const form = el2("form"), enter2 = button2("\u0412\u043E\u0439\u0442\u0438", () => {
  }, state2.busy, "primary");
  enter2.type = "submit";
  form.append(f.label, enter2);
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    h.login(f.input.value);
  });
  stage.append(form);
}
function renderRooms(stage, h, state2) {
  heading(stage, state2.pending ? "\u0416\u0434\u0451\u043C \u043F\u0440\u0438\u0433\u043B\u0430\u0448\u0430\u0432\u0448\u0435\u0433\u043E" : "\u0421 \u043A\u0435\u043C \u0432\u044B \u0438\u0434\u0451\u0442\u0435?", state2.pending ? "\u041E\u043D \u0443\u0432\u0438\u0434\u0438\u0442 \u0432\u0430\u0448\u0435 \u0438\u043C\u044F \u0438 \u043F\u0440\u0438\u043C\u0435\u0442 \u0437\u0430\u044F\u0432\u043A\u0443." : "\u041F\u043E\u0437\u043E\u0432\u0438\u0442\u0435 \u0437\u043D\u0430\u043A\u043E\u043C\u043E\u0433\u043E \u043D\u0430\u043F\u0430\u0440\u043D\u0438\u043A\u0430. \u041E\u0431\u0449\u0430\u0442\u044C\u0441\u044F \u043C\u043E\u0436\u043D\u043E \u0442\u0430\u043C, \u0433\u0434\u0435 \u0432\u0430\u043C \u0443\u0434\u043E\u0431\u043D\u043E.");
  const name = field("\u0412\u0430\u0448\u0435 \u0438\u043C\u044F", "text", state2.name, h.name);
  name.input.minLength = 2;
  name.input.maxLength = 32;
  stage.append(name.label);
  if (!state2.pending) {
    if (state2.inviteToken) stage.append(button2("\u0417\u0430\u043F\u0440\u043E\u0441\u0438\u0442\u044C \u0432\u0445\u043E\u0434", h.join, state2.busy, "primary"));
    else {
      const size = choice("\u0423\u0447\u0430\u0441\u0442\u043D\u0438\u043A\u043E\u0432", [[2, "\u0414\u0432\u043E\u0435"], [3, "\u0422\u0440\u043E\u0435"]], state2.size, h.size);
      stage.append(size.label, button2("\u0421\u043E\u0437\u0434\u0430\u0442\u044C \u043A\u043E\u043C\u0430\u043D\u0434\u0443", h.create, state2.busy, "primary"));
    }
  }
  if (state2.listing?.rooms.length) {
    const list = el2("div", void 0, "previous");
    list.append(el2("h2", "\u0412\u0430\u0448\u0438 \u043A\u043E\u043C\u0430\u043D\u0434\u044B"));
    for (const r of state2.listing.rooms) list.append(button2(`\u041F\u0440\u043E\u0434\u043E\u043B\u0436\u0438\u0442\u044C \xB7 ${r.name} \xB7 ${r.count}/${r.size}`, () => h.enter(r.id), state2.busy, "quiet"));
    stage.append(list);
  }
}
function renderLobby(stage, v, state2, h, open) {
  const full = v.members.length === v.size;
  heading(stage, full ? "\u0412\u0441\u0435 \u043D\u0430 \u043C\u0435\u0441\u0442\u0435" : "\u041F\u043E\u0437\u043E\u0432\u0438\u0442\u0435 \u043D\u0430\u043F\u0430\u0440\u043D\u0438\u043A\u0430", full ? void 0 : "\u041F\u0435\u0440\u0435\u0434\u0430\u0439\u0442\u0435 \u043F\u0440\u0438\u0433\u043B\u0430\u0448\u0435\u043D\u0438\u0435. \u041D\u0430\u043F\u0430\u0440\u043D\u0438\u043A \u0432\u0432\u0435\u0434\u0451\u0442 \u043F\u0430\u0440\u043E\u043B\u044C \u0438 \u043E\u0442\u043F\u0440\u0430\u0432\u0438\u0442 \u0437\u0430\u044F\u0432\u043A\u0443.");
  if (v.actorId === v.ownerId) {
    for (let slot = 1; slot < v.size; slot++) if (!v.members.some((m) => m.slot === slot)) {
      const link = state2.inviteLinks[slot], active3 = v.invites.some((i) => i.slot === slot);
      if (!link || !active3) {
        const b = button2(v.size === 2 ? "\u041F\u0440\u0438\u0433\u043B\u0430\u0441\u0438\u0442\u044C \u043D\u0430\u043F\u0430\u0440\u043D\u0438\u043A\u0430" : `\u041F\u0440\u0438\u0433\u043B\u0430\u0441\u0438\u0442\u044C \u043D\u0430\u043F\u0430\u0440\u043D\u0438\u043A\u0430 ${slot}`, () => h.invite(slot), state2.busy, "primary");
        b.dataset.inviteSlot = slot;
        stage.append(b);
      } else {
        const f = field("\u041F\u0440\u0438\u0433\u043B\u0430\u0448\u0435\u043D\u0438\u0435 " + slot, "text", link);
        f.input.readOnly = true;
        f.input.dataset.inviteLink = slot;
        const row = el2("div", void 0, "invitation");
        row.append(f.label, button2("\u0421\u043A\u043E\u043F\u0438\u0440\u043E\u0432\u0430\u0442\u044C", () => h.copy(link), state2.busy), button2("\u041E\u0442\u043E\u0437\u0432\u0430\u0442\u044C", () => h.revoke(slot), state2.busy, "quiet"));
        stage.append(row);
      }
    }
    for (const q of v.pending) {
      const row = el2("div", void 0, "request");
      row.append(el2("span", q.name), button2("\u041F\u0440\u0438\u043D\u044F\u0442\u044C", () => h.accept(q.id), state2.busy, "primary"), button2("\u041E\u0442\u043A\u043B\u043E\u043D\u0438\u0442\u044C", () => h.reject(q.id), state2.busy, "quiet"));
      stage.append(row);
    }
  }
  if (full) {
    stage.append(button2("\u0413\u043E\u0442\u043E\u0432", () => h.command("READY_TEAM"), state2.busy || v.readyTeam.includes(v.actorId), "primary"));
    if (v.readyTeam.includes(v.actorId)) stage.append(el2("p", "\u0412\u044B \u0433\u043E\u0442\u043E\u0432\u044B. \u0416\u0434\u0451\u043C \u043E\u0441\u0442\u0430\u043B\u044C\u043D\u044B\u0445.", "status"));
  } else stage.append(el2("p", `${v.members.length} \u0438\u0437 ${v.size} \u043D\u0430 \u043C\u0435\u0441\u0442\u0435. \u041D\u0430\u0447\u043D\u0451\u043C, \u043A\u043E\u0433\u0434\u0430 \u0441\u043E\u0431\u0435\u0440\u0443\u0442\u0441\u044F \u0432\u0441\u0435.`, "status"));
  if (full && v.size === 3 && v.actorId === v.ownerId) {
    const d = drawer("roles", "\u0420\u0430\u0441\u043F\u0440\u0435\u0434\u0435\u043B\u0435\u043D\u0438\u0435 \u0440\u043E\u043B\u0435\u0439", open), config = { ...v.assignments.entry };
    for (const role of roles) d.append(choice(roleNames[role], v.members.map((m) => [m.id, m.name]), config[role], (value) => config[role] = value).label);
    const combined = v.members.find((m) => roles.filter((r) => config[r] === m.id).length === 2);
    let retained = roles.find((r) => config[r] === combined?.id && v.assignments.main[r] === combined?.id) || "light";
    if (v.size === 2) d.append(choice("\u0421\u043E\u0445\u0440\u0430\u043D\u0438\u0442\u044C \u043E\u0434\u043D\u0443 \u0440\u043E\u043B\u044C \u043F\u043E\u0441\u043B\u0435 \u0432\u0445\u043E\u0434\u0430", roles.map((r) => [r, roleNames[r]]), retained, (x) => retained = x).label);
    d.append(button2("\u0421\u043E\u0445\u0440\u0430\u043D\u0438\u0442\u044C \u0440\u043E\u043B\u0438", () => h.assign(config, retained), state2.busy));
    stage.append(d);
  }
}
function renderEntry(stage, v, state2, h, open) {
  heading(stage, "\u0421\u043E\u0431\u0435\u0440\u0438\u0442\u0435 \u043F\u0435\u0447\u0430\u0442\u0438", "\u0420\u0430\u0441\u0441\u0442\u0430\u0432\u044C\u0442\u0435 \u0448\u0435\u0441\u0442\u044C \u043F\u0435\u0447\u0430\u0442\u0435\u0439. \u0423 \u043A\u0430\u0436\u0434\u043E\u0433\u043E \u2014 \u0447\u0430\u0441\u0442\u044C \u0437\u0430\u043F\u0438\u0441\u0435\u0439.");
  const notes = el2("section", void 0, "observations");
  notes.dataset.personalClues = "";
  notes.append(el2("h2", "\u0412\u0430\u0448\u0438 \u0437\u0430\u043F\u0438\u0441\u0438"));
  for (const c of v.entryClues) notes.append(el2("p", clueText(c), "clue"));
  stage.append(notes);
  const assembly = el2("div", void 0, "assembly mechanism");
  v.entrySlots.forEach((value, i) => {
    const place = i + 1, own = v.controls.includes(place), card = el2("div", void 0, "seal" + (own ? " own" : ""));
    card.dataset.commonPlace = place;
    card.dataset.value = value ?? "";
    card.append(el2("span", String(place), "number"), glyph(value));
    if (own) {
      const options = [["", value ? "\u0421\u043D\u044F\u0442\u044C" : "\u0412\u044B\u0431\u0440\u0430\u0442\u044C"], ...NAMES.map((n, j) => [j + 1, n, v.entrySlots.includes(j + 1) && value !== j + 1])], f = choice("\u041C\u0435\u0441\u0442\u043E " + place, options, value ?? "", (x) => h.command("SET_ENTRY", { control: place, value: x ? Number(x) : null }));
      f.label.classList.add("seal-choice");
      f.input.dataset.place = place;
      f.input.disabled = state2.busy;
      card.append(f.label, el2("small", "\u0432\u0430\u0448\u0435", "ownership"));
    } else {
      card.append(el2("span", value ? NAMES[value - 1] : "\u2014", "seal-name"), el2("small", ownerOf(v, place), "ownership"));
    }
    assembly.append(card);
  });
  stage.append(assembly);
  confirm(stage, v, state2, h);
  const tools = el2("div", void 0, "tools");
  renderJournal(tools, v, open);
  renderHelp(tools, v, h, open);
  stage.append(tools);
}
function renderMain(stage, v, state2, h, open) {
  heading(stage, "\u041F\u043E\u0433\u0430\u0441\u0438\u0442\u0435 \u0437\u043D\u0430\u043A\u0438", "\u0417\u043D\u0430\u043A\u0438 \u0434\u043E\u043B\u0436\u043D\u044B \u043F\u043E\u0433\u0430\u0441\u043D\u0443\u0442\u044C \u0443 \u0432\u0441\u0435\u0445. \u041C\u0435\u043D\u044F\u0439\u0442\u0435 \u0440\u044B\u0447\u0430\u0433\u0438 \u0438 \u043E\u0431\u0441\u0443\u0436\u0434\u0430\u0439\u0442\u0435, \u0447\u0442\u043E \u0438\u0437\u043C\u0435\u043D\u0438\u043B\u043E\u0441\u044C.");
  const notes = el2("section", void 0, "observations");
  notes.dataset.personalReadings = "";
  notes.append(el2("h2", "\u0412\u044B \u0432\u0438\u0434\u0438\u0442\u0435"));
  const readings = el2("div", void 0, "readings");
  for (const r of v.readings) {
    const line = el2("div", void 0, "reading " + (r.state ? "active" : "quiet"));
    line.append(glyph(r.id), el2("span", `${NAMES[r.id - 1]} \xB7 ${r.state ? "\u0433\u043E\u0440\u0438\u0442" : "\u043F\u043E\u0433\u0430\u0448\u0435\u043D"}`));
    readings.append(line);
  }
  notes.append(readings);
  stage.append(notes);
  const controls = el2("div", void 0, "lever-grid mechanism");
  v.lightPositions.forEach((value, i) => {
    const id = i + 1, own = v.controls.includes(id), card = own ? button2("", () => h.command("SET_LIGHT", { control: id, value: 1 - value }), state2.busy, "lever own") : el2("div", void 0, "lever peer");
    card.dataset.publicControl = id;
    card.dataset.value = value;
    card.append(el2("span", "\u0420\u044B\u0447\u0430\u0433 " + id, "lever-label"), el2("span", value ? "II" : "I", "lever-position"), el2("small", own ? "\u0438\u0437\u043C\u0435\u043D\u0438\u0442\u044C" : ownerOf(v, id), "ownership"));
    if (own) {
      card.dataset.control = id;
      card.dataset.focusKey = "control-" + id;
      card.setAttribute("aria-label", `\u0420\u044B\u0447\u0430\u0433 ${id}, \u043F\u043E\u043B\u043E\u0436\u0435\u043D\u0438\u0435 ${value ? "II" : "I"}`);
      card.setAttribute("aria-pressed", String(!!value));
    }
    controls.append(card);
  });
  stage.append(controls);
  confirm(stage, v, state2, h);
  const tools = el2("div", void 0, "tools");
  renderJournal(tools, v, open);
  renderHelp(tools, v, h, open);
  stage.append(tools);
}
function confirm(stage, v, state2, h) {
  const approved = v.approvals.includes(v.actorId), incomplete = v.stage === "ENTRY" && v.entrySlots.some((x) => x === null);
  const action = button2(approved ? "\u0416\u0434\u0451\u043C \u043D\u0430\u043F\u0430\u0440\u043D\u0438\u043A\u0430\u2026" : "\u041F\u0440\u043E\u0432\u0435\u0440\u0438\u0442\u044C \u0432\u043C\u0435\u0441\u0442\u0435", () => h.command(v.stage === "ENTRY" ? "CONFIRM_ENTRY" : "CONFIRM_MAIN"), state2.busy || approved || incomplete, "primary check");
  stage.append(action);
  const waiting = v.members.filter((m) => !v.approvals.includes(m.id)).map((m) => m.id === v.actorId ? "\u0432\u044B" : m.name).join(", ");
  stage.append(el2("p", approved ? "\u0416\u0434\u0451\u043C \u043F\u043E\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043D\u0438\u044F: " + waiting + "." : incomplete ? "\u0417\u0430\u043F\u043E\u043B\u043D\u0438\u0442\u0435 \u0432\u0441\u0435 \u043C\u0435\u0441\u0442\u0430. \u041F\u043E\u0434\u0441\u0432\u0435\u0447\u0435\u043D\u043D\u044B\u0435 \u2014 \u0432\u0430\u0448\u0438." : v.approvals.length ? "\u0416\u0434\u0451\u043C \u043F\u043E\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043D\u0438\u044F: " + waiting + "." : "\u041F\u0440\u043E\u0432\u0435\u0440\u044F\u0435\u043C \u0442\u043E\u043B\u044C\u043A\u043E \u043F\u043E \u0441\u043E\u0433\u043B\u0430\u0441\u0438\u044E \u0432\u0441\u0435\u0439 \u043A\u043E\u043C\u0430\u043D\u0434\u044B.", "status"));
  if (v.lastCheck === "incorrect") stage.append(el2("p", "\u041F\u043E\u043A\u0430 \u043D\u0435 \u0441\u043E\u0432\u043F\u0430\u043B\u043E. \u0418\u0437\u043C\u0435\u043D\u0438\u0442\u0435 \u0441\u0431\u043E\u0440\u043A\u0443 \u2014 \u043D\u0430\u0431\u043B\u044E\u0434\u0435\u043D\u0438\u044F \u0441\u043E\u0445\u0440\u0430\u043D\u0435\u043D\u044B.", "error"));
}
function renderHelp(stage, v, h, open) {
  const d = drawer("help", "\u041F\u043E\u0434\u0441\u043A\u0430\u0437\u043A\u0438", open);
  d.append(button2(v.hint ? "\u0421\u043B\u0435\u0434\u0443\u044E\u0449\u0430\u044F \u043F\u043E\u0434\u0441\u043A\u0430\u0437\u043A\u0430" : "\u041F\u043E\u0434\u0441\u043A\u0430\u0437\u043A\u0430", () => h.command("HINT"), v.hint?.level >= (v.hintLimit ?? 3)));
  for (const hint of v.hints || [v.hint].filter(Boolean)) d.append(el2("p", hint.text, "hint"));
  stage.append(d);
}
function renderJournal(stage, v, open) {
  const d = drawer("journal", "\u0416\u0443\u0440\u043D\u0430\u043B", open), journal = el2("div", void 0, "journal");
  d.querySelector("summary").setAttribute("aria-label", "\u0416\u0443\u0440\u043D\u0430\u043B");
  if (v.journal.length) {
    const count = el2("span", String(v.journal.length), "journal-count");
    count.setAttribute("aria-hidden", "true");
    d.querySelector("summary").append(count);
  }
  journal.dataset.journal = "";
  journal.dataset.limit = 20;
  journal.tabIndex = 0;
  journal.dataset.focusKey = "journal-scroll";
  journal.setAttribute("role", "region");
  journal.setAttribute("aria-label", v.stage === "ENTRY" ? "\u0418\u0441\u0442\u043E\u0440\u0438\u044F \u0441\u0431\u043E\u0440\u043A\u0438 \u043F\u0435\u0447\u0430\u0442\u0435\u0439" : "\u0418\u0441\u0442\u043E\u0440\u0438\u044F \u043E\u043F\u044B\u0442\u043E\u0432");
  journal.refresh = () => {
    journal.replaceChildren();
    const limit = Number(journal.dataset.limit) || 20;
    for (const j of [...v.journal].reverse().slice(0, limit)) {
      const item = el2("article", void 0, "journal-entry");
      item.dataset.journalEntry = j.number;
      const entry = j.type === "entry", title = el2("div", void 0, "journal-heading");
      title.append(el2("span", `${entry ? "\u0417\u0430\u043F\u0438\u0441\u044C" : "\u041E\u043F\u044B\u0442"} ${j.number}`), el2("span", j.actorName, "journal-author"));
      item.append(title);
      const value = (x) => entry ? x === null ? "\u043F\u0443\u0441\u0442\u043E" : NAMES[x - 1] : x ? "II" : "I";
      item.append(el2("p", `${entry ? "\u041C\u0435\u0441\u0442\u043E" : "\u0420\u044B\u0447\u0430\u0433"} ${j.control ?? "\u2014"}: ${value(j.from)} \u2192 ${value(j.to)}`, "journal-action"));
      if (!entry) {
        const changes = el2("div", void 0, "journal-changes");
        for (const before of j.before) {
          const after = j.after.find((s) => s.id === before.id), changed = after && before.state !== after.state;
          const line = el2("div", void 0, "journal-sign" + (changed ? " changed" : ""));
          line.append(glyph(before.id), el2("span", `${NAMES[before.id - 1]}: ${changed ? before.state ? "\u0433\u043E\u0440\u0438\u0442 \u2192 \u043F\u043E\u0433\u0430\u0448\u0435\u043D" : "\u043F\u043E\u0433\u0430\u0448\u0435\u043D \u2192 \u0433\u043E\u0440\u0438\u0442" : "\u0431\u0435\u0437 \u0438\u0437\u043C\u0435\u043D\u0435\u043D\u0438\u0439 (" + (before.state ? "\u0433\u043E\u0440\u0438\u0442" : "\u043F\u043E\u0433\u0430\u0448\u0435\u043D") + ")"}`));
          changes.append(line);
        }
        item.append(changes);
      }
      journal.append(item);
    }
    if (!v.journal.length) journal.append(el2("p", v.stage === "ENTRY" ? "\u0418\u0437\u043C\u0435\u043D\u0438\u0442\u0435 \u043F\u0435\u0447\u0430\u0442\u044C \u2014 \u0437\u0434\u0435\u0441\u044C \u0441\u043E\u0445\u0440\u0430\u043D\u0438\u0442\u0441\u044F \u043C\u0435\u0441\u0442\u043E, \u0437\u043D\u0430\u043A \u0438 \u0438\u043C\u044F \u0438\u0433\u0440\u043E\u043A\u0430." : "\u0418\u0437\u043C\u0435\u043D\u0438\u0442\u0435 \u0440\u044B\u0447\u0430\u0433 \u2014 \u0437\u0434\u0435\u0441\u044C \u0441\u043E\u0445\u0440\u0430\u043D\u044F\u0442\u0441\u044F \u0442\u043E\u043B\u044C\u043A\u043E \u0432\u0430\u0448\u0438 \u043D\u0430\u0431\u043B\u044E\u0434\u0435\u043D\u0438\u044F.", "journal-empty"));
    if (v.journal.length > limit) {
      const more = button2("\u0415\u0449\u0451 \u0437\u0430\u043F\u0438\u0441\u0438", () => {
        const scroll = journal.scrollTop;
        journal.dataset.limit = limit + 20;
        journal.refresh();
        journal.scrollTop = scroll;
      }, false, "quiet journal-more");
      more.dataset.focusKey = "journal-more";
      journal.append(more);
    }
  };
  journal.refresh();
  d.append(journal);
  stage.append(d);
}

// client/effects.js
function attachEffects(root2, { reducedMotion = matchMedia("(prefers-reduced-motion: reduce)") } = {}) {
  let previous, timer;
  function update(view) {
    root2.classList.toggle("gate-open", !!view && ["MAIN", "SPARK_READY", "SPARK", "COMPLETE"].includes(view.stage));
    if (view && previous && view.id === previous.id && ["MAIN", "SPARK"].includes(view.stage) && view.draftVersion !== previous.draftVersion && !reducedMotion.matches) {
      root2.classList.remove("probe-flare", "phase-flare");
      void root2.offsetWidth;
      root2.classList.add(view.spark && previous.spark && (view.spark.window.phase !== previous.spark.window.phase || view.spark.window.layoutRevision !== previous.spark.window.layoutRevision) ? "phase-flare" : "probe-flare");
      clearTimeout(timer);
      timer = setTimeout(() => root2.classList.remove("probe-flare", "phase-flare"), 450);
    }
    if (!view) root2.classList.remove("probe-flare", "phase-flare");
    previous = view;
  }
  return { update, destroy() {
    clearTimeout(timer);
    root2.classList.remove("probe-flare", "phase-flare", "gate-open");
  } };
}

// client/interruption.js
function createInterruptions(root2) {
  let context, buffer, active3 = null;
  function prepare() {
    if (context) return void context.resume().catch(() => {
    });
    const Audio = window.AudioContext || window.webkitAudioContext;
    if (!Audio) return;
    try {
      context = new Audio();
      buffer = context.createBuffer(1, Math.ceil(context.sampleRate * 1.25), context.sampleRate);
      const samples = buffer.getChannelData(0);
      let seed = 29521, low = 0;
      for (let i = 0; i < samples.length; i++) {
        seed = 1664525 * seed + 1013904223 >>> 0;
        const noise = seed / 2147483648 - 1, t = i / context.sampleRate;
        low += 0.08 * (noise - low);
        const attack = Math.min(1, t / 8e-3), tail = Math.exp(-t * 4.8);
        const thud = Math.sin(2 * Math.PI * (72 * t - 18 * t * t));
        samples[i] = Math.tanh((low * 3 + noise * 0.24 + thud * 0.7) * attack * tail);
      }
      void context.resume().catch(() => {
      });
    } catch {
      context = null;
      buffer = null;
    }
  }
  function impact() {
    if (!context || !buffer || context.state !== "running") return null;
    const source = context.createBufferSource(), gain = context.createGain();
    source.buffer = buffer;
    gain.gain.value = 0.24;
    source.connect(gain).connect(context.destination);
    source.start();
    source.onended = () => {
      source.disconnect();
      gain.disconnect();
    };
    return source;
  }
  function show(kind) {
    if (active3 || !["scare", "video"].includes(kind) || document.hidden) return Promise.resolve();
    return new Promise((resolve) => {
      const overlay = document.createElement("div");
      overlay.className = "interruption";
      overlay.dataset.interruption = kind;
      overlay.setAttribute("role", "dialog");
      overlay.setAttribute("aria-modal", "true");
      overlay.setAttribute("aria-label", "\u041F\u0440\u0435\u0440\u044B\u0432\u0430\u043D\u0438\u0435 \u0441\u0438\u0433\u043D\u0430\u043B\u0430");
      const close = document.createElement("button");
      close.className = "interruption-close";
      close.textContent = "\u0417\u0430\u043A\u0440\u044B\u0442\u044C";
      close.type = "button";
      const oldInert = root2.inert, previousFocus = document.activeElement;
      root2.inert = true;
      document.body.classList.add("interruption-open");
      let timer, deadline, source, media, finished = false;
      function finish() {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        clearTimeout(deadline);
        try {
          source?.stop();
        } catch {
        }
        if (media instanceof HTMLVideoElement) {
          media.pause();
          media.removeAttribute("src");
          media.load();
        }
        overlay.remove();
        root2.inert = oldInert;
        document.body.classList.remove("interruption-open");
        document.removeEventListener("keydown", keys);
        document.removeEventListener("visibilitychange", hidden);
        active3 = null;
        if (previousFocus?.isConnected) previousFocus.focus({ preventScroll: true });
        resolve();
      }
      function keys(e) {
        if (e.key === "Escape") {
          e.preventDefault();
          finish();
        }
        if (e.key === "Tab") {
          const buttons = [...overlay.querySelectorAll("button:not([hidden])")];
          if (buttons.length) {
            e.preventDefault();
            const i = buttons.indexOf(document.activeElement);
            buttons[(i + (e.shiftKey ? -1 : 1) + buttons.length) % buttons.length].focus();
          }
        }
      }
      function hidden() {
        if (document.hidden) finish();
      }
      close.addEventListener("click", finish);
      document.addEventListener("keydown", keys);
      document.addEventListener("visibilitychange", hidden);
      active3 = finish;
      overlay.append(close);
      document.body.append(overlay);
      close.focus({ preventScroll: true });
      if (kind === "scare") {
        deadline = setTimeout(finish, 15e3);
        media = new Image();
        media.alt = "";
        media.className = "interruption-art";
        media.onload = () => {
          if (finished) return;
          clearTimeout(deadline);
          timer = setTimeout(() => {
            if (finished) return;
            overlay.classList.add("is-active");
            source = impact();
            timer = setTimeout(finish, 1750);
          }, 140);
        };
        media.onerror = finish;
        overlay.prepend(media);
        media.src = "./assets/interruption.png";
      } else {
        let failure = function() {
          if (finished) return;
          clearTimeout(deadline);
          failed = true;
          media.controls = false;
          play.hidden = false;
          play.textContent = "\u041F\u043E\u0432\u0442\u043E\u0440\u0438\u0442\u044C \u0432\u0438\u0434\u0435\u043E";
          status.textContent = "\u0412\u0438\u0434\u0435\u043E \u043D\u0435 \u0437\u0430\u0433\u0440\u0443\u0437\u0438\u043B\u043E\u0441\u044C. \u041C\u043E\u0436\u043D\u043E \u043F\u043E\u0432\u0442\u043E\u0440\u0438\u0442\u044C \u0438\u043B\u0438 \u043E\u0442\u043A\u0440\u044B\u0442\u044C \u043E\u0442\u0434\u0435\u043B\u044C\u043D\u043E.";
          link.hidden = false;
        }, watchLoading = function() {
          clearTimeout(deadline);
          deadline = setTimeout(failure, 15e3);
        }, start = function() {
          if (finished) return;
          if (failed) {
            failed = false;
            play.textContent = "\u0412\u043E\u0441\u043F\u0440\u043E\u0438\u0437\u0432\u0435\u0441\u0442\u0438";
            status.textContent = "\u041F\u0440\u0438\u0451\u043C \u0432\u0438\u0434\u0435\u043E\u2026";
            link.hidden = true;
            media.load();
            watchLoading();
          }
          media.play().catch((e) => {
            if (finished) return;
            if (e.name === "NotAllowedError") {
              play.hidden = false;
              status.textContent = "";
            } else if (media.error) failure();
            else play.hidden = false;
          });
        };
        const stage = document.createElement("div");
        stage.className = "interruption-video-stage";
        media = document.createElement("video");
        media.setAttribute("playsinline", "");
        media.setAttribute("webkit-playsinline", "");
        media.preload = "auto";
        media.volume = 0.65;
        media.poster = "./assets/interruption-poster.jpg";
        const play = document.createElement("button");
        play.type = "button";
        play.className = "interruption-play";
        play.textContent = "\u0412\u043E\u0441\u043F\u0440\u043E\u0438\u0437\u0432\u0435\u0441\u0442\u0438";
        const status = document.createElement("p");
        status.className = "interruption-video-status";
        status.setAttribute("role", "status");
        status.textContent = "\u041F\u0440\u0438\u0451\u043C \u0432\u0438\u0434\u0435\u043E\u2026";
        const link = document.createElement("a");
        link.className = "interruption-video-link";
        link.textContent = "\u041E\u0442\u043A\u0440\u044B\u0442\u044C \u0432\u0438\u0434\u0435\u043E \u043E\u0442\u0434\u0435\u043B\u044C\u043D\u043E";
        link.href = "./assets/interruption.mp4";
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.hidden = true;
        let failed = false;
        media.addEventListener("playing", () => {
          if (finished) return;
          clearTimeout(deadline);
          failed = false;
          play.hidden = true;
          status.textContent = "";
          link.hidden = true;
          media.controls = true;
        });
        media.addEventListener("ended", finish);
        media.addEventListener("error", failure);
        media.addEventListener("loadeddata", () => {
          if (!finished) {
            clearTimeout(deadline);
            if (!failed) status.textContent = "";
          }
        });
        media.addEventListener("waiting", () => {
          if (!finished) {
            status.textContent = "\u041F\u0440\u0438\u0451\u043C \u0432\u0438\u0434\u0435\u043E\u2026";
            watchLoading();
          }
        });
        play.addEventListener("click", start);
        stage.append(media, play);
        overlay.prepend(stage);
        overlay.append(status, link);
        media.src = "./assets/interruption.mp4";
        watchLoading();
        start();
      }
    });
  }
  return { prepare, show };
}

// client/config.js
var API_BASE = "https://receiver-03-api.enstainmoris.workers.dev";

// client/session-token.js
function createTokenStore(storage, key = "receiver03-session-v1") {
  let memory = null, persistent = true;
  return {
    get() {
      try {
        if (storage) memory = storage.getItem(key);
        else persistent = false;
      } catch {
        persistent = false;
      }
      return memory;
    },
    set(value) {
      memory = value;
      try {
        storage?.setItem(key, value);
      } catch {
        persistent = false;
      }
    },
    clear() {
      memory = null;
      try {
        storage?.removeItem(key);
      } catch {
        persistent = false;
      }
    },
    get persistent() {
      return persistent;
    },
    key
  };
}

// client/broadcast-resources.js
async function getBroadcastResources(roomId, api2, finale = false) {
  const data = await (finale ? api2.getFinale(roomId) : api2.getBroadcast(roomId)), assets2 = {}, urls = [];
  if (!Array.isArray(data.frames) || !Array.isArray(data.assets) || typeof data.version !== "string") throw new Error("CONNECTION");
  try {
    await Promise.all(data.assets.map(async (name) => {
      if (!/^[a-z-]+\.(jpg|png)$/.test(name)) throw new Error("CONNECTION");
      const response = await api2.getBroadcastAsset(roomId, name);
      if (!response.ok) throw new Error("CONNECTION");
      const blob = await response.blob();
      if (!blob.type.startsWith("image/")) throw new Error("CONNECTION");
      const url = URL.createObjectURL(blob);
      urls.push(url);
      assets2["./assets/broadcast/" + name] = url;
    }));
    return { ...data, assets: assets2, dispose() {
      for (const url of urls) URL.revokeObjectURL(url);
    } };
  } catch (e) {
    for (const url of urls) URL.revokeObjectURL(url);
    throw e;
  }
}

// client/app.js
var root = document.querySelector("#app");
var state = { screen: "auth", name: "", size: 2, room: null, listing: null, inviteToken: new URLSearchParams(location.hash.slice(1)).get("invite"), inviteLinks: {}, pending: null, connected: false, busy: false, error: "", notice: "" };
var pendingTimer;
var copySelection;
var messages = { INVALID_INPUT: "\u041F\u0440\u043E\u0432\u0435\u0440\u044C\u0442\u0435 \u0438\u043C\u044F, \u0440\u043E\u043B\u0438 \u0438 \u0432\u044B\u0431\u0440\u0430\u043D\u043D\u044B\u0435 \u043F\u0435\u0447\u0430\u0442\u0438. \u041F\u0435\u0447\u0430\u0442\u044C \u043C\u043E\u0436\u0435\u0442 \u0437\u0430\u043D\u0438\u043C\u0430\u0442\u044C \u0442\u043E\u043B\u044C\u043A\u043E \u043E\u0434\u043D\u043E \u043C\u0435\u0441\u0442\u043E.", CONFLICT: "\u0421\u043E\u0441\u0442\u043E\u044F\u043D\u0438\u0435 \u0443\u0436\u0435 \u0438\u0437\u043C\u0435\u043D\u0438\u043B\u043E\u0441\u044C. \u0421\u043D\u0438\u043C\u043E\u043A \u043E\u0431\u043D\u043E\u0432\u043B\u0451\u043D \u2014 \u043F\u043E\u0432\u0442\u043E\u0440\u0438\u0442\u0435 \u043D\u0443\u0436\u043D\u043E\u0435 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u0435.", FORBIDDEN: "\u0414\u043B\u044F \u044D\u0442\u043E\u0433\u043E \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u044F \u043D\u0435\u0442 \u0434\u043E\u0441\u0442\u0443\u043F\u0430 \u0438\u043B\u0438 \u043D\u0443\u0436\u043D\u043E\u0433\u043E \u044D\u0442\u0430\u043F\u0430.", UNAUTHORIZED: "\u0421\u0435\u0441\u0441\u0438\u044F \u0437\u0430\u043A\u043E\u043D\u0447\u0438\u043B\u0430\u0441\u044C. \u0412\u0432\u0435\u0434\u0438\u0442\u0435 \u043F\u0430\u0440\u043E\u043B\u044C \u0441\u043D\u043E\u0432\u0430.", EXPIRED: "\u0421\u0440\u043E\u043A \u043A\u043E\u043C\u043D\u0430\u0442\u044B \u0438\u043B\u0438 \u043F\u0440\u0438\u0433\u043B\u0430\u0448\u0435\u043D\u0438\u044F \u0438\u0441\u0442\u0451\u043A.", UNAVAILABLE: "\u0421\u0438\u0433\u043D\u0430\u043B \u0432\u0440\u0435\u043C\u0435\u043D\u043D\u043E \u043D\u0435\u0434\u043E\u0441\u0442\u0443\u043F\u0435\u043D. \u041F\u0440\u043E\u0433\u0440\u0435\u0441\u0441 \u043D\u0435 \u0441\u0431\u0440\u043E\u0448\u0435\u043D.", CONNECTION: "\u0421\u043E\u0435\u0434\u0438\u043D\u0435\u043D\u0438\u0435 \u043F\u0440\u0435\u0440\u0432\u0430\u043D\u043E. \u041F\u043E\u0441\u043B\u0435\u0434\u043D\u0435\u0435 \u0441\u043E\u0445\u0440\u0430\u043D\u0451\u043D\u043D\u043E\u0435 \u0441\u043E\u0441\u0442\u043E\u044F\u043D\u0438\u0435 \u043E\u0441\u0442\u0430\u0451\u0442\u0441\u044F \u0434\u0435\u0439\u0441\u0442\u0432\u0443\u044E\u0449\u0438\u043C." };
var effects = attachEffects(document.body);
messages.RATE_LIMITED = "\u0421\u043B\u0438\u0448\u043A\u043E\u043C \u0447\u0430\u0441\u0442\u044B\u0435 \u0437\u0430\u043F\u0440\u043E\u0441\u044B. \u041F\u043E\u0434\u043E\u0436\u0434\u0438\u0442\u0435 \u043C\u0438\u043D\u0443\u0442\u0443 \u0438 \u043F\u043E\u043F\u0440\u043E\u0431\u0443\u0439\u0442\u0435 \u0441\u043D\u043E\u0432\u0430. \u041F\u0440\u043E\u0433\u0440\u0435\u0441\u0441 \u0441\u043E\u0445\u0440\u0430\u043D\u0451\u043D.";
var interruptions = createInterruptions(root);
function draw() {
  renderView(root, state, handlers);
  document.body.dataset.stage = state.room?.stage || state.screen;
  effects.update(state.room);
  if (copySelection) {
    const input = [...root.querySelectorAll("[data-invite-link]")].find((e) => e.value === copySelection);
    if (input) {
      input.focus();
      input.select();
    }
    copySelection = null;
  }
}
async function expired() {
  api.close();
  clearTimeout(pendingTimer);
  state.room = null;
  state.pending = null;
  state.inviteLinks = {};
  state.connected = false;
  await api.session().catch(() => {
  });
  state.screen = "auth";
  state.error = messages.UNAUTHORIZED;
  draw();
}
function mergeRoom(v) {
  if (state.screen === "room" && state.room && v.id === state.room.id && v.revision >= state.room.revision) state.room = v;
}
async function roomClosed(id) {
  if (state.room?.id !== id) return;
  api.close();
  clearTimeout(pendingTimer);
  state.room = null;
  state.pending = null;
  state.inviteLinks = {};
  state.inviteToken = null;
  state.connected = false;
  state.screen = "rooms";
  state.listing = null;
  state.error = "";
  state.notice = "\u041A\u043E\u043C\u0430\u043D\u0434\u0430 \u0440\u0430\u0441\u0444\u043E\u0440\u043C\u0438\u0440\u043E\u0432\u0430\u043D\u0430. \u041C\u043E\u0436\u043D\u043E \u0441\u043E\u0437\u0434\u0430\u0442\u044C \u043D\u043E\u0432\u0443\u044E.";
  draw();
  try {
    state.listing = await api.listRooms();
  } catch (e) {
    state.error = messages[e.message] || "\u041D\u0435 \u0443\u0434\u0430\u043B\u043E\u0441\u044C \u043E\u0431\u043D\u043E\u0432\u0438\u0442\u044C \u0441\u043F\u0438\u0441\u043E\u043A \u043A\u043E\u043C\u0430\u043D\u0434.";
  }
  draw();
}
state.privateBroadcast = !!API_BASE;
var browserStorage;
try {
  browserStorage = localStorage;
} catch {
}
var api = createApi({ onSnapshot: (v) => {
  if (state.screen === "room") {
    mergeRoom(v);
    draw();
  }
}, onConnection: (yes) => {
  state.connected = yes;
  if (state.screen === "room") draw();
}, onExpired: expired, onRoomClosed: roomClosed }, { baseUrl: API_BASE, tokenStore: createTokenStore(browserStorage) });
async function run(fn) {
  if (state.busy) return;
  state.busy = true;
  root.setAttribute("aria-busy", "true");
  state.error = "";
  state.notice = "";
  if (state.screen === "auth") {
    const submit = root.querySelector("form button[type=submit]");
    if (submit) {
      submit.disabled = true;
      submit.textContent = "\u041F\u0440\u043E\u0432\u0435\u0440\u044F\u0435\u043C\u2026";
    }
  }
  try {
    await fn();
  } catch (e) {
    if (e.message === "ROOM_CLOSED") await roomClosed(state.room?.id);
    else if (e.message === "UNAUTHORIZED" && state.screen === "auth") {
      if (e.effect) await interruptions.show(e.effect);
      state.error = "\u041D\u0435\u0432\u0435\u0440\u043D\u044B\u0439 \u043F\u0430\u0440\u043E\u043B\u044C.";
    } else {
      state.error = e.message === "INVALID_INPUT" && state.screen === "rooms" ? "\u0418\u043C\u044F \u0434\u043E\u043B\u0436\u043D\u043E \u0441\u043E\u0434\u0435\u0440\u0436\u0430\u0442\u044C \u043E\u0442 2 \u0434\u043E 32 \u0441\u0438\u043C\u0432\u043E\u043B\u043E\u0432." : messages[e.message] || "\u041D\u0435 \u0443\u0434\u0430\u043B\u043E\u0441\u044C \u0432\u044B\u043F\u043E\u043B\u043D\u0438\u0442\u044C \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u0435.";
      if (e.message === "CONFLICT" && state.room) mergeRoom(await api.getRoom(state.room.id).catch(() => state.room));
      if (e.message === "UNAUTHORIZED") await expired();
    }
  } finally {
    state.busy = false;
    root.setAttribute("aria-busy", "false");
    draw();
  }
}
async function rooms() {
  api.close();
  clearTimeout(pendingTimer);
  state.screen = "rooms";
  state.room = null;
  state.listing = await api.listRooms();
  if (state.pending) pendingTimer = setTimeout(checkPending, 800);
  draw();
}
async function enter(id) {
  clearTimeout(pendingTimer);
  state.room = await api.getRoom(id);
  state.screen = "room";
  state.notice = "";
  state.inviteLinks = {};
  api.connect(id);
  draw();
}
async function checkPending() {
  try {
    const listing = await api.listRooms(), match = listing.rooms.find((r) => r.id === state.pending?.roomId);
    if (match) {
      state.pending = null;
      await enter(match.id);
      return;
    }
    if (!listing.requests.some((q) => q.id === state.pending?.requestId)) {
      state.pending = null;
      state.error = "\u0417\u0430\u044F\u0432\u043A\u0430 \u043E\u0442\u043A\u043B\u043E\u043D\u0435\u043D\u0430 \u0438\u043B\u0438 \u043F\u0440\u0438\u0433\u043B\u0430\u0448\u0435\u043D\u0438\u0435 \u0438\u0441\u0442\u0435\u043A\u043B\u043E.";
      draw();
      return;
    }
  } catch {
  }
  pendingTimer = setTimeout(checkPending, 1500);
}
var handlers = {
  redraw: draw,
  beforeDeparture: () => {
    api.close();
    clearTimeout(pendingTimer);
  },
  finaleResources: (id) => getBroadcastResources(id, api, true),
  broadcastResources: (id) => getBroadcastResources(id, api),
  name: (x) => state.name = x,
  size: (x) => state.size = Number(x),
  login: (p) => {
    if (state.busy) return;
    interruptions.prepare();
    return run(async () => {
      await api.authenticate(p);
      await rooms();
    });
  },
  rooms: () => run(rooms),
  enter: (id) => run(() => enter(id)),
  create: () => run(async () => {
    const r = await api.createRoom(state.size, state.name);
    await enter(r.id);
  }),
  join: () => run(async () => {
    state.pending = await api.join(state.inviteToken, state.name);
    state.inviteToken = null;
    history.replaceState(null, "", location.pathname);
    pendingTimer = setTimeout(checkPending, 800);
  }),
  logout: () => run(async () => {
    api.close();
    clearTimeout(pendingTimer);
    try {
      await api.logout();
    } catch (e) {
      if (state.room && state.screen === "room") api.connect(state.room.id);
      else if (state.pending) pendingTimer = setTimeout(checkPending, 800);
      throw e;
    }
    state.room = null;
    state.listing = null;
    state.inviteLinks = {};
    state.connected = false;
    state.screen = "auth";
    draw();
    root.querySelector("[data-password-input]")?.focus({ preventScroll: true });
  }),
  invite: (slot) => run(async () => {
    const q = await api.invite(state.room.id, slot);
    if (state.room) state.inviteLinks[slot] = location.origin + location.pathname + "#invite=" + encodeURIComponent(q.token);
  }),
  copy: (link) => run(async () => {
    try {
      await navigator.clipboard.writeText(link);
      state.notice = "\u041F\u0440\u0438\u0433\u043B\u0430\u0448\u0435\u043D\u0438\u0435 \u0441\u043A\u043E\u043F\u0438\u0440\u043E\u0432\u0430\u043D\u043E.";
    } catch {
      state.notice = "\u0421\u0441\u044B\u043B\u043A\u0430 \u0432\u044B\u0434\u0435\u043B\u0435\u043D\u0430 \u2014 \u0441\u043A\u043E\u043F\u0438\u0440\u0443\u0439\u0442\u0435 \u0435\u0451 \u0432\u0440\u0443\u0447\u043D\u0443\u044E.";
      copySelection = link;
    }
  }),
  revoke: (slot) => run(async () => {
    const id = state.room.id;
    await api.revoke(id, slot);
    delete state.inviteLinks[slot];
    mergeRoom(await api.getRoom(id));
  }),
  accept: (id) => run(async () => mergeRoom(await api.accept(state.room.id, id))),
  reject: (id) => run(async () => mergeRoom(await api.reject(state.room.id, id))),
  assign: (owners, retainedRole) => handlers.command("SET_ASSIGNMENTS", { owners, retainedRole }),
  disband: () => {
    if (state.busy || !state.room || state.room.actorId !== state.room.ownerId) return;
    const id = state.room.id;
    if (window.confirm("\u0420\u0430\u0441\u0444\u043E\u0440\u043C\u0438\u0440\u043E\u0432\u0430\u0442\u044C \u043A\u043E\u043C\u0430\u043D\u0434\u0443? \u0412\u0441\u0435 \u0443\u0447\u0430\u0441\u0442\u043D\u0438\u043A\u0438 \u0432\u044B\u0439\u0434\u0443\u0442 \u0438\u0437 \u043A\u043E\u043C\u043D\u0430\u0442\u044B, \u0430 \u043F\u0440\u0438\u0433\u043B\u0430\u0448\u0435\u043D\u0438\u044F \u043F\u0435\u0440\u0435\u0441\u0442\u0430\u043D\u0443\u0442 \u0440\u0430\u0431\u043E\u0442\u0430\u0442\u044C.")) return run(async () => {
      await api.disband(id);
      await roomClosed(id);
    });
  },
  command: (type, payload = {}) => run(async () => {
    const c = { id: crypto.randomUUID(), type, payload, expectedDraftVersion: state.room.draftVersion };
    mergeRoom(await api.sendCommand(state.room.id, c));
  })
};
window.addEventListener("hashchange", () => {
  state.inviteToken = new URLSearchParams(location.hash.slice(1)).get("invite");
  if (state.inviteToken && state.screen !== "auth") run(rooms);
});
window.addEventListener("pagehide", handlers.beforeDeparture);
window.addEventListener("pageshow", (e) => {
  if (!e.persisted) return;
  if (state.screen === "room" && state.room) api.connect(state.room.id);
  else if (state.pending) pendingTimer = setTimeout(checkPending, 800);
});
await run(async () => {
  const session = await api.session();
  if (session.authorized) await rooms();
});
