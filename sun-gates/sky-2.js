// client/event-stream.js
async function readEventStream(response, { signal, onEvent, onActivity }) {
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
    const i = value.indexOf(":"), field = i < 0 ? value : value.slice(0, i), raw = i < 0 ? "" : value.slice(i + 1), text = raw.startsWith(" ") ? raw.slice(1) : raw;
    if (field === "data") data.push(text);
    else if (field === "event") type = text;
  }
  try {
    while (!signal?.aborted) {
      const { value, done } = await reader.read();
      if (!done && value?.length) onActivity?.();
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
    const streamController = new AbortController();
    let idleTimer;
    const cancelStream = () => {
      clearTimeout(idleTimer);
      streamController.abort();
    };
    signal.addEventListener("abort", cancelStream, { once: true });
    const activity = () => {
      clearTimeout(idleTimer);
      if (!signal.aborted) idleTimer = setTimeout(() => streamController.abort(), 45e3);
    };
    activity();
    try {
      const response = await raw(`/api/rooms/${id}/events`, { signal: streamController.signal });
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
      await readEventStream(response, { signal: streamController.signal, onActivity: activity, onEvent: (e) => {
        if (generation !== serial || signal.aborted || streamController.signal.aborted) return;
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
    } finally {
      clearTimeout(idleTimer);
      signal.removeEventListener("abort", cancelStream);
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

// client/config.js
var API_BASE = "https://receiver-03-api.enstainmoris.workers.dev";

// client/session-token.js
function createTokenStore(storage2, key = "receiver03-session-v1") {
  let memory = null, persistent = true;
  return {
    get() {
      try {
        if (storage2) memory = storage2.getItem(key);
        else persistent = false;
      } catch {
        persistent = false;
      }
      return memory;
    },
    set(value) {
      memory = value;
      try {
        storage2?.setItem(key, value);
      } catch {
        persistent = false;
      }
    },
    clear() {
      memory = null;
      try {
        storage2?.removeItem(key);
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

// client/glyphs.js
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

// client/sky-2.js
var storage;
try {
  storage = localStorage;
} catch {
}
var api = createApi({}, { baseUrl: API_BASE, tokenStore: createTokenStore(storage) });
async function openSky() {
  const id = new URL(location.href).searchParams.get("room");
  if (!id || !/^[A-Za-z0-9_-]{1,128}$/.test(id)) throw new Error("FORBIDDEN");
  if (!(await api.session()).authorized) throw new Error("UNAUTHORIZED");
  const passage = await api.getFinale(id);
  if (passage.sky2 !== true) throw new Error("FORBIDDEN");
  const response = await fetch("../stars/");
  if (!response.ok) throw new Error("CONNECTION");
  const template = new DOMParser().parseFromString(await response.text(), "text/html");
  const style = document.createElement("style");
  style.textContent = template.querySelector("style").textContent.replaceAll("./assets/", "../assets/");
  document.head.append(style);
  document.body.replaceChildren(...Array.from(template.body.children, (element) => document.importNode(element, true)));
  document.querySelectorAll("#view .star-core, #view-2 .star-core").forEach((link) => link.remove());
  const center = document.createElement("a");
  center.className = "star-core";
  center.href = "#violet-contour";
  center.setAttribute("aria-label", "\u041F\u0440\u043E\u0434\u043E\u043B\u0436\u0438\u0442\u044C");
  document.querySelector("#view-3").append(center);
  const contour = document.createElement("section");
  contour.id = "violet-contour";
  contour.className = "art violet-contour";
  const back = document.createElement("a");
  back.className = "art-back";
  back.href = "#view-3";
  back.setAttribute("aria-label", "\u0412\u0435\u0440\u043D\u0443\u0442\u044C\u0441\u044F \u043A \u0437\u0432\u0435\u0437\u0434\u0435");
  const outline = finishedContour();
  outline.querySelectorAll("svg").forEach((mark) => mark.remove());
  outline.setAttribute("aria-label", "\u0424\u0438\u043E\u043B\u0435\u0442\u043E\u0432\u044B\u0439 \u043A\u043E\u043D\u0442\u0443\u0440");
  contour.append(back, outline);
  document.body.append(contour);
  const fragment = location.hash;
  if (["#sky", "#view", "#view-2", "#view-3", "#violet-contour"].includes(fragment)) {
    history.replaceState(null, "", location.pathname + location.search);
    location.replace(fragment);
  }
  if (passage.nextSignal) console.info("\u0421\u043B\u0435\u0434\u0443\u044E\u0449\u0438\u0439 \u044D\u0444\u0438\u0440 // " + passage.nextSignal);
}
openSky().catch(() => {
  const status = document.querySelector(".sky-status");
  if (status) status.textContent = "\u0421\u0438\u0433\u043D\u0430\u043B \u043D\u0435\u0434\u043E\u0441\u0442\u0443\u043F\u0435\u043D.";
  const back = document.createElement("a");
  back.className = "sky-return";
  back.href = "../";
  back.textContent = "\u0412\u0435\u0440\u043D\u0443\u0442\u044C\u0441\u044F";
  document.body.append(back);
});
