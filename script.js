import { initializeApp } from "https://www.gstatic.com/firebasejs/11.7.1/firebase-app.js";
import {
  getAuth,
  onAuthStateChanged,
  signInWithEmailAndPassword,
  createUserWithEmailAndPassword,
  signOut,
  updateProfile,
  connectAuthEmulator
} from "https://www.gstatic.com/firebasejs/11.7.1/firebase-auth.js";
import {
  getFirestore,
  doc,
  getDoc,
  setDoc,
  updateDoc,
  onSnapshot,
  serverTimestamp,
  connectFirestoreEmulator,
  collection,
  query,
  where,
  getDocs,
  limit,
  addDoc
} from "https://www.gstatic.com/firebasejs/11.7.1/firebase-firestore.js";
import {
  getStorage,
  ref,
  uploadBytes,
  getDownloadURL,
  connectStorageEmulator
} from "https://www.gstatic.com/firebasejs/11.7.1/firebase-storage.js";

async function reviewEntryApproval({ roomUid, targetUid, status }) {
  await setDoc(doc(db, "review_requests", `${roomUid}_${targetUid}`), {
    roomUid: roomUid,
    targetUid: targetUid,
    status: status,
    createdAt: serverTimestamp()
  });
}

const firebaseConfig = {
  apiKey: "AIzaSyBZDUfNQO6nj0Y-agzpE1oCkYufbMi_Txk",
  authDomain: "spinfate1.firebaseapp.com",
  projectId: "spinfate1",
  storageBucket: "spinfate1.firebasestorage.app",
  messagingSenderId: "503992322554",
  appId: "1:503992322554:web:2b5397e6c44411eb23fc43",
  measurementId: "G-KDHKCTNEHP"
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const storage = getStorage(app);

const roomUid = (() => {
  const m = location.pathname.match(/^\/(?:room|host)\/([^/]+)/);
  return m ? decodeURIComponent(m[1]) : "";
})();
const isRoomPage = location.pathname.startsWith("/room/");
const isHostPage = location.pathname.startsWith("/host/");

const dom = {
  wheel: document.getElementById("wheel"),
  centerBtn: document.getElementById("centerBtn"),
  centerLabel: document.getElementById("centerLabel"),
  resultText: document.getElementById("resultText"),
  hint: document.getElementById("hint"),
  titleText: document.getElementById("titleText"),
  roomNameLabel: document.getElementById("roomNameLabel"),
  roomHostIcon: document.getElementById("roomHostIcon"),
  drawImageThumb: document.getElementById("drawImageThumb"),
  spinRemainingMini: document.getElementById("spinRemainingMini"),
  entryRequestBtn: document.getElementById("entryRequestBtn"),
  entryRequestIcon: document.getElementById("entryRequestIcon"),

  descWrap: document.getElementById("descWrap"),
  descText: document.getElementById("descText"),
  descToggle: document.getElementById("descToggle"),

  btnPlus: document.getElementById("btnPlus"),
  btnPrize: document.getElementById("btnPrize"),

  modalBack: document.getElementById("modalBack"),
  modalClose: document.getElementById("modalClose"),
  btnCancel: document.getElementById("btnCancel"),
  btnSave: document.getElementById("btnSave"),
  btnAdd: document.getElementById("btnAdd"),
  itemList: document.getElementById("itemList"),
  targetPrizeSelect: document.getElementById("targetPrizeSelect"),
  titleInput: document.getElementById("titleInput"),
  roomDescriptionBlock: document.getElementById("roomDescriptionBlock"),
  roomDescriptionInput: document.getElementById("roomDescriptionInput"),

  drawImageViewBack: document.getElementById("drawImageViewBack"),
  drawImageViewImg: document.getElementById("drawImageViewImg"),

  btnLogin: document.getElementById("btnLogin"),
  userBox: document.getElementById("userBox"),
  userIcon: document.getElementById("userIcon"),
  userTag: document.getElementById("userTag"),
  btnGoRoom: document.getElementById("btnGoRoom"),
  btnEntryRequestsHost: document.getElementById("btnEntryRequestsHost"),
  entryRequestHostBadge: document.getElementById("entryRequestHostBadge"),
  accountMenuBtn: document.getElementById("accountMenuBtn"),
  accountMenu: document.getElementById("accountMenu"),
  btnLogout: document.getElementById("btnLogout"),
  btnAccountEdit: document.getElementById("btnAccountEdit"),
  btnPrizeList: document.getElementById("btnPrizeList"),
  btnPrizeImageList: document.getElementById("btnPrizeImageList"),
  missingImageCount: document.getElementById("missingImageCount"),

  authBack: document.getElementById("authBack"),
  authClose: document.getElementById("authClose"),
  authCancel: document.getElementById("authCancel"),
  authSubmit: document.getElementById("authSubmit"),
  authTitle: document.getElementById("authTitle"),
  authErr: document.getElementById("authErr"),
  loginForm: document.getElementById("loginForm"),
  signupForm: document.getElementById("signupForm"),
  toSignup: document.getElementById("toSignup"),
  toLogin: document.getElementById("toLogin"),
  loginEmail: document.getElementById("loginEmail"),
  loginPass: document.getElementById("loginPass"),
  signupEmail: document.getElementById("signupEmail"),
  signupPass: document.getElementById("signupPass"),
  signupPass2: document.getElementById("signupPass2"),
  signupPlan: document.getElementById("signupPlan"),
  signupNick: document.getElementById("signupNick"),
  signupIcon: document.getElementById("signupIcon"),
  iconPreview: document.getElementById("iconPreview"),
  shipSei: document.getElementById("shipSei"),
  shipMei: document.getElementById("shipMei"),
  shipSeiKana: document.getElementById("shipSeiKana"),
  shipMeiKana: document.getElementById("shipMeiKana"),
  shipPhone: document.getElementById("shipPhone"),
  shipPostal: document.getElementById("shipPostal"),
  shipAddrAuto: document.getElementById("shipAddrAuto"),
  shipAddr2: document.getElementById("shipAddr2"),
  shipBuilding: document.getElementById("shipBuilding"),

  accountEditBack: document.getElementById("accountEditBack"),
  accountEditClose: document.getElementById("accountEditClose"),
  accountEditCancel: document.getElementById("accountEditCancel"),
  accountEditSave: document.getElementById("accountEditSave"),
  accountEditNick: document.getElementById("accountEditNick"),
  accountEditIcon: document.getElementById("accountEditIcon"),
  accountEditIconPreview: document.getElementById("accountEditIconPreview"),
  accountEditDescription: document.getElementById("accountEditDescription"),
  accountEditPhone: document.getElementById("accountEditPhone"),
  accountEditPostal: document.getElementById("accountEditPostal"),
  accountEditAddrAuto: document.getElementById("accountEditAddrAuto"),
  accountEditAddr2: document.getElementById("accountEditAddr2"),
  accountEditBuilding: document.getElementById("accountEditBuilding"),

  entryRequestBack: document.getElementById("entryRequestBack"),
  entryRequestClose: document.getElementById("entryRequestClose"),
  entryRequestCancel: document.getElementById("entryRequestCancel"),
  entryRequestList: document.getElementById("entryRequestList"),

  prizeBack: document.getElementById("prizeBack"),
  prizeClose: document.getElementById("prizeClose"),
  prizeCancel: document.getElementById("prizeCancel"),
  prizeSave: document.getElementById("prizeSave"),
  prizeCsvSave: document.getElementById("prizeCsvSave"),
  prizeName: document.getElementById("prizeName"),

  prizeImageInput: document.getElementById("prizeImageInput"),
  prizeImageSelect: document.getElementById("prizeImageSelect"),
  prizeImagePreview: document.getElementById("prizeImagePreview"),
  prizePoint: document.getElementById("prizePoint"),

  prizeCost: document.getElementById("prizeCost"),
  prizeExchangeRate: document.getElementById("prizeExchangeRate"),
  prizeCsvInput: document.getElementById("prizeCsvInput"),

  prizeListBack: document.getElementById("prizeListBack"),
  prizeListClose: document.getElementById("prizeListClose"),
  prizeListCancel: document.getElementById("prizeListCancel"),
  prizeListRows: document.getElementById("prizeListRows"),

  prizeImageListBack: document.getElementById("prizeImageListBack"),
  prizeImageListClose: document.getElementById("prizeImageListClose"),
  prizeImageListCancel: document.getElementById("prizeImageListCancel"),
  prizeImageListRows: document.getElementById("prizeImageListRows"),
  missingImageSection: document.getElementById("missingImageSection"),
  missingImageSectionCount: document.getElementById("missingImageSectionCount"),
  btnOpenMissingImages: document.getElementById("btnOpenMissingImages"),

  missingImagesBack: document.getElementById("missingImagesBack"),
  missingImagesClose: document.getElementById("missingImagesClose"),
  missingImagesCancel: document.getElementById("missingImagesCancel"),
  missingImagesRows: document.getElementById("missingImagesRows"),
  missingImagesInput: document.getElementById("missingImagesInput"),
  missingDropZone: document.getElementById("missingDropZone"),

  confirmBack: document.getElementById("confirmBack"),
  confirmClose: document.getElementById("confirmClose"),
  confirmCancel: document.getElementById("confirmCancel"),
  confirmOk: document.getElementById("confirmOk"),
  confirmText: document.getElementById("confirmText"),

  replacePrizeImageInput: document.getElementById("replacePrizeImageInput"),

  prizeInfo: document.getElementById("prizeInfo"),
  prizeInfoImg: document.getElementById("prizeInfoImg"),
  prizeInfoName: document.getElementById("prizeInfoName")
};

const ctx = dom.wheel.getContext("2d");

const state = {
  me: null,
  myProfile: null,
  room: null,
  items: [],
  roomUnsub: null,
  myProfileUnsub: null,
  myEntryRequestUnsub: null,
  authMode: "login",
  entryRequestStatus: 0,
  descExpanded: false,
  signupIconFile: null,
  accountEditIconFile: null,
  prizeImageFile: null,
  repeatConfirmNeeded: true,
  prizeMasters: [],
  prizeImages: [],
  missingPrizeImages: [],
  approvedUsers: [],
  pendingConfirm: null,
  replaceTargetImageName: "",
  wheel: {
    status: "idle",
    angle: 0,
    target: 0,
    settling: false,
    rafId: 0,
    stopHandledAtPhase2: false
  }
};

function isHost() {
  if (!state.me) return false;
  if (!isHostPage) return false;
  return state.me.uid === roomUid;
}

function isFreePlan() {
  return String(state.myProfile?.plan || "FREE") === "FREE";
}

async function canParticipantStopRoom() {
  if (!state.me || !isRoomPage || isHost()) return false;

  const q = query(
    collection(db, "joining"),
    where("hostUid", "==", roomUid),
    where("status", "==", 2)
  );

  const snap = await getDocs(q);
  return (
    snap.size === 1 &&
    String(snap.docs[0].data()?.participantUid || "") === state.me.uid
  );
}

function parseList(raw) {
  try {
    const v = typeof raw === "string" ? JSON.parse(raw || "[]") : raw;
    if (!Array.isArray(v)) return [];
    return v
      .map((row) => {
        if (!Array.isArray(row)) return null;
        return [
          String(row[0] ?? "").trim(),
          Math.max(1, Number(row[1] ?? 1) || 1),
          String(row[2] ?? "#cccccc")
        ];
      })
      .filter((x) => x && x[0]);
  } catch {
    return [];
  }
}

function stringifyList(list) {
  return JSON.stringify(
    list.map((x) => [
      String(x[0] ?? "").trim(),
      Math.max(1, Number(x[1] ?? 1) || 1),
      String(x[2] ?? "#cccccc")
    ])
  );
}

function clampIndex(index, items) {
  if (!Array.isArray(items) || items.length === 0) return -1;
  const n = Number(index);
  if (!Number.isInteger(n)) return -1;
  if (n < 0 || n >= items.length) return -1;
  return n;
}

function normalizeAngle(rad) {
  const two = Math.PI * 2;
  let v = Number(rad || 0) % two;
  if (v < 0) v += two;
  return v;
}

function weightedTotal(items) {
  return items.reduce((s, x) => s + Math.max(1, Number(x[1] || 1)), 0);
}

const ITEM_COLORS = [
  "#A7F3D0",
  "#BFDBFE",
  "#FBCFE8",
  "#FDE68A",
  "#C7D2FE",
  "#F9A8D4",
  "#FDBA74",
  "#86EFAC",
  "#93C5FD"
];

function getNextItemColor(index) {
  return ITEM_COLORS[index % ITEM_COLORS.length];
}

function pickWeightedIndex(items) {
  if (!Array.isArray(items) || !items.length) return -1;

  const total = items.reduce((s, x) => s + Math.max(1, Number(x[1] || 1)), 0);
  let r = Math.random() * total;

  for (let i = 0; i < items.length; i++) {
    r -= Math.max(1, Number(items[i][1] || 1));
    if (r < 0) return i;
  }

  return items.length - 1;
}

function calcFinalAngleForIndex(items, index, offsetRatio = 0.5) {
  if (!items.length || index < 0 || index >= items.length) return 0;

  const total = weightedTotal(items);
  let a = -Math.PI / 2;

  for (let i = 0; i < items.length; i++) {
    const span = (Math.max(1, Number(items[i][1] || 1)) / total) * Math.PI * 2;

    if (i === index) {
      const ratio = Math.max(0.08, Math.min(0.92, Number(offsetRatio || 0.5)));
      const hit = a + span * ratio;
      return normalizeAngle(-Math.PI / 2 - hit);
    }

    a += span;
  }
}

function calcTargetAngleFromCurrent(current, finalAngle, spins = 2) {
  const cur = normalizeAngle(current);
  const fin = normalizeAngle(finalAngle);
  let delta = fin - cur;
  if (delta < 0) delta += Math.PI * 2;
  return current + delta + spins * Math.PI * 2;
}

function easeOutCubic(t) {
  return 1 - Math.pow(1 - t, 3);
}

function stopRaf() {
  if (state.wheel.rafId) {
    cancelAnimationFrame(state.wheel.rafId);
    state.wheel.rafId = 0;
  }
}

function scheduleTick() {
  if (state.wheel.rafId) return;
  state.wheel.rafId = requestAnimationFrame(tick);
}

async function finishRoomSpin({ roomUid }) {
  const roomRef = doc(db, "rooms", roomUid);
  await updateDoc(roomRef, {
    phase: 0,
    updatedAt: serverTimestamp()
  });
}

function tick() {
  state.wheel.rafId = 0;

  if (state.wheel.status === "spinning") {
    state.wheel.angle += 0.26;
    dom.resultText.textContent = "抽選中...";
    drawWheel();
    scheduleTick();
    return;
  }

  if (state.wheel.status === "settling") {
    const w = state.wheel;

    if (w.targetAngle == null && state.room?.angle != null) {
      w.startAngle = state.wheel.angle;
      w.targetAngle = calcTargetAngleFromCurrent(
        w.startAngle,
        Number(state.room.angle || 0),
        2
      );
      w.t = 0;
    }

    w.t = (w.t || 0) + 1;
    const duration = 160;
    let p = w.t / duration;
    if (p > 1) p = 1;
    const e = easeOutCubic(p);
    w.angle = w.startAngle + (w.targetAngle - w.startAngle) * e;
    drawWheel();

    if (p < 1) {
      scheduleTick();
      return;
    }

    w.status = "idle";
    w.angle = normalizeAngle(w.targetAngle);

    if (!isRoomPage && !isHostPage) {
      const nextIndex = pickWeightedIndex(state.items);

      renderRoom({
        ...(state.room || {}),
        phase: 0,
        angle: w.angle,
        indexview: state.room.index,
      });
      return;
    }

    drawWheel();

    if (isHost() && state.room && Number(state.room.phase) === 2 && !state.wheel.finishedCalled) {
      state.wheel.finishedCalled = true;
      finishRoomSpin({ roomUid }).catch(console.error);
    }
    return;
  }

  drawWheel();
}

function drawWheel() {
  const W = dom.wheel.width;
  const H = dom.wheel.height;
  const cx = W / 2;
  const cy = H / 2;
  const R = Math.min(W, H) * 0.46;

  ctx.clearRect(0, 0, W, H);
  ctx.save();
  ctx.translate(cx, cy);
  ctx.rotate(state.wheel.angle);

  const items = state.items;
  if (!items.length) {
    ctx.beginPath();
    ctx.arc(0, 0, R, 0, Math.PI * 2);
    ctx.fillStyle = "#f3f4f6";
    ctx.fill();
    ctx.restore();
    return;
  }

  const total = weightedTotal(items);
  let a = -Math.PI / 2;

  for (const item of items) {
    const span = (Math.max(1, Number(item[1] || 1)) / total) * Math.PI * 2;

    ctx.beginPath();
    ctx.moveTo(0, 0);
    ctx.arc(0, 0, R, a, a + span);
    ctx.closePath();
    ctx.fillStyle = item[2] || "#cccccc";
    ctx.fill();

    ctx.save();
    ctx.rotate(a + span / 2);
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    ctx.fillStyle = "#111";
    ctx.font = "bold 20px sans-serif";
    ctx.fillText(String(item[0] || ""), R - 20, 0);
    ctx.restore();

    a += span;
  }

  ctx.restore();
}

function setCenterButtonByState() {
  if (state.wheel.status === "spinning") {
    dom.centerLabel.textContent = "STOP";
    dom.centerBtn.classList.add("stop");
    return;
  }
  dom.centerLabel.textContent = "START";
  dom.centerBtn.classList.remove("stop");
}

function setImage(el, url) {
  if (!url) {
    el.removeAttribute("src");
    el.style.display = "none";
    return;
  }
  el.src = url;
  el.style.display = "block";
}

function renderDescription() {
  if (!isRoomPage && !isHostPage) {
    dom.descWrap.style.display = "none";
    dom.descText.innerHTML = "";
    dom.descToggle.style.display = "none";
    return;
  }
  const text = String(state.room?.description || "");
  if (!text) {
    dom.descWrap.style.display = "none";
    dom.descText.innerHTML = "";
    dom.descToggle.style.display = "none";
    return;
  }

  dom.descWrap.style.display = "block";
  dom.descText.innerHTML = escapeHtml(text).replace(/\n/g, "<br>");
  dom.descText.style.webkitLineClamp = state.descExpanded ? "unset" : "2";
  dom.descText.style.lineClamp = state.descExpanded ? "unset" : "2";
  dom.descText.style.overflow = state.descExpanded ? "visible" : "hidden";

  requestAnimationFrame(() => {
    const over = dom.descText.scrollHeight > dom.descText.clientHeight + 2;
    dom.descToggle.style.display = over || state.descExpanded ? "block" : "none";
    dom.descToggle.textContent = state.descExpanded ? "△" : "▽";
  });
}

function renderRemaining() {
  if (!state.myProfile || isFreePlan() || !isHostPage || !isHost()) {
    dom.spinRemainingMini.style.display = "none";
    dom.spinRemainingMini.textContent = "";
    return;
  }

  const limit = Math.max(0, Number(state.myProfile.limit || 0));
  const used = Math.max(0, Number(state.myProfile.usageUsed || 0));
  const remain = Math.max(0, limit - used);

  dom.spinRemainingMini.style.display = "block";
  dom.spinRemainingMini.textContent = `残り ${remain} 回`;
}

function renderUser() {
  const isTopPage = !isRoomPage && !isHostPage;
  const isOwner = isHost();
  const isFree = isFreePlan();
  // 1. 基本的なユーザー情報の表示/非表示
  if (!state.me) {
    dom.btnLogin.style.display = "inline-flex";
    dom.userBox.style.display = "none";
    if (!isFree && isTopPage) {
      dom.btnGoRoom.style.display = "inline-flex";
      dom.btnGoRoom.textContent = "HOST";
    } else if (!isFree && isHostPage && isOwner) {
      dom.btnGoRoom.style.display = "inline-flex";
      dom.btnGoRoom.textContent = "Top";
    } else {
      dom.btnGoRoom.style.display = "none";
    }
    dom.btnPrizeList.style.display = "none";
    dom.btnPrizeImageList.style.display = "none";
    // トップページならボタンを表示、それ以外は非表示
    dom.btnPlus.style.display = isTopPage ? "inline-flex" : "none";
    return;
  }

  dom.btnLogin.style.display = "none";
  dom.userBox.style.display = "flex";
  // 2. 権限に基づく制御
  // トップページはデモとして編集可。それ以外はホスト本人かつ有料プランのみ可。
  const canEditItems = isTopPage || (isOwner && !isFree);

  dom.btnPrize.style.display = (isOwner && !isFree) ? "inline-flex" : "none";
  dom.btnPlus.style.display = canEditItems ? "inline-flex" : "none";
  dom.btnPrizeList.style.display = (isOwner && !isFree) ? "block" : "none";
  dom.btnPrizeImageList.style.display = (isOwner && !isFree) ? "flex" : "none";

  const nick = state.myProfile?.nick || "USER";
  const iconUrl = state.myProfile?.iconUrl || "";
  dom.userTag.textContent = nick;
  setImage(dom.userIcon, iconUrl);
  dom.userIcon.style.display = iconUrl ? "block" : "none";

  renderEntryRequestUi();
}

function renderRoom(room) {
  state.room = room;
  state.items = parseList(room.list);

  dom.titleText.textContent = room.title || "タイトル";
  dom.roomNameLabel.textContent = room.hostNick ? `${room.hostNick}ルーム` : "";
  setImage(dom.roomHostIcon, room.hostIconUrl || "");
  renderDescription();

  const indexView = clampIndex(room.indexview, state.items);
  if (state.wheel.status !== "spinning") {
    dom.resultText.textContent =
      indexView >= 0 && state.items[indexView] ? state.items[indexView][0] : "-";
  }

  const phase = Number(room.phase || 0);

  if (phase === 0) {
    state.wheel.status = "idle";
    state.wheel.angle = normalizeAngle(Number(room.angle || 0));
    state.wheel.stopHandledAtPhase2 = false;
    stopRaf();
    drawWheel();
  } else if (phase === 1) {
    if (state.wheel.status !== "spinning") {
      state.wheel.status = "spinning";
      state.wheel.stopHandledAtPhase2 = false;
      scheduleTick();
    }
  } else if (phase === 2) {
    if (!state.wheel.stopHandledAtPhase2) {
      state.wheel.status = "settling";
      state.wheel.startAngle = state.wheel.angle;
      state.wheel.targetAngle = calcTargetAngleFromCurrent(
        state.wheel.angle,
        Number(room.angle || 0),
        2
      );
      state.wheel.t = 0;
      state.wheel.stopHandledAtPhase2 = true;
      scheduleTick();
    }
  }

  setCenterButtonByState();
  renderRemaining();
  renderEntryRequestUi();
  renderPrizeInfo();
}

function openModal(el) {
  if (el) el.style.display = "flex";
}

function closeModal(el) {
  if (el) el.style.display = "none";
}

function closeAllMenus() {
  dom.accountMenu.style.display = "none";
}

function makeItemRow(name = "", weight = 1, color = null, mode = "A") {
  const isTopPage = !isRoomPage && !isHostPage; // トップページ判定
  color = color || getNextItemColor(dom.itemList.querySelectorAll(".itemRow").length);
  
  const row = document.createElement("div");
  row.className = "itemRow";
  
  // HTML構造の分岐
  let inputHtml = isTopPage 
    ? `<input type="text" class="itemName" placeholder="項目名" value="${escapeHtml(name)}">`
    : `<select class="itemSelect"></select>`;

  row.innerHTML = `
    ${inputHtml}
    <input type="number" class="itemWeight" min="1" step="1" value="${Number(weight || 1)}">
    <button type="button" class="swatch" style="background:${escapeHtml(color)};"></button>
    <input type="color" class="itemColor" value="${escapeHtml(color)}">
    <button type="button" class="del">×</button>
  `;

  // DB連携ページの場合のみセレクトボックスに値を流し込む
  if (!isTopPage) {
    const sel = row.querySelector(".itemSelect");
    sel.innerHTML = `<option value=""></option>`;
    const cSrc = (mode === "A") ? state.approvedUsers : state.prizeMasters;
    cSrc.forEach(c => {
      const opt = document.createElement("option");
      opt.value = (mode === "A" ? c.participantUid : c.id);
      opt.textContent = (mode === "A" ? c.nick : c.name);
      sel.appendChild(opt);
    });
    if (name) {
      const hit = cSrc.find(c => String((mode === "A" ? c.nick : c.name) || "") === String(name));
      if (hit) sel.value = (mode === "A" ? hit.participantUid : hit.id);
    }
  }

  const swatch = row.querySelector(".swatch");
  const colorInput = row.querySelector(".itemColor");
  const delBtn = row.querySelector(".del");

  swatch.addEventListener("click", () => colorInput.click());
  colorInput.addEventListener("input", () => { swatch.style.background = colorInput.value; });
  delBtn.addEventListener("click", () => row.remove());

  return row;
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function normalizeImageKey(name) {
  const base = String(name || "")
    .trim()
    .toLowerCase()
    .replace(/\.[^.]+$/, "")
    .replace(/\.$/, "");
  if (/[\\/:*?"<>|_]/.test(base)) {
    throw new Error("画像名に使用できない文字が含まれています");
  }
  return base;
}

function fillItemEditorFromRoom() {
  const room = state.room || {};
  const isTopPage = !isRoomPage && !isHostPage;

  dom.titleInput.value = room.title || "";
  dom.roomDescriptionInput.value = room.description || "";
  const prizeNote = [...dom.modalBack.querySelectorAll('div')].find(el => el.textContent.trim() === "※選択した景品の画像が抽選に使用されます");
  if (prizeNote) prizeNote.style.display = isTopPage ? "none" : "block";
  
  // 説明文ブロックとモード選択の制御
  dom.roomDescriptionBlock.style.display = isTopPage ? "none" : "block";
  const editDrawModeEl = document.getElementById("editDrawMode");
  const mode = editDrawModeEl.value = room.drawMode || "A";

  if (isTopPage) {
    // トップページなら全て隠す
    editDrawModeEl.closest('div').style.display = "none";
    document.getElementById("targetPrizeLabel").style.display = "none"; // 注釈もここで隠れます
    dom.targetPrizeSelect.style.display = "none";
    document.getElementById("modeCSettings").style.display = "none";
    dom.btnSave.disabled = false; 
  } else {
    // ルーム・ホストページ用の表示ロジック
    editDrawModeEl.closest('div').style.display = "block";
    refreshPrizeAdminData(); 
  }

  // 項目リスト生成
  dom.itemList.innerHTML = "";
  const items = parseList(room.list);
  if (items.length) {
    items.forEach((x) => dom.itemList.appendChild(makeItemRow(x[0], x[1], x[2], mode)));
  } else {
    dom.itemList.appendChild(makeItemRow("", 1, getNextItemColor(0), mode));
  }
}

function readItemsFromEditor() {
  const rows = [...dom.itemList.querySelectorAll(".itemRow")];
  const isTopPage = !isRoomPage && !isHostPage;
  
  return rows.map((row) => {
    let name = isTopPage 
      ? row.querySelector(".itemName")?.value.trim() || ""
      : row.querySelector(".itemSelect")?.selectedOptions?.[0]?.textContent?.trim() || "";
    
    const weight = Math.max(1, Number(row.querySelector(".itemWeight")?.value || 1) || 1);
    const color = row.querySelector(".itemColor")?.value || "#cccccc";
    return name ? [name, weight, color] : null;
  }).filter(Boolean);
}

async function uploadImageFile(file, path) {
  if (!file) return "";

  const img = await new Promise((resolve, reject) => {
    const url = URL.createObjectURL(file);
    const image = new Image();
    image.onload = () => {
      URL.revokeObjectURL(url);
      resolve(image);
    };
    image.onerror = reject;
    image.src = url;
  });

  const canvas = document.createElement("canvas");
  const size = 256;
  canvas.width = size;
  canvas.height = size;

  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, size, size);

  const sw = img.width;
  const sh = img.height;
  const srcSize = Math.min(sw, sh);
  const sx = (sw - srcSize) / 2;
  const sy = (sh - srcSize) / 2;

  ctx.drawImage(img, sx, sy, srcSize, srcSize, 0, 0, size, size);

  const blob = await new Promise((resolve) => {
    canvas.toBlob(resolve, "image/webp", 0.9);
  });

  if (!blob) {
    throw new Error("画像変換に失敗しました");
  }

  const storageRef = ref(storage, path);
  await uploadBytes(storageRef, blob, { contentType: "image/webp" });
  return await getDownloadURL(storageRef);
}

async function ensureHostRoom() {
  if (!state.me || !isHost()) return;
  const roomRef = doc(db, "rooms", state.me.uid);
  const snap = await getDoc(roomRef);
  if (snap.exists()) return;

  try {
    // ネットワーク通信を行わず、Firestoreに直接データをセットします
    await setDoc(roomRef, {
      roomUid: state.me.uid,
      title: "タイトル",
      description: "",
      list: JSON.stringify([]),
      targetPrizeId: "",
      point: 0,
      drawMode: "A",
      maxSlots: 0,
      userMax: 0,
      updatedAt: serverTimestamp() // Firestoreのサーバー時間を使用
    });
  } catch (e) {
    console.error("Room initialization failed:", e);
  }
}

function applyLocalDefaultRoom() {
  renderRoom({
    angle: 0,
    hostIconUrl: "",
    hostNick: "",
    imageUrl: "",
    list: JSON.stringify([
      ["A", 1, "#A7F3D0"],
      ["B", 1, "#BFDBFE"],
      ["C", 1, "#FBCFE8"],
    ]),
    phase: 0,
    index: -1,
    indexview: -1,
    title: "タイトル",
    point: 0
  });
}

function subscribeRoom() {
  if (state.roomUnsub) {
    state.roomUnsub();
    state.roomUnsub = null;
  }

  // 1. トップページの場合のみデモ（ABC）を表示
  if (!isRoomPage && !isHostPage) {
    applyLocalDefaultRoom();
    return;
  }

  // 2. ルームページの場合
  state.roomUnsub = onSnapshot(doc(db, "rooms", roomUid), (snap) => {
    if (!snap.exists()) {
      // 3. ルームデータがない場合は「空」の状態で初期化
      renderRoom({
        angle: 0,
        hostIconUrl: "",
        hostNick: "",
        imageUrl: "",
        list: JSON.stringify([]), // ここを空にする
        phase: 0,
        index: -1,
        indexview: -1,
        title: "タイトル",
        point: 0
      });
      return;
    }
    renderRoom(snap.data());
  });
}

function subscribeMyProfile(uid) {
  if (typeof state.myProfileUnsub === "function") state.myProfileUnsub();
  state.myProfileUnsub = onSnapshot(doc(db, "spinfate_profiles", uid), (snap) => {
    state.myProfile = snap.exists() ? { uid, ...snap.data() } : null;
    renderUser();
    renderRemaining();
  });
}

function setAuthMode(mode) {
  state.authMode = mode;
  const isLogin = mode === "login";
  dom.authTitle.textContent = isLogin ? "ログイン" : "アカウント作成";
  dom.authSubmit.textContent = isLogin ? "ログイン" : "作成";
  dom.loginForm.style.display = isLogin ? "grid" : "none";
  dom.signupForm.style.display = isLogin ? "none" : "grid";
  dom.authErr.textContent = "";
}

function getEntryRequestIconSrc(status) {
  if (status === 1) return "/img/i1.jpg";
  if (status === 2) return "/img/i2.jpg";
  if (status === 3) return "/img/i3.jpg";
  return "/img/i0.jpg";
}

function renderEntryRequestList() {
  const rows = Array.isArray(state.pendingEntryRequests) ? state.pendingEntryRequests : [];

  if (!rows.length) {
    dom.entryRequestList.innerHTML = `<div class="entryEmpty">申請中の参加者はいません</div>`;
    return;
  }

  dom.entryRequestList.innerHTML = rows.map((row) => `
    <div class="entryReqRow">
      <div class="entryReqLeft">
        <img src="${escapeHtml(row.iconUrl || "")}" alt="">
        <div class="entryReqNick">${escapeHtml(row.nick || "NO NAME")}</div>
      </div>
      <div class="entryReqActions">
        <button class="entryOkBtn" type="button" data-act="ok" data-uid="${escapeHtml(row.participantUid)}">OK</button>
        <button class="entryNgBtn" type="button" data-act="ng" data-uid="${escapeHtml(row.participantUid)}">NG</button>
      </div>
    </div>
  `).join("");
}

dom.entryRequestList.addEventListener("click", async (e) => {
  const btn = e.target.closest("button[data-act][data-uid]");
  if (!btn) return;

  if (!isHostPage || !isHost()) return;

  const act = String(btn.dataset.act || "");
  const targetUid = String(btn.dataset.uid || "");
  if (!targetUid) return;

  btn.disabled = true;

  try {
    // 呼び出し先は、Firestoreに書き込む関数へ変更されています
    await reviewEntryApproval({
      roomUid,
      targetUid,
      status: act === "ok" ? 2 : 3
    }); 
  } catch (err) {
    console.error("更新エラー:", err);
    alert("更新に失敗しました");
    btn.disabled = false;
  }
});

function renderEntryRequestUi() {
  const isRoomParticipant = isRoomPage && state.me && !isHost();

  if (!isRoomParticipant) {
    dom.entryRequestBtn.style.display = "none";
  } else {
    const status = Number(state.entryRequestStatus || 0);
    dom.entryRequestBtn.style.display = "inline-flex";
    dom.entryRequestIcon.src = getEntryRequestIconSrc(status);
    dom.entryRequestBtn.disabled = status === 1 || status === 2;
    dom.entryRequestBtn.style.cursor = (status === 1 || status === 2) ? "default" : "pointer";
  }

  const isHostManagerPage = isHostPage && state.me && isHost();
  if (!isHostManagerPage) {
    dom.btnEntryRequestsHost.style.display = "none";
    dom.entryRequestHostBadge.style.display = "none";
    return;
  }

  const count = Array.isArray(state.pendingEntryRequests) ? state.pendingEntryRequests.length : 0;

  dom.btnEntryRequestsHost.style.display = count > 0 ? "inline-flex" : "none";
  dom.entryRequestHostBadge.style.display = count > 0 ? "block" : "none";
  dom.entryRequestHostBadge.textContent = String(count);

  renderEntryRequestList();
}

function subscribeMyEntryRequest() {
  if (state.myEntryRequestUnsub) {
    state.myEntryRequestUnsub();
    state.myEntryRequestUnsub = null;
  }

  state.entryRequestStatus = 0;

  if (!isRoomPage || !state.me || isHost()) {
    renderEntryRequestUi();
    return;
  }

  const q = query(
    collection(db, "joining"),
    where("hostUid", "==", roomUid),
    where("participantUid", "==", state.me.uid),
    limit(1)
  );

  state.myEntryRequestUnsub = onSnapshot(q, (snap) => {
    if (snap.empty) {
      state.entryRequestStatus = 0;
    } else {
      state.entryRequestStatus = Number(snap.docs[0].data()?.status || 0);
    }
    renderEntryRequestUi();
  });
}

function subscribeHostEntryRequests() {
  if (state.hostEntryRequestsUnsub) {
    state.hostEntryRequestsUnsub();
    state.hostEntryRequestsUnsub = null;
  }

  if (!isHostPage || !state.me || !isHost()) {
    state.pendingEntryRequests = [];
    return;
  }

  const q = query(
    collection(db, "joining"),
    where("hostUid", "==", roomUid),
    where("status", "==", 1)
  );

  state.hostEntryRequestsUnsub = onSnapshot(q, (snap) => {
    state.pendingEntryRequests = snap.docs
      .map((d) => ({ id: d.id, ...(d.data() || {}) }))
      .sort((a, b) => String(a.nick || "").localeCompare(String(b.nick || ""), "ja"));

    renderEntryRequestUi();
  });
}

async function requestEntryApproval() {
  if (!isRoomPage || !state.me || isHost()) return;
  await setDoc(doc(db, "entry_requests", state.me.uid), { roomUid: roomUid, createdAt: serverTimestamp() });
}

async function fetchAddressByPostal(postal) {
  const zip = String(postal || "").replace(/\D/g, "");
  if (zip.length !== 7) return "";

  const res = await fetch(`https://zipcloud.ibsnet.co.jp/api/search?zipcode=${zip}`);
  const json = await res.json();

  if (!json || !Array.isArray(json.results) || !json.results[0]) return "";

  const r = json.results[0];
  return `${r.address1 || ""}${r.address2 || ""}${r.address3 || ""}`;
}

async function applyPostalToAddress(postalEl, addrEl) {
  const zip = String(postalEl?.value || "").replace(/\D/g, "");
  postalEl.value = zip;
  if (zip.length !== 7) return;

  try {
    const addr = await fetchAddressByPostal(zip);
    if (addr) addrEl.value = addr;
  } catch (e) {
    console.error(e);
  }
}

async function saveRoomFromEditor() {
  const isTopPage = !isRoomPage && !isHostPage;
  if (!isTopPage && !dom.targetPrizeSelect.value) {
    alert("項目を選んでください");
    return;
  }
  const items = readItemsFromEditor();
  if (!items.length) {
    alert("項目を1つ以上入力してください");
    return;
  }
  const drawMode = document.getElementById("editDrawMode").value;
  const maxSlots = parseInt(document.getElementById("editMaxSlots").value);
  const userMax = parseInt(document.getElementById("editUserMax").value);
  if (!isRoomPage && !isHostPage) {
    renderRoom({
      ...(state.room || {}),
      title: dom.titleInput.value.trim() || "タイトル",
      list: stringifyList(items),
      angle: 0,
      phase: 0,
      indexview: -1,
      point: Number(state.room?.point || 0),
    });
    state.repeatConfirmNeeded = false;
    closeModal(dom.modalBack);
    return;
  }

  if (!state.me) return;
  const requestRef = collection(db, "users", state.me.uid, "room_config_requests");
  await addDoc(requestRef, {
    roomUid: roomUid,
    title: dom.titleInput.value.trim(),
    description: dom.roomDescriptionInput.value,
    list: stringifyList(items),
    targetPrizeId: dom.targetPrizeSelect.value,
    drawMode: drawMode,
    maxSlots: maxSlots,
    userMax: userMax,
    point: Number(state.room?.point || 0),
    createdAt: serverTimestamp()
  });

  state.repeatConfirmNeeded = false;
  closeModal(dom.modalBack);
}

async function doLogin() {
  await signInWithEmailAndPassword(
    auth,
    dom.loginEmail.value.trim(),
    dom.loginPass.value
  );
  closeModal(dom.authBack);
}

async function doSignup() {
  const email = dom.signupEmail.value.trim();
  const pass = dom.signupPass.value;
  const pass2 = dom.signupPass2.value;
  const plan = dom.signupPlan.value;
  const nick = dom.signupNick.value.trim();

  if (!email || !pass || !nick) throw new Error("未入力があります");
  if (pass !== pass2) throw new Error("パスワード確認が一致しません");

  const cred = await createUserWithEmailAndPassword(auth, email, pass);

  await setDoc(doc(db, "spinfate_profiles", cred.user.uid), {
    createdAt: serverTimestamp(),
    limit: 0,
    nick,
    plan,
    shipping: {
      postal: dom.shipPostal.value.trim(),
      addr: dom.shipAddr2.value.trim(),
      building: dom.shipBuilding.value.trim(),
      sei: dom.shipSei.value.trim(),
      mei: dom.shipMei.value.trim(),
      seiKana: dom.shipSeiKana.value.trim(),
      meiKana: dom.shipMeiKana.value.trim(),
      phone: dom.shipPhone.value.trim()
    },
    shippingAuto: dom.shipAddrAuto.value.trim(),
    usageUsed: 0,
    usageMonth: "",
    designate: [],
    iconUrl: "",
    updatedAt: serverTimestamp()
  }, { merge: true });

  await auth.authStateReady();

  let iconUrl = "";
  if (state.signupIconFile) {
    iconUrl = await uploadImageFile(
      state.signupIconFile,
      `users/${cred.user.uid}/icon.webp`
    );
  }

  await updateProfile(cred.user, {
    displayName: nick,
    photoURL: iconUrl || null
  });

  if (iconUrl) {
    await setDoc(doc(db, "spinfate_profiles", cred.user.uid), {
      iconUrl,
      updatedAt: serverTimestamp()
    }, { merge: true });
  }

  closeModal(dom.authBack);
}

async function savePrizeMaster() {
  if (!state.me || isFreePlan()) {
    throw new Error("主催者のみ登録できます");
  }

  const name = String(dom.prizeName.value || "").trim();
  const point = Math.max(0, Number(dom.prizePoint.value || 0) || 0);
  const cost = Math.max(0, Number(dom.prizeCost.value || 0) || 0);
  const exchangeRate = Math.max(0, Math.min(100, Number(dom.prizeExchangeRate.value || 0) || 0));

  if (!name) {
    throw new Error("景品名を入力してください");
  }

  let imageUrl = String(dom.prizeImageSelect.value || "").trim();
  if (state.prizeImageFile) {
    const imageKey = normalizeImageKey(state.prizeImageFile.name);
    imageUrl = await uploadImageFile(
      state.prizeImageFile,
      `prize_masters/${state.me.uid}/${imageKey}.webp`
    );
  }

  // データベース操作を直接行わず、サーバー側の関数を呼び出す
  const requestRef = collection(db, "users", state.me.uid, "prize_master_requests");
  await addDoc(requestRef, {
    name,
    point,
    cost,
    exchangeRate,
    imageUrl,
    createdAt: serverTimestamp()
  });

  // UIの初期化
  dom.prizeName.value = "";
  dom.prizePoint.value = "";
  dom.prizeCost.value = "";
  dom.prizeExchangeRate.value = "";
  dom.prizeImageInput.value = "";
  dom.prizeImagePreview.removeAttribute("src");
  state.prizeImageFile = null;

  closeModal(dom.prizeBack);
}

async function resolveExistingPrizeImageUrl(imageName) {
  const raw = String(imageName || "").trim();
  if (!raw) return "";

  const imageKey = normalizeImageKey(raw);
  const storagePath = `prize_masters/${state.me.uid}/${imageKey}.webp`;

  try {
    const url = await getDownloadURL(ref(storage, storagePath));
    return url;
  } catch {
    return `__missing__:${imageName}`;
  }
}

function populatePrizeImageSelect() {
  dom.prizeImageSelect.innerHTML = `<option value="">登録画像から選択</option>`;
  state.prizeImages.forEach((item) => {
    const opt = document.createElement("option");
    opt.value = String(item.imageUrl || "");
    opt.textContent = String(item.imageName || "");
    dom.prizeImageSelect.appendChild(opt);
  });
}

async function savePrizeMastersCsv() {
  if (!state.me || isFreePlan()) {
    throw new Error("主催者のみ登録できます");
  }

  const file = dom.prizeCsvInput.files?.[0];
  if (!file) {
    throw new Error("CSVファイルを選択してください");
  }

  const text = await file.text();
  const lines = text
    .replace(/^\uFEFF/, "")
    .split(/\r?\n/)
    .map((x) => x.trim())
    .filter(Boolean);

  if (!lines.length) {
    throw new Error("CSVが空です");
  }

  for (const line of lines) {
    const cols = line.split(",").map((x) => x.trim());
    const name = String(cols[0] || "").trim();
    const point = Math.max(0, Number(cols[1] || 0) || 0);
    const cost = Math.max(0, Number(cols[2] || 0) || 0);
    const exchangeRate = Math.max(0, Math.min(100, Number(cols[3] || 0) || 0));
    const imageName = String(cols[4] || "").trim();

    if (!name) continue;

    let imageUrl = "";
    if (imageName) {
      const normalizedImageName = normalizeImageKey(imageName);
      imageUrl = await resolveExistingPrizeImageUrl(imageName);
      if (!imageUrl || imageUrl.startsWith("__missing__:")) {
        imageUrl = `__missing__:${normalizedImageName}`;
        await addMissingPrizeImage({ imageName: normalizedImageName });
      }
    }

    await addDoc(collection(db, "users", state.me.uid, "prize_master_requests"), {
      name,
      point,
      cost,
      exchangeRate,
      imageUrl,
      createdAt: serverTimestamp()
    });
  }

  dom.prizeCsvInput.value = "";
  await refreshPrizeAdminData();
  closeModal(dom.prizeBack);
}

async function addMissingPrizeImage({ imageName }) {
  if (!state.me) return;

  const requestRef = collection(db, "users", state.me.uid, "missing_image_requests");

  await addDoc(requestRef, {
    imageName: String(imageName).trim(),
    createdAt: serverTimestamp()
  });
}

function renderMissingImageBadges() {
  const count = state.missingPrizeImages.length;
  dom.missingImageCount.style.display = count > 0 ? "inline-block" : "none";
  dom.missingImageCount.textContent = String(count);
  dom.missingImageSection.style.display = count > 0 ? "block" : "none";
  dom.missingImageSectionCount.textContent = count > 0 ? `(${count})` : "";
}

function renderPrizeList() {
  if (!state.prizeMasters.length) {
    dom.prizeListRows.innerHTML = `<div class="entryEmpty">登録景品はありません</div>`;
    return;
  }

  dom.prizeListRows.innerHTML = state.prizeMasters.map((item) => `
  <div class="entryReqRow">
    <div class="entryReqLeft">
      ${String(item.imageUrl || "").trim() && !String(item.imageUrl || "").startsWith("__missing__:") ? `
        <img src="${escapeHtml(item.imageUrl || "")}" alt="" style="width:40px; height:40px; border-radius:10px; object-fit:cover; background:rgba(15,23,42,.08);">
      ` : ""}
      <div class="entryReqNick">${escapeHtml(item.name || "")}</div>
    </div>
    <div class="entryReqActions">
      <button class="entryNgBtn" type="button" data-prize-delete="${escapeHtml(item.id)}" data-prize-name="${escapeHtml(item.name || "")}">🗑</button>
    </div>
  </div>
  `).join("");
}

function getPrizeImageUsageCount(imageName) {
  if (!state.me || !imageName) return 0;

  const storagePath = `prize_masters/${state.me.uid}/${imageName}.webp`;
  const encodedStoragePath = encodeURIComponent(storagePath);
  const fileName = `${imageName}.webp`;

  return state.prizeMasters.filter((item) => {
    const imageUrl = String(item.imageUrl || "").trim();
    return (
      imageUrl === `__missing__:${imageName}` ||
      imageUrl.includes(encodedStoragePath) ||
      imageUrl.includes(storagePath) ||
      imageUrl.endsWith(`/${fileName}`) ||
      imageUrl.includes(`/${fileName}?`) ||
      imageUrl.includes(encodeURIComponent(fileName))
    );
  }).length;
}

function renderPrizeImageList() {
  if (!state.prizeImages.length) {
    dom.prizeImageListRows.innerHTML = `<div class="entryEmpty">登録画像はありません</div>`;
    return;
  }

  dom.prizeImageListRows.innerHTML = state.prizeImages.map((item) => `
    <div class="entryReqRow">
      <div class="entryReqLeft">
        <img src="${escapeHtml(item.imageUrl || "")}" alt="">
        <div class="entryReqNick">${escapeHtml(item.imageName || "")}</div>
      </div>
      <div class="entryReqActions">
        <button class="entryNgBtn" type="button" data-image-delete="${escapeHtml(item.imageName || "")}">🗑</button>
      </div>
    </div>
  `).join("");
}

function renderMissingImages() {
  if (!state.missingPrizeImages.length) {
    dom.missingImagesRows.innerHTML = `<div class="entryEmpty">不足画像はありません</div>`;
    return;
  }

  dom.missingImagesRows.innerHTML = state.missingPrizeImages.map((item) => `
    <div class="entryReqRow">
      <div class="entryReqNick">${escapeHtml(item.imageName || "")}</div>
    </div>
  `).join("");
}

async function refreshPrizeAdminData() {
  const mode = document.getElementById("editDrawMode")?.value || "A";
  if (!state.me || isFreePlan() || !isHostPage || !isHost()) {
    state.prizeMasters = [];
    state.prizeImages = [];
    state.missingPrizeImages = [];
    renderPrizeList();
    renderPrizeImageList();
    renderMissingImages();
    renderMissingImageBadges();
    return;
  }

  const [prizeSnap, imageSnap, missingSnap] = await Promise.all([
    getDocs(collection(db, "users", state.me.uid, "prizes")),
    getDocs(collection(db, "users", state.me.uid, "images")),
    getDocs(collection(db, "users", state.me.uid, "missing_images"))
  ]);

  state.prizeMasters = prizeSnap.docs.map(d => ({ id: d.id, ...d.data() }));
  state.prizeImages = imageSnap.docs.map(d => ({ id: d.id, ...d.data() }));
  state.missingPrizeImages = missingSnap.docs.map(d => ({ id: d.id, ...d.data() }));

  dom.targetPrizeSelect.innerHTML = `<option value="">項目を選択</option>`;
  const pSrc = (mode === "B") ? state.approvedUsers : state.prizeMasters;
  pSrc.forEach((p) => {
    const opt = document.createElement("option");
    opt.value = String((mode === "B" ? p.participantUid : p.id) || "");
    opt.textContent = String((mode === "B" ? p.nick : p.name) || "");
    dom.targetPrizeSelect.appendChild(opt);
  });

  renderPrizeList();
  renderPrizeImageList();
  renderMissingImages();
  renderMissingImageBadges();
}

async function refreshApprovedUsers() {
  state.approvedUsers = [];

  if (!state.me || isFreePlan() || !isHostPage || !isHost()) {
    return;
  }

  const q = query(
    collection(db, "joining"),
    where("hostUid", "==", roomUid),
    where("status", "==", 2)
  );

  const snap = await getDocs(q);

  state.approvedUsers = snap.docs
    .map((d) => ({ id: d.id, ...(d.data() || {}) }))
    .sort((a, b) => String(a.nick || "").localeCompare(String(b.nick || ""), "ja"));
}

function renderPrizeInfo() {
  const prizeId = String(state.room?.targetPrizeId || "");
  const prize = (state.prizeMasters || []).find(
    (x) => String(x.id || "") === prizeId
  );

  if (!prize) {
    dom.prizeInfo.style.display = "none";
    dom.prizeInfoName.textContent = "";
    dom.prizeInfoImg.style.display = "none";
    dom.prizeInfoImg.src = "";
    return;
  }

  dom.prizeInfo.style.display = "block";
  dom.prizeInfoName.textContent = String(prize.name || "");

  if (prize.imageUrl) {
    dom.prizeInfoImg.src = String(prize.imageUrl);
    dom.prizeInfoImg.style.display = "block";
  } else {
    dom.prizeInfoImg.style.display = "none";
    dom.prizeInfoImg.src = "";
  }
}

function openConfirm(text, onOk) {
  state.pendingConfirm = onOk;
  dom.confirmText.textContent = text;
  openModal(dom.confirmBack);
}

async function uploadMissingPrizeImages(files) {
  const list = Array.from(files || []);
  if (!list.length || !state.me) return;

  const missingMap = new Map(
    state.missingPrizeImages.map((x) => [String(x.imageName || ""), true])
  );

  for (const file of list) {
    const imageName = normalizeImageKey(file.name);
    if (!missingMap.has(imageName)) {
      continue;
    }

    const imageUrl = await uploadImageFile(
      file,
      `prize_masters/${state.me.uid}/${imageName}.webp`
    );
    // 内部ロジック（どの景品を更新するか等）を隠蔽し、リクエストのみ送信
    if (!state.me) return;
    const requestRef = collection(db, "users", state.me.uid, "prize_image_apply_requests");
    await addDoc(requestRef, {
      imageName: String(imageName).trim(),
      imageUrl: String(imageUrl).trim(),
      createdAt: serverTimestamp()
    });
  }

  await refreshPrizeAdminData();
}

async function saveAccountEdit() {
  if (!state.me) return;

  let iconUrl = state.myProfile?.iconUrl || "";
  if (state.accountEditIconFile) {
    iconUrl = await uploadImageFile(
      state.accountEditIconFile,
      `users/${state.me.uid}/icon.webp`
    );
  }

  const nick = dom.accountEditNick.value.trim();

  await setDoc(doc(db, "spinfate_profiles", state.me.uid), {
    nick,
    iconUrl,
    shipping: {
      postal: dom.accountEditPostal.value.trim(),
      addr: dom.accountEditAddr2.value.trim(),
      building: dom.accountEditBuilding.value.trim(),
      phone: dom.accountEditPhone.value.trim()
    },
    shippingAuto: dom.accountEditAddrAuto.value.trim(),
    updatedAt: serverTimestamp()
  }, { merge: true });

  await updateProfile(state.me, {
    displayName: nick || null,
    photoURL: iconUrl || null
  });

  if (isHost()) {
    await setDoc(doc(db, "rooms", state.me.uid), {
      hostNick: nick,
      hostIconUrl: iconUrl,
      updatedAt: serverTimestamp()
    }, { merge: true });
  }

  closeModal(dom.accountEditBack);
}

dom.centerBtn.addEventListener("click", async () => {
  if (!state.room) return;

  if (state.wheel.status === "idle") {
    if (isHostPage && state.repeatConfirmNeeded) {
      const ok = confirm("前回と同じ内容で抽選をしますか？");
      if (!ok) return;
      state.repeatConfirmNeeded = false;
      return;
    }

    if (isHostPage && isHost()) {
      const limit = Math.max(0, Number(state.myProfile?.limit || 0));
      const used = Math.max(0, Number(state.myProfile?.usageUsed || 0));
      const remain = Math.max(0, limit - used);
      if (remain <= 0) {
        alert("今月の抽選回数は終了しました");
        return;
      }
    }

    if (!isRoomPage && !isHostPage) {
      renderRoom({
        ...(state.room || {}),
        phase: 1,
      });
      return;
    }

    if (!isHostPage || !isHost()) return;

    state.wheel.status = "spinning";
    state.wheel.stopHandledAtPhase2 = false;
    state.wheel.finishedCalled = false;
    setCenterButtonByState();
    scheduleTick();
    try {
      const roomRef = doc(db, "rooms", roomUid);
      await updateDoc(roomRef, {
        phase: 1, // これが開始トリガー
        updatedAt: serverTimestamp()
      });
    } catch (e) {
      state.wheel.status = "idle";
      setCenterButtonByState();
      stopRaf();
      drawWheel();
      alert(e?.message || "開始に失敗しました");
    }
    return;
  }

  if (state.wheel.status === "spinning") {
    state.repeatConfirmNeeded = true;
    if (!isRoomPage && !isHostPage) {
      const items = state.items || [];
      if (!items.length) return;
      const picked = pickWeightedIndex(items);
      if (picked < 0) return;
      const offsetRatio = 0.08 + Math.random() * 0.84;
      const finalAngle = calcFinalAngleForIndex(items, picked, offsetRatio);
      renderRoom({
        ...(state.room || {}),
        phase: 2,
        index: picked,
        angle: finalAngle
      });
      return;
    }

    try {
      let canStop = isHostPage && state.me?.uid === roomUid;
      if (!canStop) {
        const q = query(
          collection(db, "joining"),
          where("hostUid", "==", roomUid),
          where("status", "==", 2)
        );
        const snap = await getDocs(q);
        canStop =
          snap.size === 1 &&
          String(snap.docs[0].data()?.participantUid || "") === String(state.me?.uid || "");
      }
      if (!canStop) return;
      const roomRef = doc(db, "rooms", roomUid);
      await updateDoc(roomRef, {
        phase: 2,
        actionByUid: auth.currentUser.uid // バックエンドで判定するためにuidを含める
      });
    } catch (e) {
      alert(e?.message || "停止に失敗しました");
    }
  }
});

dom.btnPlus.addEventListener("click", async () => {
  if (isRoomPage && !isHost()) return;
  await refreshPrizeAdminData();
  await refreshApprovedUsers();
  fillItemEditorFromRoom();
  openModal(dom.modalBack);
});

document.getElementById("editDrawMode").addEventListener("change", () => {
  if (!state.room) state.room = {};
  state.room.drawMode = document.getElementById("editDrawMode").value;
  fillItemEditorFromRoom();
});

dom.modalClose.addEventListener("click", () => closeModal(dom.modalBack));
dom.btnCancel.addEventListener("click", () => closeModal(dom.modalBack));
dom.btnAdd.addEventListener("click", () => {
  const idx = dom.itemList.querySelectorAll(".itemRow").length;
  const mode = document.getElementById("editDrawMode").value;
  dom.itemList.appendChild(makeItemRow("", 1, getNextItemColor(idx), mode));
});

dom.btnSave.addEventListener("click", () => {
  saveRoomFromEditor().catch((e) => alert(e?.message || "保存に失敗しました"));
});

dom.drawImageThumb.addEventListener("click", () => {
  if (!state.room?.imageUrl) return;
  dom.drawImageViewImg.src = state.room.imageUrl;
  openModal(dom.drawImageViewBack);
});
dom.drawImageViewBack.addEventListener("click", (e) => {
  if (e.target === dom.drawImageViewBack || e.target === dom.drawImageViewImg) {
    closeModal(dom.drawImageViewBack);
  }
});

dom.descToggle.addEventListener("click", () => {
  state.descExpanded = !state.descExpanded;
  renderDescription();
});

dom.entryRequestBtn.addEventListener("click", async () => {
  const status = Number(state.entryRequestStatus || 0);
  if (status === 1 || status === 2) return;

  try {
    await requestEntryApproval();
  } catch (e) {
    alert(e?.message || "参加希望の送信に失敗しました");
  }
});

dom.btnEntryRequestsHost.addEventListener("click", () => {
  openModal(dom.entryRequestBack);
});

dom.entryRequestClose.addEventListener("click", () => closeModal(dom.entryRequestBack));
dom.entryRequestCancel.addEventListener("click", () => closeModal(dom.entryRequestBack));

dom.btnLogin.addEventListener("click", () => {
  setAuthMode("login");
  openModal(dom.authBack);
});

dom.authClose.addEventListener("click", () => closeModal(dom.authBack));
dom.authCancel.addEventListener("click", () => closeModal(dom.authBack));
dom.toSignup.addEventListener("click", () => setAuthMode("signup"));
dom.toLogin.addEventListener("click", () => setAuthMode("login"));
dom.authSubmit.addEventListener("click", async (e) => {
  e.preventDefault(); // フォームの意図しない挙動を防ぐ
  dom.authErr.textContent = "";
  
  try {
    if (state.authMode === "login") {
      // ログインに必要な項目だけをその場で確実に取得
      const email = dom.loginEmail.value.trim();
      const pass = dom.loginPass.value;
      
      if (!email || !pass) throw new Error("メールとパスワードを入力してください");
      
      await signInWithEmailAndPassword(auth, email, pass);
      closeModal(dom.authBack);
    } else {
      // サインアップ処理へ
      await doSignup();
    }
  } catch (err) {
    // Firebaseのエラーメッセージを分かりやすく表示
    dom.authErr.textContent = err.message;
  }
});

dom.loginForm?.addEventListener("submit", async (e) => {
  e.preventDefault();
  try {
    dom.authErr.textContent = "";
    await doLogin();
  } catch (e2) {
    dom.authErr.textContent = e2?.message || String(e2);
  }
});

dom.signupForm?.addEventListener("submit", async (e) => {
  e.preventDefault();
  try {
    dom.authErr.textContent = "";
    await doSignup();
  } catch (e2) {
    dom.authErr.textContent = e2?.message || String(e2);
  }
});

dom.signupIcon.addEventListener("change", (e) => {
  const file = e.target.files?.[0];
  state.signupIconFile = file || null;
  if (file) dom.iconPreview.src = URL.createObjectURL(file);
});

dom.shipPostal.addEventListener("input", () => {
  dom.shipPostal.value = dom.shipPostal.value.replace(/\D/g, "").slice(0, 7);
});
dom.shipPostal.addEventListener("blur", () => {
  applyPostalToAddress(dom.shipPostal, dom.shipAddrAuto);
});

dom.accountEditPostal.addEventListener("input", () => {
  dom.accountEditPostal.value = dom.accountEditPostal.value.replace(/\D/g, "").slice(0, 7);
});
dom.accountEditPostal.addEventListener("blur", () => {
  applyPostalToAddress(dom.accountEditPostal, dom.accountEditAddrAuto);
});

dom.accountMenuBtn.addEventListener("click", (e) => {
  e.stopPropagation();
  dom.accountMenu.style.display =
    dom.accountMenu.style.display === "block" ? "none" : "block";
});

document.addEventListener("click", () => closeAllMenus());

dom.btnLogout.addEventListener("click", async () => {
  await signOut(auth);
  closeAllMenus();
});

dom.btnGoRoom.addEventListener("click", () => {
  if (isHostPage || isRoomPage) {
    location.href = "/";
    return;
  }
  location.href = `/host/${state.me?.uid || ""}`;
});

dom.btnAccountEdit.addEventListener("click", async () => {
  closeAllMenus();
  const p = state.myProfile || {};
  dom.accountEditNick.value = p.nick || "";
  dom.accountEditIconPreview.src = p.iconUrl || "";
  dom.accountEditDescription.value = state.room?.description || "";
  dom.accountEditPhone.value = p.shipping?.phone || "";
  dom.accountEditPostal.value = p.shipping?.postal || "";
  dom.accountEditAddrAuto.value = p.shippingAuto || "";
  dom.accountEditAddr2.value = p.shipping?.addr || "";
  dom.accountEditBuilding.value = p.shipping?.building || "";
  state.accountEditIconFile = null;
  openModal(dom.accountEditBack);
});

dom.accountEditClose.addEventListener("click", () => closeModal(dom.accountEditBack));
dom.accountEditCancel.addEventListener("click", () => closeModal(dom.accountEditBack));
dom.accountEditSave.addEventListener("click", () => {
  saveAccountEdit().catch((e) => alert(e?.message || "保存に失敗しました"));
});
dom.accountEditIcon.addEventListener("change", (e) => {
  const file = e.target.files?.[0];
  state.accountEditIconFile = file || null;
  if (file) dom.accountEditIconPreview.src = URL.createObjectURL(file);
});

dom.btnPrize.addEventListener("click", async () => {
  if (!state.me || isFreePlan()) return;

  try {
    await refreshPrizeAdminData();
  } catch (e) {
    console.error(e);
  }

  dom.prizeName.value = "";
  dom.prizePoint.value = "";
  dom.prizeCost.value = "";
  dom.prizeExchangeRate.value = "";
  dom.prizeCsvInput.value = "";
  dom.prizeImageInput.value = "";
  dom.prizeImageSelect.value = "";
  dom.prizeImagePreview.removeAttribute("src");
  state.prizeImageFile = null;

  populatePrizeImageSelect();
  openModal(dom.prizeBack);
});

dom.prizeClose.addEventListener("click", () => closeModal(dom.prizeBack));
dom.prizeCancel.addEventListener("click", () => closeModal(dom.prizeBack));

dom.prizeImageInput.addEventListener("change", (e) => {
  const file = e.target.files?.[0];
  state.prizeImageFile = file || null;

  if (!file) {
    dom.prizeImagePreview.removeAttribute("src");
    return;
  }

  dom.prizeImagePreview.src = URL.createObjectURL(file);
});

dom.prizeImageSelect.addEventListener("change", () => {
  const url = String(dom.prizeImageSelect.value || "").trim();

  if (!url) {
    if (!state.prizeImageFile) {
      dom.prizeImagePreview.removeAttribute("src");
    }
    return;
  }

  dom.prizeImageInput.value = "";
  state.prizeImageFile = null;
  dom.prizeImagePreview.src = url;
});

dom.prizeSave.addEventListener("click", () => {
  savePrizeMaster().catch((e) => alert(e?.message || "景品登録に失敗しました"));
});

dom.prizeCsvSave.addEventListener("click", () => {
  savePrizeMastersCsv().catch((e) => alert(e?.message || "CSV登録に失敗しました"));
});

dom.btnPrizeList.addEventListener("click", async () => {
  closeAllMenus();
  await refreshPrizeAdminData();
  openModal(dom.prizeListBack);
});

dom.btnPrizeImageList.addEventListener("click", async () => {
  closeAllMenus();
  await refreshPrizeAdminData();
  openModal(dom.prizeImageListBack);
});

dom.btnOpenMissingImages.addEventListener("click", async () => {
  await refreshPrizeAdminData();
  openModal(dom.missingImagesBack);
});

dom.prizeListClose.addEventListener("click", () => closeModal(dom.prizeListBack));
dom.prizeListCancel.addEventListener("click", () => closeModal(dom.prizeListBack));
dom.prizeImageListClose.addEventListener("click", () => closeModal(dom.prizeImageListBack));
dom.prizeImageListCancel.addEventListener("click", () => closeModal(dom.prizeImageListBack));
dom.missingImagesClose.addEventListener("click", () => closeModal(dom.missingImagesBack));
dom.missingImagesCancel.addEventListener("click", () => closeModal(dom.missingImagesBack));
dom.confirmClose.addEventListener("click", () => closeModal(dom.confirmBack));
dom.confirmCancel.addEventListener("click", () => closeModal(dom.confirmBack));

dom.confirmOk.addEventListener("click", async () => {
  const fn = state.pendingConfirm;
  closeModal(dom.confirmBack);
  state.pendingConfirm = null;
  if (fn) {
    await fn();
    await refreshPrizeAdminData();
  }
});

dom.prizeListRows.addEventListener("click", (e) => {
  const btn = e.target.closest("[data-prize-delete]");
  if (!btn) return;

  const prizeId = btn.dataset.prizeDelete;
  const prizeName = btn.dataset.prizeName || "";
  openConfirm(`「${prizeName}」を削除しますか？`, async () => {
    // 内部構造を隠蔽した削除リクエストを送信
    if (!state.me) return;
    const requestRef = collection(db, "users", state.me.uid, "prize_deletion_requests");
    await addDoc(requestRef, {
      prizeId: String(prizeId),
      createdAt: serverTimestamp()
    });
  });
});

dom.prizeImageListRows.addEventListener("click", (e) => {
  const delBtn = e.target.closest("[data-image-delete]");
  if (!delBtn) return;

  const imageName = delBtn.dataset.imageDelete || "";
  const usageCount = getPrizeImageUsageCount(imageName);

  openConfirm(
    usageCount > 0
      ? `${usageCount}件でこの画像が使われていますが削除しますか？`
      : `「${imageName}」を削除しますか？`,
    async () => {
      if (!state.me) return;
      // 内部構造を隠蔽し、削除リクエストを送信
      const requestRef = collection(db, "users", state.me.uid, "image_deletion_requests");
      await addDoc(requestRef, {
        imageName: String(imageName).trim(),
        createdAt: serverTimestamp()
      });
    }
  );
});

async function clearMissingPrizeImage(imageName) {
  if (!state.me) return;
  // ユーザー専用の「削除リクエスト用」パスへ書き込み（マスターのパスは隠蔽）
  const requestRef = collection(db, "users", state.me.uid, "missing_image_deletion_requests");

  await addDoc(requestRef, {
    imageName: String(imageName).trim(),
    createdAt: serverTimestamp()
  });
}

dom.missingImagesInput.addEventListener("change", async () => {
  await uploadMissingPrizeImages(dom.missingImagesInput.files);
  dom.missingImagesInput.value = "";
});

dom.missingDropZone.addEventListener("dragover", (e) => {
  e.preventDefault();
});

dom.missingDropZone.addEventListener("drop", async (e) => {
  e.preventDefault();
  await uploadMissingPrizeImages(e.dataTransfer?.files || []);
});

[
  dom.modalBack,
  dom.authBack,
  dom.accountEditBack,
  dom.prizeBack,
  dom.entryRequestBack,
  dom.prizeListBack,
  dom.prizeImageListBack,
  dom.missingImagesBack,
  dom.confirmBack
].forEach((back) => {
  back.addEventListener("click", (e) => {
    if (e.target === back) closeModal(back);
  });
});

onAuthStateChanged(auth, async (user) => {
  state.me = user || null;
  state.myProfile = null;
  state.entryRequestStatus = 0;
  state.pendingEntryRequests = [];

  if (user && roomUid) {
    if (isRoomPage && user.uid === roomUid) {
      location.replace(`/host/${roomUid}`);
      return;
    }
    if (isHostPage && user.uid !== roomUid) {
      location.replace(`/room/${roomUid}`);
      return;
    }
  }

  renderUser();

  if (state.myProfileUnsub) {
    state.myProfileUnsub();
    state.myProfileUnsub = null;
  }
  if (state.myEntryRequestUnsub) {
    state.myEntryRequestUnsub();
    state.myEntryRequestUnsub = null;
  }
  if (state.hostEntryRequestsUnsub) {
    state.hostEntryRequestsUnsub();
    state.hostEntryRequestsUnsub = null;
  }

  if (user) {
    subscribeMyProfile(user.uid);
    if (isHostPage && user.uid === roomUid) {
      await ensureHostRoom().catch(console.error);
    }
  }

  subscribeMyEntryRequest();
  subscribeHostEntryRequests();
  await refreshApprovedUsers();
  renderRemaining();
  renderEntryRequestUi();
});

subscribeRoom();
drawWheel();