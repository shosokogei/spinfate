const { onDocumentCreated, onDocumentUpdated, onDocumentDeleted } = require("firebase-functions/v2/firestore");
const admin = require("firebase-admin");
const { FieldValue } = require("firebase-admin/firestore");

if (!admin.apps.length) {
  admin.initializeApp();
}
const db = admin.firestore();

// --- ユーティリティ関数（ロジック完全維持） ---

function parseRoomList(list) {
  let parsed;
  try {
    parsed = JSON.parse(String(list || "[]"));
  } catch {
    return null;
  }
  if (!Array.isArray(parsed) || parsed.length === 0) return null;
  return parsed;
}

function pickWeightedIndex(parsed) {
  const total = parsed.reduce((s, x) => s + Math.max(1, Number(x[1] || 1)), 0);
  let r = Math.random() * total;
  for (let i = 0; i < parsed.length; i++) {
    r -= Math.max(1, Number(parsed[i][1] || 1));
    if (r < 0) return i;
  }
  return parsed.length - 1;
}

function calcFinalAngleForIndex(parsed, index, offsetRatio = 0.5) {
  const total = parsed.reduce((s, x) => s + Math.max(1, Number(x[1] || 1)), 0);
  let a = -Math.PI / 2;
  for (let i = 0; i < parsed.length; i++) {
    const span = (Math.max(1, Number(parsed[i][1] || 1)) / total) * Math.PI * 2;
    if (i === index) {
      const ratio = Math.max(0.08, Math.min(0.92, Number(offsetRatio || 0.5)));
      const hit = a + span * ratio;
      let angle = -Math.PI / 2 - hit;
      const two = Math.PI * 2;
      angle %= two;
      if (angle < 0) angle += two;
      return angle;
    }
    a += span;
  }
  return 0;
}

// --- 1. スピン開始トリガー (制限チェック込) ---
exports.triggerStartRoomSpin = onDocumentUpdated("rooms/{roomUid}", async (event) => {
  const before = event.data.before.data();
  const after = event.data.after.data();

  if (before.phase === 0 && after.phase === 1) {
    const roomUid = event.params.roomUid;
    const roomRef = event.data.after.ref;
    const profileRef = db.collection("spinfate_profiles").doc(roomUid);
    const [roomSnap, profileSnap] = await Promise.all([roomRef.get(), profileRef.get()]);

    if (!roomSnap.exists) return;

    const profile = profileSnap.exists ? (profileSnap.data() || {}) : {};
    const remain = Math.max(0, Number(profile.limit || 0) - Number(profile.usageUsed || 0));

    if (remain <= 0) {
      await roomRef.update({ phase: 0, updatedAt: FieldValue.serverTimestamp() });
      return;
    }

    await roomRef.set({
      phase: 1,
      indexview: -1,
      updatedAt: FieldValue.serverTimestamp()
    }, { merge: true });
  }
});

// --- 2. スピン停止トリガー (角度計算) ---
exports.triggerStopRoomSpin = onDocumentUpdated("rooms/{roomUid}", async (event) => {
  const before = event.data.before.data();
  const after = event.data.after.data();

  if (before.phase === 1 && after.phase === 2) {
    const roomRef = event.data.after.ref;
    const room = after || {};
    const parsed = parseRoomList(room.list);
    const pickedIndex = Number(room.index);

    if (parsed && Number.isInteger(pickedIndex) && pickedIndex >= 0 && pickedIndex < parsed.length) {
      const offsetRatio = 0.08 + Math.random() * 0.84;
      const finalAngle = calcFinalAngleForIndex(parsed, pickedIndex, offsetRatio);

      await roomRef.update({
        phase: 2,
        index: pickedIndex,
        angle: finalAngle,
        updatedAt: FieldValue.serverTimestamp()
      });
    }
  }
});

// --- 3. スピン完了トリガー (トランザクション処理) ---
exports.triggerFinishRoomSpin = onDocumentUpdated("rooms/{roomUid}", async (event) => {
  const before = event.data.before.data();
  const after = event.data.after.data();

  if (before.phase === 2 && after.phase === 0) {
    const roomUid = event.params.roomUid;
    const roomRef = event.data.after.ref;
    const profileRef = db.collection("spinfate_profiles").doc(roomUid);

    await db.runTransaction(async (tx) => {
      const roomSnap = await tx.get(roomRef);
      if (!roomSnap.exists) return;

      const room = roomSnap.data() || {};
      const pickedIndex = Number(room.index);
      const parsed = parseRoomList(room.list);

      if (parsed && Number.isInteger(pickedIndex) && pickedIndex >= 0 && pickedIndex < parsed.length) {
        tx.set(profileRef, {
          usageUsed: FieldValue.increment(1)
        }, { merge: true });

        tx.set(roomRef, {
          indexview: pickedIndex,
          updatedAt: FieldValue.serverTimestamp()
        }, { merge: true });
      }
    });
  }
});

// --- 4. 参加申請 (entry_requests) ---
exports.requestEntryApproval = onDocumentCreated("entry_requests/{docId}", async (event) => {
  const { participantUid, roomUid } = event.data.data();
  const roomSnap = await db.collection("rooms").doc(roomUid).get();
  if (!roomSnap.exists) {
    await event.data.ref.delete();
    return;
  }
  const profileSnap = await db.collection("spinfate_profiles").doc(participantUid).get();
  const profile = profileSnap.exists ? profileSnap.data() : {};
  const joiningRef = db.collection("joining").doc(`${roomUid}_${participantUid}`);
  const joiningSnap = await joiningRef.get();
  const data = joiningSnap.exists ? joiningSnap.data() : {};

  if (Number(data.status || 0) < 1) {
    await joiningRef.set({
      hostUid: roomUid,
      participantUid,
      status: 1,
      nick: String(profile.nick || ""),
      iconUrl: String(profile.iconUrl || ""),
      updatedAt: FieldValue.serverTimestamp()
    }, { merge: true });
  }
  await event.data.ref.delete();
});

// --- 5. 承認処理 (review_requests) ---
exports.reviewEntryApprovalTrigger = onDocumentCreated("review_requests/{docId}", async (event) => {
  const { roomUid, targetUid, status } = event.data.data();
  const snap = await db.collection("joining")
    .where("hostUid", "==", roomUid)
    .where("participantUid", "==", targetUid)
    .limit(1).get();

  if (!snap.empty) {
    await snap.docs[0].ref.update({
      status,
      reviewedAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp()
    });
  }
  await event.data.ref.delete();
});

// --- 6. 景品保存 (savePrizeMaster) ---
exports.savePrizeMaster = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "login required");
  const d = request.data;
  await db.collection("prize_masters").add({
    hostUid: request.auth.uid,
    name: String(d.name || "").trim(),
    point: Number(d.point || 0),
    cost: Number(d.cost || 0),
    exchangeRate: Number(d.exchangeRate || 0),
    imageUrl: String(d.imageUrl || ""),
    createdAt: FieldValue.serverTimestamp()
  });
  return { ok: true };
});

// --- 7. 画像適用リクエスト ---
exports.onPrizeImageApplyRequested = onDocumentCreated("users/{uid}/prize_image_apply_requests/{requestId}", async (event) => {
  const { imageName, imageUrl } = event.data.data();
  const uid = event.params.uid;
  const batch = db.batch();
  const snap = await db.collection("prize_masters").where("hostUid", "==", uid).get();

  snap.docs.forEach(doc => {
    const cur = String(doc.data().imageUrl || "");
    if (cur === `__missing__:${imageName}` || cur.includes(`${imageName}.webp`)) {
      batch.update(doc.ref, { imageUrl });
    }
  });
  batch.delete(db.collection("missing_prize_images").doc(`${uid}_${imageName}`));
  await batch.commit();
});

// --- 8. 画像削除リクエスト ---
exports.onImageDeletionRequested = onDocumentCreated("users/{uid}/image_deletion_requests/{requestId}", async (event) => {
  const { imageName } = event.data.data();
  const uid = event.params.uid;
  const storagePath = `prize_masters/${uid}/${imageName}.webp`;

  try {
    const bucket = admin.storage().bucket();
    await bucket.file(storagePath).delete();
  } catch (e) {
    if (e.code !== 404) console.error(e);
  }

  const batch = db.batch();
  const snap = await db.collection("prize_masters").where("hostUid", "==", uid).get();
  snap.docs.forEach(doc => {
    if (String(doc.data().imageUrl).includes(`${imageName}.webp`)) {
      batch.update(doc.ref, { imageUrl: `__missing__:${imageName}` });
    }
  });
  batch.set(db.collection("missing_prize_images").doc(`${uid}_${imageName}`), { hostUid: uid, imageName, createdAt: FieldValue.serverTimestamp() }, { merge: true });
  batch.delete(db.collection("users").doc(uid).collection("images").doc(imageName));
  await batch.commit();
});

// --- 9. 不足画像リクエスト ---
exports.onMissingPrizeRequestCreated = onDocumentCreated("users/{uid}/missing_image_requests/{requestId}", async (event) => {
  const { imageName } = event.data.data();
  await db.collection("missing_prize_images").doc(`${event.params.uid}_${imageName}`).set({
    hostUid: event.params.uid,
    imageName,
    createdAt: FieldValue.serverTimestamp()
  }, { merge: true });
});

// --- 10. 同期トリガー群 ---
exports.onPrizeMasterCreated = onDocumentCreated("prize_masters/{id}", async (e) => {
  await db.collection("users").doc(e.data.data().hostUid).collection("prizes").doc(e.params.id).set(e.data.data());
});

exports.onMissingPrizeMasterCreated = onDocumentCreated("missing_prize_images/{id}", async (e) => {
  await db.collection("users").doc(e.data.data().hostUid).collection("missing_images").doc(e.params.id).set(e.data.data());
});

exports.onMissingPrizeMasterDeleted = onDocumentDeleted("missing_prize_images/{id}", async (e) => {
  await db.collection("users").doc(e.data.data().hostUid).collection("missing_images").doc(e.params.id).delete();
});

exports.onPrizeImageSync = onDocumentCreated("prize_masters/{id}", async (e) => {
  const d = e.data.data();
  if (!d.imageUrl || d.imageUrl.startsWith("__missing__:")) return;
  try {
    const u = new URL(d.imageUrl);
    const m = u.pathname.match(/\/o\/(.+)$/);
    const name = decodeURIComponent(m[1]).split("/").pop().replace(/\.webp$/i, "");
    await db.collection("users").doc(d.hostUid).collection("images").doc(name).set({
      imageName: name,
      imageUrl: d.imageUrl,
      updatedAt: FieldValue.serverTimestamp()
    });
  } catch (err) {}
});

// --- 11. ルーム設定リクエスト ---
exports.onRoomConfigRequested = onDocumentCreated("users/{uid}/room_config_requests/{requestId}", async (event) => {
  const data = event.data.data();
  const parsed = parseRoomList(data.list);
  if (!parsed) return;

  const profileSnap = await db.collection("spinfate_profiles").doc(data.roomUid).get();
  const profile = profileSnap.exists ? profileSnap.data() : {};

  await db.collection("rooms").doc(data.roomUid).set({
    ...data,
    angle: 0,
    phase: 0,
    index: pickWeightedIndex(parsed),
    hostIconUrl: String(profile.iconUrl || ""),
    hostNick: String(profile.nick || ""),
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });
});

exports.onMissingImageDeletionRequested = onDocumentCreated("users/{uid}/missing_image_deletion_requests/{requestId}", async (event) => {
  const data = event.data.data();
  if (!data) return;
  
  const uid = event.params.uid;
  const imageName = String(data.imageName || "").trim();
  if (!imageName) return;

  // データベース上の不足画像データを削除する
  await db.collection("missing_prize_images").doc(`${uid}_${imageName}`).delete();
  
  // 処理済みのリクエストを削除して整理する
  await event.data.ref.delete();
});