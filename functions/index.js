const { onCall, HttpsError } = require("firebase-functions/v2/https");
const admin = require("firebase-admin");
const { FieldValue } = require("firebase-admin/firestore");

admin.initializeApp();
const db = admin.firestore();

function parseRoomList(list) {
  let parsed;
  try {
    parsed = JSON.parse(String(list || "[]"));
  } catch {
    throw new HttpsError("invalid-argument", "invalid list");
  }

  if (!Array.isArray(parsed) || parsed.length === 0) {
    throw new HttpsError("invalid-argument", "list required");
  }

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

exports.updateRoomIndex = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }

  const roomUid = String(request.data?.roomUid || "");

  if (!roomUid) {
    throw new HttpsError("invalid-argument", "roomUid required");
  }

  const roomRef = db.collection("rooms").doc(roomUid);
  const snap = await roomRef.get();

  if (!snap.exists) {
    throw new HttpsError("not-found", "room not found");
  }

  const parsed = parseRoomList(snap.data().list);

  await roomRef.update({
    index: pickWeightedIndex(parsed)
  });

  return { ok: true };
});

exports.entrySlot = onCall({ cors: true }, async (request) => {
  if (!request.auth) throw new HttpsError("unauthenticated", "login required");

  const uid = request.auth.uid;
  const roomUid = String(request.data?.roomUid || "");
  const entryCount = request.data?.entryCount;

  if (!roomUid) {
    throw new HttpsError("invalid-argument", "roomUid required");
  }
  const count = Math.max(1, Number(entryCount || 1));

  const roomRef = db.collection("rooms").doc(roomUid);
  const slotRef = db.collection("slots").doc(`${roomUid}_${uid}`);

  return await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRef);
    if (!roomSnap.exists) throw new HttpsError("not-found", "room not found");
    const room = roomSnap.data();
    
    // バリデーション
    if (room.phase !== 0) throw new HttpsError("failed-precondition", "抽選開始済み");
    const max = Number(room.maxSlots || 0);
    const filled = Number(room.filledSlots || 0);
    if (max > 0 && (filled + count) > max) throw new HttpsError("out-of-range", "残口数不足");

    // 更新
    tx.update(roomRef, { 
      filledSlots: FieldValue.increment(count) 
    });
    tx.set(slotRef, { 
      roomUid, uid, count: FieldValue.increment(count), updatedAt: FieldValue.serverTimestamp() 
    }, { merge: true });

    // 満了したら自動スタート（phaseを1にする）
    if (max > 0 && (filled + count) >= max) {
      tx.update(roomRef, { phase: 1 });
    }
    return { ok: true };
  });
});

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

exports.startRoomSpin = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;
  const roomUid = String(request.data?.roomUid || "");

  if (!roomUid) {
    throw new HttpsError("invalid-argument", "roomUid required");
  }
  if (uid !== roomUid) {
    throw new HttpsError("permission-denied", "host only");
  }

  const roomRef = db.collection("rooms").doc(roomUid);
  const profileRef = db.collection("spinfate_profiles").doc(uid);

  const roomSnap = await roomRef.get();

  if (!roomSnap.exists) {
    throw new HttpsError("not-found", "room not found");
  }

  const profileSnap = await profileRef.get();
  const profile = profileSnap.exists ? (profileSnap.data() || {}) : {};
  const limit = Math.max(0, Number(profile.limit || 0));
  const used = Math.max(0, Number(profile.usageUsed || 0));
  const remain = Math.max(0, limit - used);

  if (remain <= 0) {
    throw new HttpsError("failed-precondition", "今月の抽選回数は終了しました");
  }

  await roomRef.set({
    phase: 1,
    indexview: -1,
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });

  return { ok: true };
});

exports.stopRoomSpin = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;
  const roomUid = String(request.data?.roomUid || "");

  if (!roomUid) {
    throw new HttpsError("invalid-argument", "roomUid required");
  }

  if (uid !== roomUid) {
    const joiningSnap = await db.collection("joining")
      .where("hostUid", "==", roomUid)
      .where("status", "==", 2)
      .get();

    const isSingleParticipant =
      joiningSnap.size === 1 &&
      String(joiningSnap.docs[0].data()?.participantUid || "") === uid;

    if (!isSingleParticipant) {
      throw new HttpsError("permission-denied", "host only");
    }
  }

  const roomRef = db.collection("rooms").doc(roomUid);
  const roomSnap = await roomRef.get();

  if (!roomSnap.exists) {
    throw new HttpsError("not-found", "room not found");
  }

  const room = roomSnap.data() || {};
  const parsed = parseRoomList(room.list);

  const pickedIndex = Number(room.index);
  if (!Number.isInteger(pickedIndex) || pickedIndex < 0 || pickedIndex >= parsed.length) {
    throw new HttpsError("failed-precondition", "picked index invalid");
  }

  const offsetRatio = 0.08 + Math.random() * 0.84;
  const finalAngle = calcFinalAngleForIndex(parsed, pickedIndex, offsetRatio);

  await roomRef.set({
    phase: 2,
    index: pickedIndex,
    angle: finalAngle,
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });

  return { ok: true };
});

exports.finishRoomSpin = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;
  const roomUid = String(request.data?.roomUid || "");

  if (!roomUid) {
    throw new HttpsError("invalid-argument", "roomUid required");
  }
  if (uid !== roomUid) {
    throw new HttpsError("permission-denied", "host only");
  }

  const roomRef = db.collection("rooms").doc(roomUid);
  const profileRef = db.collection("spinfate_profiles").doc(uid);

  await db.runTransaction(async (tx) => {
    const roomSnap = await tx.get(roomRef);

    if (!roomSnap.exists) {
      throw new HttpsError("not-found", "room not found");
    }

    const room = roomSnap.data() || {};
    const parsed = parseRoomList(room.list);

    const pickedIndex = Number(room.index);
    if (!Number.isInteger(pickedIndex) || pickedIndex < 0 || pickedIndex >= parsed.length) {
      throw new HttpsError("failed-precondition", "picked index invalid");
    }

    tx.set(profileRef, {
      usageUsed: FieldValue.increment(1)
    }, { merge: true });

    tx.set(roomRef, {
      phase: 0,
      indexview: pickedIndex,
      updatedAt: FieldValue.serverTimestamp()
    }, { merge: true });
  });

  return { ok: true };
});

exports.requestEntryApproval = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const participantUid = request.auth.uid;
  const hostUid = String(request.data?.roomUid || "");

  if (!hostUid) {
    throw new HttpsError("invalid-argument", "roomUid required");
  }
  if (participantUid === hostUid) {
    throw new HttpsError("failed-precondition", "host cannot request own room");
  }

  const roomSnap = await db.collection("rooms").doc(hostUid).get();
  if (!roomSnap.exists) {
    throw new HttpsError("not-found", "room not found");
  }

  const profileSnap = await db.collection("spinfate_profiles").doc(participantUid).get();
  const profile = profileSnap.exists ? (profileSnap.data() || {}) : {};

  const joiningRef = db.collection("joining").doc(`${hostUid}_${participantUid}`);
  const joiningSnap = await joiningRef.get();
  const data = joiningSnap.exists ? (joiningSnap.data() || {}) : {};
  const currentStatus = Number(data.status || 0);

  if (currentStatus === 1) {
    return { ok: true, status: 1 };
  }
  if (currentStatus === 2) {
    return { ok: true, status: 2 };
  }

  await joiningRef.set({
    hostUid,
    participantUid,
    status: 1,
    point: Number(data.point || 0),
    nick: String(profile.nick || ""),
    iconUrl: String(profile.iconUrl || "")
  }, { merge: true });

  return { ok: true, status: 1 };
});

exports.reviewEntryApproval = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const hostUid = request.auth.uid;
  const roomUid = String(request.data?.roomUid || "");
  const targetUid = String(request.data?.targetUid || "");
  const status = Number(request.data?.status || 0);
  if (!roomUid) {
    throw new HttpsError("invalid-argument", "roomUid required");
  }
  if (hostUid !== roomUid) {
    throw new HttpsError("permission-denied", "host only");
  }
  if (!targetUid) {
    throw new HttpsError("invalid-argument", "targetUid required");
  }
  if (status !== 2 && status !== 3) {
    throw new HttpsError("invalid-argument", "status invalid");
  }

  const joiningSnap = await db.collection("joining")
    .where("hostUid", "==", hostUid)
    .where("participantUid", "==", targetUid)
    .limit(1)
    .get();

  if (joiningSnap.empty) {
    throw new HttpsError("not-found", "request not found");
  }

  const ref = joiningSnap.docs[0].ref;
  const data = joiningSnap.docs[0].data() || {};

  await ref.set({
    hostUid,
    participantUid: targetUid,
    status,
    point: Number(data.point || 0),
    reviewedAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });

  return { ok: true, status };
});

exports.savePrizeMaster = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;

  const name = String(request.data?.name || "").trim();
  const point = Math.max(0, Number(request.data?.point || 0) || 0);
  const cost = Math.max(0, Number(request.data?.cost || 0) || 0);
  const exchangeRate = Math.max(0, Math.min(100, Number(request.data?.exchangeRate || 0) || 0));
  const imageUrl = String(request.data?.imageUrl || "").trim();

  if (!name) {
    throw new HttpsError("invalid-argument", "name required");
  }

  const prizeRef = db.collection("prize_masters").doc();

  await prizeRef.set({
    hostUid: uid,
    name,
    point,
    cost,
    exchangeRate,
    imageUrl
  });

  return { ok: true, prizeMasterId: prizeRef.id };
});

exports.addMissingPrizeImage = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;

  const imageName = String(request.data?.imageName || "").trim();
  if (!imageName) {
    throw new HttpsError("invalid-argument", "imageName required");
  }

  await db.collection("missing_prize_images").doc(`${uid}_${imageName}`).set({
    hostUid: uid,
    imageName
  }, { merge: true });

  return { ok: true };
});

exports.clearMissingPrizeImage = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;

  const imageName = String(request.data?.imageName || "").trim();
  if (!imageName) {
    throw new HttpsError("invalid-argument", "imageName required");
  }

  await db.collection("missing_prize_images").doc(`${uid}_${imageName}`).delete();
  return { ok: true };
});

exports.listPrizeMasters = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;

  const snap = await db.collection("prize_masters")
    .where("hostUid", "==", uid)
    .get();

  const items = snap.docs
    .map((d) => ({ id: d.id, ...(d.data() || {}) }))
    .sort((a, b) => String(a.name || "").localeCompare(String(b.name || ""), "ja"));

  return { ok: true, items };
});

exports.deletePrizeMaster = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;
  const prizeId = String(request.data?.prizeId || "");

  if (!prizeId) {
    throw new HttpsError("invalid-argument", "prizeId required");
  }

  const ref = db.collection("prize_masters").doc(prizeId);
  const snap = await ref.get();

  if (!snap.exists) {
    throw new HttpsError("not-found", "prize not found");
  }

  const data = snap.data() || {};
  if (String(data.hostUid || "") !== uid) {
    throw new HttpsError("permission-denied", "host only");
  }

  await ref.delete();
  return { ok: true };
});

exports.listMissingPrizeImages = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;

  const [missingSnap, prizeSnap] = await Promise.all([
    db.collection("missing_prize_images")
      .where("hostUid", "==", uid)
      .get(),
    db.collection("prize_masters")
      .where("hostUid", "==", uid)
      .get()
  ]);

  const map = new Map();

  missingSnap.docs.forEach((d) => {
    const data = d.data() || {};
    const imageName = String(data.imageName || "").trim();
    if (!imageName) return;

    map.set(imageName, {
      id: d.id,
      hostUid: uid,
      imageName
    });
  });

  prizeSnap.docs.forEach((d) => {
    const data = d.data() || {};
    const imageUrl = String(data.imageUrl || "").trim();
    if (!imageUrl.startsWith("__missing__:")) return;

    const imageName = imageUrl.slice("__missing__:".length).trim();
    if (!imageName) return;

    if (!map.has(imageName)) {
      map.set(imageName, {
        id: `${uid}_${imageName}`,
        hostUid: uid,
        imageName
      });
    }
  });

  const items = Array.from(map.values())
    .sort((a, b) => String(a.imageName || "").localeCompare(String(b.imageName || ""), "ja"));

  return { ok: true, items };
});

exports.applyPrizeImage = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;

  const imageName = String(request.data?.imageName || "").trim();
  const imageUrl = String(request.data?.imageUrl || "").trim();

  if (!imageName) {
    throw new HttpsError("invalid-argument", "imageName required");
  }
  if (!imageUrl) {
    throw new HttpsError("invalid-argument", "imageUrl required");
  }

  const storagePath = `prize_masters/${uid}/${imageName}.webp`;
  const encodedPath = encodeURIComponent(storagePath);

  const snap = await db.collection("prize_masters")
    .where("hostUid", "==", uid)
    .get();

  const batch = db.batch();

  snap.docs.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const currentUrl = String(data.imageUrl || "");
    if (
      currentUrl === `__missing__:${imageName}` ||
      currentUrl.includes(`${imageName}.webp`)
    ) {
      batch.set(docSnap.ref, { imageUrl }, { merge: true });
    }
  });

  batch.delete(db.collection("missing_prize_images").doc(`${uid}_${imageName}`));
  await batch.commit();

  return { ok: true };
});

exports.listPrizeImages = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;

  const snap = await db.collection("prize_masters")
    .where("hostUid", "==", uid)
    .get();

  const map = new Map();

  snap.docs.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const imageUrl = String(data.imageUrl || "").trim();

    if (!imageUrl || imageUrl.startsWith("__missing__:")) {
      return;
    }

    let imageName = "";

    try {
      const u = new URL(imageUrl);
      const m = u.pathname.match(/\/o\/(.+)$/);
      const storagePath = m ? decodeURIComponent(m[1]) : "";
      const fileName = storagePath.split("/").pop() || "";
      imageName = fileName.replace(/\.webp$/i, "");
    } catch {
      imageName = "";
    }

    if (!imageName) {
      return;
    }

    if (!map.has(imageName)) {
      map.set(imageName, {
        imageName,
        imageUrl
      });
    }
  });

  const items = Array.from(map.values())
    .sort((a, b) => String(a.imageName || "").localeCompare(String(b.imageName || ""), "ja"));

  return { ok: true, items };
});

exports.deletePrizeImage = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;
  const imageName = String(request.data?.imageName || "").trim();

  if (!imageName) {
    throw new HttpsError("invalid-argument", "imageName required");
  }

  const storagePath = `prize_masters/${uid}/${imageName}.webp`;

  try {
    const bucket = process.env.FUNCTIONS_EMULATOR
      ? admin.storage().bucket()
      : admin.storage().bucket("spinfate1.appspot.com");

    await bucket.file(storagePath).delete();
  } catch (e) {
    if (e.code !== 404) throw e;
  }

  const imageUrlPrefix = `https://firebasestorage.googleapis.com/`;
  const snap = await db.collection("prize_masters")
    .where("hostUid", "==", uid)
    .get();

  const batch = db.batch();

  snap.docs.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const imageUrl = String(data.imageUrl || "");
    if (
      imageUrl === `__missing__:${imageName}` ||
      imageUrl.includes(encodeURIComponent(storagePath)) ||
      imageUrl.includes(`${imageName}.webp`)
    ) {
      batch.set(docSnap.ref, { imageUrl: `__missing__:${imageName}` }, { merge: true });
      batch.set(
        db.collection("missing_prize_images").doc(`${uid}_${imageName}`),
        { hostUid: uid, imageName },
        { merge: true }
      );
    }
  });
  await batch.commit();
  await db.collection("missing_prize_images").doc(`${uid}_${imageName}`).set({
    hostUid: uid,
    imageName
  }, { merge: true });
  return { ok: true };
});

exports.saveRoomConfig = onCall({ cors: true }, async (request) => {
  if (!request.auth) {
    throw new HttpsError("unauthenticated", "login required");
  }
  const uid = request.auth.uid;
  const roomUid = String(request.data?.roomUid || "");

  if (!roomUid) {
    throw new HttpsError("invalid-argument", "roomUid required");
  }
  if (uid !== roomUid) {
    throw new HttpsError("permission-denied", "host only");
  }

  const title = String(request.data?.title || "").trim();
  const description = String(request.data?.description || "");
  const list = String(request.data?.list || "[]");
  const drawMode = String(request.data?.drawMode || "A"); 
  const maxSlots = Math.max(0, Number(request.data?.maxSlots) || 0);
  const userMax = Math.max(0, Number(request.data?.userMax) || 0);
  const targetPrizeId = String(request.data?.targetPrizeId || "").trim();
  const point = Math.max(0, Number(request.data?.point || 0) || 0);

  let parsed;
  try {
    parsed = JSON.parse(list);
  } catch {
    throw new HttpsError("invalid-argument", "invalid list");
  }

  if (!Array.isArray(parsed) || parsed.length === 0) {
    throw new HttpsError("invalid-argument", "list required");
  }

  const roomRef = db.collection("rooms").doc(roomUid);
  const profileRef = db.collection("spinfate_profiles").doc(uid);
  const profileSnap = await profileRef.get();
  const profile = profileSnap.exists ? profileSnap.data() : {};

  const nextIndex = (() => {
    const total = parsed.reduce((s, x) => s + Math.max(1, Number(x[1] || 1)), 0);
    let r = Math.random() * total;
    for (let i = 0; i < parsed.length; i++) {
      r -= Math.max(1, Number(parsed[i][1] || 1));
      if (r < 0) return i;
    }
    return parsed.length - 1;
  })();

  await roomRef.set({
    angle: 0,
    hostIconUrl: String(profile?.iconUrl || ""),
    hostNick: String(profile?.nick || ""),
    description,
    list,
    phase: 0,
    index: nextIndex,
    title,
    targetPrizeId,
    point,
    updatedAt: FieldValue.serverTimestamp(),
    drawMode,
    maxSlots,
    userMax,
    filledSlots: 0
  }, { merge: true });

  return { ok: true };
});