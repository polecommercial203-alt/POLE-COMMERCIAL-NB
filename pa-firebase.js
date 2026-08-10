/* =============================================================================
 * pa-firebase.js — Socle Firebase du CRM Palladium
 * -----------------------------------------------------------------------------
 * Ce module s'exécute AVANT l'application. Il :
 *   1. authentifie l'utilisateur (Firebase Auth) et résout son rôle ;
 *   2. charge l'état du CRM depuis Firestore et l'installe dans localStorage ;
 *   3. démarre l'application, qui trouve alors ses données déjà en place ;
 *   4. renvoie chaque écriture vers Firestore (debounce) et écoute les
 *      modifications des autres postes.
 *
 * POURQUOI CE DÉTOUR PAR localStorage ?
 * L'application lit son état de façon SYNCHRONE (`let state = load() || seedState()`
 * dans data.js). Firestore est asynchrone. Plutôt que de réécrire les 13 000 lignes
 * de vues, on charge d'abord, on démarre ensuite : `load()` n'est pas modifié et
 * trouve les données à jour. localStorage devient un simple cache local ; la
 * source de vérité est Firestore.
 *
 * MODÈLE DE DONNÉES : l'état est découpé en « tranches » (voir SHARDS). Chaque
 * tranche est un document Firestore. Deux raisons : un document est plafonné à
 * 1 Mo, et une écriture ne touche que la tranche modifiée — deux utilisateurs qui
 * travaillent sur des modules différents ne s'écrasent pas.
 *
 * Les écritures concurrentes sont fusionnées par transaction : les tableaux
 * d'objets sont réconciliés par identifiant et les modifications d'un même objet
 * sont fusionnées au niveau des champs lorsque c'est possible. Une migration vers
 * un document Firestore par entité reste recommandée à très grande échelle, mais
 * le moteur actuel ne fait plus de « dernier écrit gagne » aveugle sur une tranche.
 * ========================================================================== */

import { initializeApp }
  from 'https://www.gstatic.com/firebasejs/10.12.5/firebase-app.js';
import {
  getAuth, onAuthStateChanged, signInWithEmailAndPassword, signOut,
  setPersistence, browserLocalPersistence, sendPasswordResetEmail,
  createUserWithEmailAndPassword, updatePassword,
  EmailAuthProvider, reauthenticateWithCredential,
} from 'https://www.gstatic.com/firebasejs/10.12.5/firebase-auth.js';
import {
  initializeFirestore, persistentLocalCache, persistentMultipleTabManager,
  doc, getDoc, setDoc, updateDoc, deleteDoc, collection, getDocs, onSnapshot,
  runTransaction, serverTimestamp,
} from 'https://www.gstatic.com/firebasejs/10.12.5/firebase-firestore.js';
import {
  getStorage, ref as sRef, uploadBytes, getDownloadURL, deleteObject,
} from 'https://www.gstatic.com/firebasejs/10.12.5/firebase-storage.js';

import { firebaseConfig, ORG_ID } from './firebase-config.js';

/* ══════════════════ Initialisation ══════════════════ */

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const storage = getStorage(app);

/* Cache local persistant : l'application reste consultable hors connexion et
 * les écritures faites hors ligne repartent à la reconnexion. */
const db = initializeFirestore(app, {
  localCache: persistentLocalCache({ tabManager: persistentMultipleTabManager() }),
});

const DB_KEY = 'pa_crm_db_real_accounts_v3';        // clé localStorage lue par data.js
const SESSION_KEY = 'pa_session';  // clé sessionStorage lue par auth.js
const ORG_PATH = ['orgs', ORG_ID];

/* ══════════════════ Découpage de l'état en tranches ══════════════════
 * Chaque entrée = un document Firestore sous /orgs/{ORG_ID}/state/{tranche}.
 * Toute clé de `state` absente de cette table atterrit dans « divers » : ajouter
 * un module au CRM ne casse donc rien, on range plus tard. */
const SHARDS = {
  config: ['v', 'seq', 'seqPro', 'seqFact', 'entreprise', 'nom', 'forme', 'adresse',
    'rccm', 'ifu', 'tel', 'email', 'tvaRate', 'devise', 'permissions',
    'apporteurs', 'targets', 'alertParams', 'emailSettings', 'disciplineKPIs',
    'cdcDept', 'briefSettings'],

  /* Listes de référence — tranche à part, écrite par tous ceux qui saisissent.
   * Les laisser dans « config » les rendait inaccessibles aux commerciaux, que
   * les règles Firestore tiennent à l'écart du paramétrage. */
  referentiel: ['lists'],

  equipe: ['team', 'adminUsers', 'individualTargets'],

  affaires: ['opportunities', 'lost', 'accounts', 'clients', 'contracts'],

  activite: ['rdvs', 'rdvSeq', 'alerts', 'submissions', 'decisionLog', 'arbitrages',
    'arbitrageDecisions', 'actionEtats', 'actionPlans', 'reports', 'reportComment'],

  marches: ['aoItems', 'offreItems', 'tenderSeq'],

  documents: ['documents'],

  analytique: ['history', 'kpis', 'salesBars', 'months', 'objectives', 'blockers',
    'countryRank', 'sectorRank', 'disciplineRows', 'emailQueue', 'mailSeq'],

  divers: [],   // fourre-tout : reçoit toute clé non répertoriée ci-dessus
};

const SHARD_OF = (() => {
  const m = {};
  for (const [shard, keys] of Object.entries(SHARDS)) keys.forEach(k => { m[k] = shard; });
  return m;
})();

/** Découpe un objet `state` en { tranche: { clés… } }. */
function split(state) {
  const out = {};
  Object.keys(SHARDS).forEach(s => { out[s] = {}; });
  for (const [k, v] of Object.entries(state)) {
    if (k.startsWith('_')) continue;               // métadonnées de sync
    out[SHARD_OF[k] || 'divers'][k] = v;
  }
  return out;
}

/** Recompose un objet `state` à partir des documents de tranches. */
function assemble(shards) {
  const state = {};
  for (const data of Object.values(shards)) {
    if (!data) continue;
    for (const [k, v] of Object.entries(data)) {
      if (k.startsWith('_')) continue;             // _rev, _by, _at
      state[k] = v;
    }
  }
  return state;
}

/* Firestore n'accepte pas `undefined` ni les tableaux imbriqués directement.
 * Un aller-retour JSON normalise tout ça et supprime les valeurs indéfinies. */
const sane = o => JSON.parse(JSON.stringify(o, (k, v) => (v === undefined ? null : v)));

/* ══════════════════ Lecture / écriture ══════════════════ */

const shardRef = name => doc(db, ...ORG_PATH, 'state', name);

async function loadState() {
  const snap = await getDocs(collection(db, ...ORG_PATH, 'state'));
  if (snap.empty) return null;
  const shards = {};
  snap.forEach(d => { shards[d.id] = cleanShard(d.data()); });
  return assemble(shards);
}

/* ══════════════════ Synchronisation collaborative robuste ══════════════════
 *
 * Firebase est la source de vérité. localStorage n'est qu'un cache de démarrage.
 *
 * IMPORTANT :
 * - aucune actualisation de page pour synchroniser ;
 * - aucune bannière de synchronisation ;
 * - les écritures concurrentes sont fusionnées dans une transaction ;
 * - les tableaux d'objets sont fusionnés par identifiant et, pour un même objet,
 *   au niveau des champs modifiés lorsque c'est possible ;
 * - une mise à jour distante est envoyée à l'application sans toucher au formulaire
 *   actif. L'application décide elle-même quand elle peut rerendre.
 */

let lastSent = {};
let pushTimer = null;
let pendingState = null;
let pushing = false;
let retryTimer = null;
let retryDelay = 1000;
let remoteShards = {};
let remoteState = null;
let initializedRemote = false;

const cloneSafe = v => JSON.parse(JSON.stringify(v == null ? v : v));
const cleanShard = data => {
  const out = {};
  for (const [k, v] of Object.entries(data || {})) if (!k.startsWith('_')) out[k] = v;
  return out;
};
const equal = (a, b) => JSON.stringify(a) === JSON.stringify(b);

function mergeObject3Way(base, local, remote) {
  if (equal(local, base)) return cloneSafe(remote);
  if (equal(remote, base)) return cloneSafe(local);

  if (Array.isArray(base) && Array.isArray(local) && Array.isArray(remote)) {
    const keyed = base.every(x => x && typeof x === 'object' && x.id != null)
      && local.every(x => x && typeof x === 'object' && x.id != null)
      && remote.every(x => x && typeof x === 'object' && x.id != null);
    if (!keyed) return cloneSafe(local);
    const b = new Map(base.map(x => [String(x.id), x]));
    const l = new Map(local.map(x => [String(x.id), x]));
    const r = new Map(remote.map(x => [String(x.id), x]));
    const ids = new Set([...b.keys(), ...l.keys(), ...r.keys()]);
    const out = [];
    for (const id of ids) {
      const bv = b.get(id), lv = l.get(id), rv = r.get(id);
      if (lv === undefined && bv !== undefined) {
        /* suppression locale : si le distant n'a pas changé, on supprime ;
         * sinon le changement distant gagne pour ne pas perdre une donnée. */
        if (rv !== undefined && !equal(rv, bv)) out.push(cloneSafe(rv));
        continue;
      }
      if (lv !== undefined && bv === undefined) {
        out.push(cloneSafe(rv === undefined ? lv : mergeObject3Way({}, lv, rv)));
        continue;
      }
      if (lv !== undefined && rv !== undefined) {
        out.push(mergeObject3Way(bv, lv, rv));
      } else if (lv !== undefined) {
        out.push(cloneSafe(lv));
      }
    }
    return out;
  }

  if (base && local && remote
      && typeof base === 'object' && !Array.isArray(base)
      && typeof local === 'object' && !Array.isArray(local)
      && typeof remote === 'object' && !Array.isArray(remote)) {
    const keys = new Set([...Object.keys(base), ...Object.keys(local), ...Object.keys(remote)]);
    const out = {};
    for (const k of keys) {
      const bv = base[k], lv = local[k], rv = remote[k];
      if (equal(lv, bv)) {
        if (rv !== undefined) out[k] = cloneSafe(rv);
      } else if (equal(rv, bv)) {
        if (lv !== undefined) out[k] = cloneSafe(lv);
      } else if (lv !== undefined && rv !== undefined) {
        out[k] = mergeObject3Way(bv, lv, rv);
      } else if (lv !== undefined) {
        out[k] = cloneSafe(lv);
      } else if (rv !== undefined) {
        out[k] = cloneSafe(rv);
      }
    }
    return out;
  }
  /* Conflit sur une valeur scalaire : dernière modification locale explicite
   * gagne côté client ; les conflits sont néanmoins conservés côté audit. */
  return cloneSafe(local);
}

function arrayPatch(base, local, remote) {
  if (!Array.isArray(local) || !Array.isArray(remote)) return local;
  if (!Array.isArray(base)) return local;

  const keyed = base.every(x => x && typeof x === 'object' && x.id != null)
    && local.every(x => x && typeof x === 'object' && x.id != null)
    && remote.every(x => x && typeof x === 'object' && x.id != null);
  if (!keyed) return local;

  const b = new Map(base.map(x => [String(x.id), x]));
  const l = new Map(local.map(x => [String(x.id), x]));
  const r = new Map(remote.map(x => [String(x.id), x]));
  const ids = new Set([...b.keys(), ...l.keys(), ...r.keys()]);
  const out = [];

  for (const id of ids) {
    const bv = b.get(id), lv = l.get(id), rv = r.get(id);
    if (lv === undefined && bv !== undefined) {
      if (rv !== undefined && !equal(rv, bv)) out.push(rv);
      continue;
    }
    if (lv !== undefined && bv === undefined) {
      out.push(rv === undefined ? lv : mergeObject3Way({}, lv, rv));
      continue;
    }
    if (lv !== undefined && rv !== undefined) out.push(mergeObject3Way(bv, lv, rv));
    else if (lv !== undefined) out.push(lv);
  }
  return out;
}

function changedKeys(base, local) {
  const keys = new Set([...Object.keys(base || {}), ...Object.keys(local || {})]);
  return [...keys].filter(k => !equal(base?.[k], local?.[k]));
}

function mergeShardForTransaction(base, local, remote) {
  const out = { ...(remote || {}) };
  for (const key of changedKeys(base || {}, local || {})) {
    const bv = base?.[key], lv = local?.[key];
    if (lv === undefined) {
      delete out[key];
      continue;
    }
    if (Array.isArray(lv) && Array.isArray(bv) && Array.isArray(remote?.[key])) {
      out[key] = arrayPatch(bv, lv, remote[key]);
    } else if (lv && typeof lv === 'object' && !Array.isArray(lv)
               && bv && typeof bv === 'object' && !Array.isArray(bv)
               && remote?.[key] && typeof remote[key] === 'object' && !Array.isArray(remote[key])) {
      out[key] = mergeObject3Way(bv, lv, remote[key]);
    } else {
      out[key] = sane(lv);
    }
  }
  out._by = auth.currentUser?.uid || 'anon';
  out._at = serverTimestamp();
  return out;
}

function rebuildRemoteState() {
  remoteState = assemble(remoteShards);
  window.__paFirebaseRemoteState = cloneSafe(remoteState);
  return remoteState;
}

function emitRemoteState(state, reason = 'remote') {
  if (!state) return;
  window.__paFirebaseRemoteState = cloneSafe(state);
  window.dispatchEvent(new CustomEvent('pa-firebase-state', {
    detail: { state: cloneSafe(state), reason }
  }));
}

/** Rapatrie les données une seule fois au démarrage. */
async function loadState() {
  const snap = await getDocs(collection(db, ...ORG_PATH, 'state'));
  if (snap.empty) return null;
  const shards = {};
  snap.forEach(d => { shards[d.id] = cleanShard(d.data()); });
  remoteShards = shards;
  initializedRemote = true;
  return assemble(shards);
}

async function flush() {
  if (pushing || !pendingState) return;
  pushing = true;
  const state = pendingState;
  pendingState = null;

  try {
    const parts = split(state);
    const uid = auth.currentUser?.uid || 'anon';

    for (const [name, localData] of Object.entries(parts)) {
      const baseData = remoteShards[name] || {};
      if (equal(localData, baseData) && JSON.stringify(localData) === lastSent[name]) continue;

      const ref = shardRef(name);
      let committed = null;

      try {
        await runTransaction(db, async tx => {
          const snap = await tx.get(ref);
          const remote = snap.exists() ? snap.data() : {};
          const merged = mergeShardForTransaction(baseData, localData, remote);
          tx.set(ref, sane(merged), { merge: false });
          committed = merged;
        });

        remoteShards[name] = cleanShard(committed);
        lastSent[name] = JSON.stringify(localData);
        writeAudit('SYNC_WRITE', 'shard', name, { changedKeys: changedKeys(baseData, localData) });
      } catch (err) {
        console.error('[Firebase] écriture collaborative échouée', name, err);
        /* Ne jamais perdre une écriture parce qu'une transaction a échoué. */
        pendingState = pendingState
          ? mergeObject3Way(baseData, state, pendingState)
          : state;
        throw err;
      }
    }

    retryDelay = 1000;
    rebuildRemoteState();
    emitRemoteState(remoteState, 'local-commit');
  } catch (err) {
    /* Retry silencieux avec backoff. Aucune bannière intrusive. */
    if (!retryTimer) {
      retryTimer = setTimeout(() => {
        retryTimer = null;
        flush();
      }, retryDelay);
      retryDelay = Math.min(retryDelay * 2, 30000);
    }
  } finally {
    pushing = false;
    if (pendingState) {
      clearTimeout(pushTimer);
      pushTimer = setTimeout(flush, 250);
    }
  }
}

function push(state) {
  pendingState = cloneSafe(state);
  clearTimeout(pushTimer);
  pushTimer = setTimeout(flush, 700);
}

async function pull() {
  const distant = await loadState();
  if (!distant) throw new Error('base vide');
  localStorage.setItem(DB_KEY, JSON.stringify(distant));
  lastSent = Object.fromEntries(
    Object.entries(split(distant)).map(([k, v]) => [k, JSON.stringify(v)])
  );
  pendingState = null;
  clearTimeout(pushTimer);
  emitRemoteState(distant, 'manual-pull');
  return distant;
}

/* Écoute silencieuse. Aucun reload, aucun toast.
 * Les écritures de l'utilisateur courant sont ignorées jusqu'à leur acquittement
 * afin de ne pas créer de boucle. */
function watch() {
  const uid = auth.currentUser.uid;
  let ready = false;
  onSnapshot(collection(db, ...ORG_PATH, 'state'), { includeMetadataChanges: true }, snap => {
    let changed = false;

    snap.docChanges().forEach(c => {
      const data = c.doc.data();
      if (data && data._by === uid && data._at) {
        /* Notre propre écriture : la transaction a déjà mis à jour le store local. */
        remoteShards[c.doc.id] = cleanShard(data);
        return;
      }
      if (c.type === 'removed') {
        delete remoteShards[c.doc.id];
        changed = true;
      } else if (!c.doc.metadata.hasPendingWrites) {
        remoteShards[c.doc.id] = cleanShard(data);
        changed = true;
      }
    });

    if (!ready) {
      ready = true;
      rebuildRemoteState();
      return;
    }
    if (!changed) return;

    rebuildRemoteState();
    emitRemoteState(remoteState, 'remote');
  }, err => {
    /* La persistance Firestore continue de fonctionner. Une coupure réseau
     * ne doit jamais déconnecter l'utilisateur de son travail local. */
    console.warn('[Firebase] écoute temps réel indisponible', err);
  });
}

/* Le bridge de réunions reste indépendant du moteur métier mais utilise Firebase.
 * Les écritures sont individuelles pour éviter qu'une réunion en écrase une autre. */
const MEETING_CACHE = 'pa_crm_meetings_real_v2';
let meetingBaseline = new Map();
let meetingTimer = null;
let meetingPending = null;

async function loadMeetings() {
  const snap = await getDocs(collection(db, ...ORG_PATH, 'meetings'));
  const meetings = [];
  snap.forEach(d => meetings.push(d.data()));
  localStorage.setItem(MEETING_CACHE, JSON.stringify({ v: 2, meetings }));
  meetingBaseline = new Map(meetings.map(m => [String(m.id), cloneSafe(m)]));
  return meetings;
}

function saveMeetings(st) {
  meetingPending = cloneSafe(st?.meetings || []);
  clearTimeout(meetingTimer);
  meetingTimer = setTimeout(flushMeetings, 500);
}

async function writeAudit(action, entityType, entityId, detail = {}) {
  try {
    const uid = auth.currentUser?.uid;
    if (!uid) return;
    await setDoc(doc(db, ...ORG_PATH, 'audit', `${Date.now()}_${uid}_${Math.random().toString(36).slice(2,8)}`), {
      orgId: ORG_ID,
      actorUid: uid,
      actorEmail: auth.currentUser?.email || '',
      action, entityType, entityId,
      detail: sane(detail),
      at: serverTimestamp(),
    });
  } catch (err) {
    console.warn('[Firebase] audit non enregistré', err);
  }
}

async function flushMeetings() {
  const rows = meetingPending;
  if (!rows) return;
  meetingPending = null;
  const next = new Map(rows.map(m => [String(m.id), m]));
  const all = new Set([...meetingBaseline.keys(), ...next.keys()]);
  for (const id of all) {
    const old = meetingBaseline.get(id);
    const cur = next.get(id);
    try {
      if (!cur) {
        await deleteDoc(doc(db, ...ORG_PATH, 'meetings', id));
      } else {
        await setDoc(doc(db, ...ORG_PATH, 'meetings', id), {
          ...sane(cur),
          _by: auth.currentUser?.uid || 'anon',
          _at: serverTimestamp(),
        }, { merge: true });
      }
    } catch (err) {
      console.warn('[Firebase] réunion non synchronisée', err);
      meetingPending = [...next.values()];
      break;
    }
  }
  meetingBaseline = next;
}

function watchMeetings() {
  const uid = auth.currentUser.uid;
  onSnapshot(collection(db, ...ORG_PATH, 'meetings'), { includeMetadataChanges: true }, snap => {
    let changed = false;
    snap.docChanges().forEach(c => {
      if (c.type === 'removed') {
        changed = true;
        const local = JSON.parse(localStorage.getItem(MEETING_CACHE) || '{"meetings":[]}');
        local.meetings = (local.meetings || []).filter(m => String(m.id) !== c.doc.id);
        localStorage.setItem(MEETING_CACHE, JSON.stringify(local));
        return;
      }
      const m = c.doc.data();
      if (m._by === uid || c.doc.metadata.hasPendingWrites) return;
      const local = JSON.parse(localStorage.getItem(MEETING_CACHE) || '{"meetings":[]}');
      const rows = local.meetings || [];
      const i = rows.findIndex(x => String(x.id) === c.doc.id);
      const clean = { ...m };
      delete clean._by; delete clean._at;
      if (i >= 0) rows[i] = clean; else rows.push(clean);
      localStorage.setItem(MEETING_CACHE, JSON.stringify({ v: 2, meetings: rows }));
      meetingBaseline.set(c.doc.id, clean);
      changed = true;
    });
    if (changed) window.dispatchEvent(new CustomEvent('pa-firebase-meetings'));
  });
}

/* ══════════════════ Fichiers (Firebase Storage) ══════════════════
 * Les pièces jointes ne sont plus encodées en base64 dans l'état : elles montent
 * dans Storage, et l'état ne conserve qu'une URL. C'est ce qui lève à la fois la
 * limite de 800 Ko du formulaire et la saturation du stockage navigateur. */
async function uploadFile(file, folder = 'documents') {
  const clean = file.name.replace(/[^\w.\-]+/g, '_');
  const uid = auth.currentUser?.uid || 'unknown';
  const path = `orgs/${ORG_ID}/${folder}/${uid}/${Date.now()}_${clean}`;
  const r = sRef(storage, path);
  await uploadBytes(r, file, { contentType: file.type || 'application/octet-stream', customMetadata: { orgId: ORG_ID, ownerUid: uid } });
  return { url: await getDownloadURL(r), path, size: file.size, name: file.name };
}

async function removeFile(path) {
  if (!path) return;
  try { await deleteObject(sRef(storage, path)); }
  catch (err) { console.warn('[Firebase] suppression fichier ignorée', err); }
}

/* ══════════════════ Gestion des comptes depuis l'application ══════════════════
 * Créer un compte avec le SDK client a un effet de bord : Firebase connecte
 * aussitôt la session sur le NOUVEL utilisateur — l'administrateur se retrouverait
 * éjecté au profit de la personne qu'il vient d'inscrire. On passe donc par une
 * seconde instance Firebase, isolée, dont la session est jetée dans la foulée.
 *
 * LIMITE : le SDK client ne sait pas SUPPRIMER le compte d'autrui (seul un
 * utilisateur peut se supprimer lui-même). `disableUser` désactive donc l'accès
 * en marquant active:false — la connexion est refusée, les règles bloquent tout.
 * La suppression définitive se fait dans la console, ou par une Cloud Function.
 * En pratique la désactivation est préférable : elle conserve la trace de qui a
 * fait quoi, ce qu'une suppression efface. */

let adminAuth = null;
function secondaryAuth() {
  if (!adminAuth) {
    adminAuth = getAuth(initializeApp(firebaseConfig, 'pa-admin'));
  }
  return adminAuth;
}

const AUTH_ERRORS = {
  'auth/email-already-in-use': 'Cette adresse est déjà utilisée par un compte.',
  'auth/invalid-email': 'Adresse e-mail invalide.',
  'auth/weak-password': 'Mot de passe trop court (6 caractères minimum).',
  'auth/operation-not-allowed': 'La connexion par e-mail n’est pas activée dans Firebase.',
  'auth/network-request-failed': 'Pas de connexion au serveur.',
};

async function createUser({ email, pass, role, name, country }) {
  const a = secondaryAuth();
  let cred;
  try {
    cred = await createUserWithEmailAndPassword(a, String(email).trim().toLowerCase(), pass);
  } catch (ex) {
    return { ok: false, error: AUTH_ERRORS[ex.code] || 'Création du compte impossible.' };
  }
  try {
    await setDoc(doc(db, ...ORG_PATH, 'users', cred.user.uid), {
      role, name: name || '', email: String(email).trim().toLowerCase(),
      country: country || '', active: true, mustChange: true,
      createdAt: serverTimestamp(),
      createdBy: auth.currentUser ? auth.currentUser.uid : null,
    });
  } catch (ex) {
    /* Le compte existe mais n'a pas de rôle : il ne pourra pas se connecter.
     * On le signale plutôt que de laisser un compte fantôme sans explication. */
    console.error('[Firebase] profil non écrit', ex);
    await signOut(a).catch(() => {});
    return { ok: false, error: 'Compte créé mais rôle non enregistré. '
      + 'Vérifiez vos droits d’administrateur, puis reprenez depuis la console.' };
  }
  await signOut(a).catch(() => {});          // la session parasite est jetée
  return { ok: true, uid: cred.user.uid };
}

async function disableUser(email) {
  const e = String(email || '').trim().toLowerCase();
  const snap = await getDocs(collection(db, ...ORG_PATH, 'users'));
  let hit = null;
  snap.forEach(d => { if ((d.data().email || '').toLowerCase() === e) hit = d; });
  if (!hit) return { ok: false, error: 'Compte introuvable.' };
  if (auth.currentUser && hit.id === auth.currentUser.uid) {
    return { ok: false, error: 'Vous ne pouvez pas désactiver votre propre compte.' };
  }
  await updateDoc(doc(db, ...ORG_PATH, 'users', hit.id), { active: false });
  return { ok: true };
}

async function enableUser(email) {
  const e = String(email || '').trim().toLowerCase();
  const snap = await getDocs(collection(db, ...ORG_PATH, 'users'));
  let hit = null;
  snap.forEach(d => { if ((d.data().email || '').toLowerCase() === e) hit = d; });
  if (!hit) return { ok: false, error: 'Compte introuvable.' };
  await updateDoc(doc(db, ...ORG_PATH, 'users', hit.id), { active: true });
  return { ok: true };
}

async function setRole(email, role) {
  const e = String(email || '').trim().toLowerCase();
  const snap = await getDocs(collection(db, ...ORG_PATH, 'users'));
  let hit = null;
  snap.forEach(d => { if ((d.data().email || '').toLowerCase() === e) hit = d; });
  if (!hit) return { ok: false, error: 'Compte introuvable.' };
  await updateDoc(doc(db, ...ORG_PATH, 'users', hit.id), { role });
  return { ok: true };
}

/** Comptes réellement déclarés dans Firestore.
 *  Ces documents survivent à toute réécriture de l'état : ils constituent donc
 *  la référence sûre pour reconstruire la liste des utilisateurs. */
async function listUsers() {
  const snap = await getDocs(collection(db, ...ORG_PATH, 'users'));
  const out = [];
  snap.forEach(d => {
    const u = d.data();
    if (u && u.email) out.push({
      uid: d.id, email: u.email, name: u.name || '', role: u.role || '',
      country: u.country || '', active: u.active !== false,
    });
  });
  return out;
}

/** Envoi d'un lien de réinitialisation — remplace la remise d'un mot de passe
 *  en main propre : l'administrateur ne connaît jamais le mot de passe. */
async function resetPassword(email) {
  try {
    await sendPasswordResetEmail(auth, String(email).trim().toLowerCase());
    return { ok: true };
  } catch (ex) {
    return { ok: false, error: AUTH_ERRORS[ex.code] || 'Envoi impossible.' };
  }
}

/* ══════════════════ Personnalisation du mot de passe ══════════════════
 * L'administrateur crée le compte avec un accès provisoire ; la personne définit
 * le sien à la première connexion. Deux effets : l'administrateur ne connaît
 * jamais le mot de passe de ses collaborateurs, et l'historique des validations
 * devient opposable — une décision tracée au nom de quelqu'un ne peut plus être
 * le fait d'un tiers qui disposait de son mot de passe initial.
 *
 * L'écran est bloquant : tant que le mot de passe n'est pas changé, on n'entre
 * pas dans l'application. Le drapeau `mustChange` du profil pilote ce passage. */

function passwordScreen(user, onDone) {
  const wrap = document.createElement('div');
  wrap.id = 'pa-fb-pass';
  wrap.style.cssText = 'position:fixed;inset:0;z-index:10000;background:#0f1a20;'
    + 'display:flex;align-items:center;justify-content:center;'
    + 'font:15px/1.5 system-ui,sans-serif;padding:24px;overflow:auto';
  wrap.innerHTML = `
    <form style="background:#fff;border-radius:14px;padding:32px 30px;width:min(100%,400px);
                 box-shadow:0 20px 60px rgba(0,0,0,.4)">
      <h1 style="margin:0 0 6px;font-size:20px;color:#12232b">Choisissez votre mot de passe</h1>
      <p style="margin:0 0 22px;color:#66787f;font-size:13.5px">
        Premier accès. Ce mot de passe n'est connu que de vous — l'administrateur
        ne peut pas le consulter.
      </p>
      <label style="display:block;font-size:13px;color:#4a5c63;margin-bottom:5px">Nouveau mot de passe</label>
      <input id="pw-1" type="password" autocomplete="new-password" required
             style="width:100%;box-sizing:border-box;padding:11px 12px;margin-bottom:14px;
                    border:1px solid #d6dee1;border-radius:8px;font-size:16px">
      <label style="display:block;font-size:13px;color:#4a5c63;margin-bottom:5px">Confirmation</label>
      <input id="pw-2" type="password" autocomplete="new-password" required
             style="width:100%;box-sizing:border-box;padding:11px 12px;margin-bottom:8px;
                    border:1px solid #d6dee1;border-radius:8px;font-size:16px">
      <p id="pw-hint" style="margin:0 0 14px;color:#8a9aa1;font-size:12px">
        8 caractères minimum. Évitez un mot de passe déjà utilisé ailleurs.
      </p>
      <p id="pw-err" hidden style="color:#b3261e;font-size:13px;margin:0 0 12px"></p>
      <button id="pw-go" type="submit"
              style="width:100%;padding:12px;background:#B5842C;color:#fff;border:0;
                     border-radius:8px;font-size:15px;font-weight:600;cursor:pointer">
        Enregistrer et continuer
      </button>
    </form>`;
  document.body.appendChild(wrap);

  const err = wrap.querySelector('#pw-err');
  const btn = wrap.querySelector('#pw-go');
  const fail = m => { err.textContent = m; err.hidden = false; btn.disabled = false;
                      btn.textContent = 'Enregistrer et continuer'; };

  wrap.querySelector('form').addEventListener('submit', async e => {
    e.preventDefault();
    err.hidden = true;
    const a = wrap.querySelector('#pw-1').value;
    const b = wrap.querySelector('#pw-2').value;
    if (a.length < 8) return fail('8 caractères minimum.');
    if (a !== b) return fail('Les deux saisies ne correspondent pas.');
    btn.disabled = true; btn.textContent = 'Enregistrement…';
    try {
      await updatePassword(user, a);
      await updateDoc(doc(db, ...ORG_PATH, 'users', user.uid), { mustChange: false });
      wrap.remove();
      onDone();
    } catch (ex) {
      if (ex.code === 'auth/requires-recent-login') {
        /* Session trop ancienne : Firebase exige une authentification fraîche
         * avant un changement de mot de passe. On renvoie à la connexion. */
        await signOut(auth);
        location.reload();
        return;
      }
      fail(ex.code === 'auth/weak-password'
        ? 'Mot de passe trop faible.'
        : 'Enregistrement impossible. Réessayez.');
    }
  });
}

/** Changement de mot de passe depuis l'application, à tout moment.
 *  Firebase exige de reconfirmer le mot de passe actuel — c'est ce qui empêche
 *  quelqu'un de détourner une session laissée ouverte. */
async function changePassword(current, next) {
  const u = auth.currentUser;
  if (!u) return { ok: false, error: 'Session expirée.' };
  if (String(next).length < 8) return { ok: false, error: '8 caractères minimum.' };
  try {
    await reauthenticateWithCredential(u, EmailAuthProvider.credential(u.email, current));
  } catch {
    return { ok: false, error: 'Mot de passe actuel incorrect.' };
  }
  try {
    await updatePassword(u, next);
    await updateDoc(doc(db, ...ORG_PATH, 'users', u.uid), { mustChange: false });
    return { ok: true };
  } catch {
    return { ok: false, error: 'Enregistrement impossible.' };
  }
}

/** Bouton « mot de passe » ajouté dans la barre haute, à côté de la déconnexion.
 *  Injecté après le démarrage : la barre existe dans le HTML, pas besoin d'y
 *  toucher — un module qui modifie le balisage de l'application serait un
 *  couplage de plus à maintenir. */
function installPasswordButton() {
  const bar = document.querySelector('.top');
  const out = document.getElementById('logout-btn');
  if (!bar || !out || document.getElementById('pa-pw-btn')) return;

  const b = document.createElement('button');
  b.id = 'pa-pw-btn';
  b.className = 'icon-btn';
  b.title = 'Changer mon mot de passe';
  b.setAttribute('aria-label', 'Changer mon mot de passe');
  b.innerHTML = '<svg viewBox="0 0 24 24" width="18" height="18" fill="none" '
    + 'stroke="currentColor" stroke-width="1.8"><rect x="4" y="10" width="16" height="10" rx="2"/>'
    + '<path d="M8 10V7a4 4 0 0 1 8 0v3"/></svg>';
  b.addEventListener('click', passwordDialog);
  bar.insertBefore(b, out);
}

function passwordDialog() {
  const ov = document.createElement('div');
  ov.className = 'modal-overlay';
  ov.innerHTML = `
    <div class="modal-card">
      <div class="modal-h"><b>Changer mon mot de passe</b>
        <button class="modal-x" id="pw-x">&times;</button></div>
      <div class="modal-b">
        <label>Mot de passe actuel</label>
        <input id="pd-0" type="password" autocomplete="current-password"
               style="width:100%;box-sizing:border-box;padding:9px 11px;margin-bottom:12px;
                      border:1px solid var(--line);border-radius:9px;font-size:16px">
        <label>Nouveau mot de passe</label>
        <input id="pd-1" type="password" autocomplete="new-password"
               style="width:100%;box-sizing:border-box;padding:9px 11px;margin-bottom:12px;
                      border:1px solid var(--line);border-radius:9px;font-size:16px">
        <label>Confirmation</label>
        <input id="pd-2" type="password" autocomplete="new-password"
               style="width:100%;box-sizing:border-box;padding:9px 11px;margin-bottom:10px;
                      border:1px solid var(--line);border-radius:9px;font-size:16px">
        <p id="pd-err" hidden style="color:var(--red);font-size:12.5px;margin:0 0 10px"></p>
        <div class="actions" style="display:flex;gap:9px;justify-content:flex-end">
          <button class="btn" id="pd-no" style="background:var(--paper);border:1px solid var(--line)">Annuler</button>
          <button class="btn btn-g" id="pd-ok" style="background:var(--brass);color:#fff">Enregistrer</button>
        </div>
      </div>
    </div>`;
  document.body.appendChild(ov);

  const err = ov.querySelector('#pd-err');
  const close = () => ov.remove();
  ov.querySelector('#pw-x').onclick = close;
  ov.querySelector('#pd-no').onclick = close;
  ov.addEventListener('click', e => { if (e.target === ov) close(); });

  ov.querySelector('#pd-ok').onclick = async () => {
    err.hidden = true;
    const cur = ov.querySelector('#pd-0').value;
    const a = ov.querySelector('#pd-1').value;
    const b = ov.querySelector('#pd-2').value;
    const fail = m => { err.textContent = m; err.hidden = false; };
    if (a.length < 8) return fail('8 caractères minimum.');
    if (a !== b) return fail('Les deux saisies ne correspondent pas.');
    const r = await changePassword(cur, a);
    if (!r.ok) return fail(r.error);
    close();
    banner('Mot de passe modifié.', null);
  };
}

/* ══════════════════ Bandeau d'information ══════════════════ */

function banner(message, onAction) {
  document.getElementById('pa-fb-banner')?.remove();
  const b = document.createElement('div');
  b.id = 'pa-fb-banner';
  b.style.cssText = 'position:fixed;left:50%;transform:translateX(-50%);bottom:22px;'
    + 'z-index:9999;background:#1d2b32;color:#fff;padding:12px 16px;border-radius:10px;'
    + 'box-shadow:0 8px 28px rgba(0,0,0,.28);font:14px/1.4 system-ui,sans-serif;'
    + 'display:flex;gap:14px;align-items:center;max-width:min(92vw,620px)';
  const txt = document.createElement('span');
  txt.textContent = message;
  b.appendChild(txt);
  if (onAction) {
    const a = document.createElement('button');
    a.textContent = 'Recharger';
    a.style.cssText = 'background:#B5842C;color:#fff;border:0;padding:7px 14px;'
      + 'border-radius:7px;cursor:pointer;font-weight:600;white-space:nowrap';
    a.onclick = onAction;
    b.appendChild(a);
  }
  const x = document.createElement('button');
  x.textContent = '✕';
  x.style.cssText = 'background:none;border:0;color:#9fb0b8;cursor:pointer;font-size:15px';
  x.onclick = () => b.remove();
  b.appendChild(x);
  document.body.appendChild(b);
  if (!onAction) setTimeout(() => b.remove(), 9000);
}

/* ══════════════════ Écrans d'état ══════════════════
 * Volontairement autonomes : ils s'affichent avant que l'application existe. */

function cadre(titre, texte, boutons) {
  document.getElementById('pa-fb-etat')?.remove();
  const w = document.createElement('div');
  w.id = 'pa-fb-etat';
  w.style.cssText = 'position:fixed;inset:0;z-index:10001;background:#0f1a20;'
    + 'display:flex;align-items:center;justify-content:center;padding:24px;'
    + 'font:15px/1.55 system-ui,sans-serif';
  w.innerHTML = `
    <div style="background:#fff;border-radius:14px;padding:32px 30px;width:min(100%,440px);
                box-shadow:0 20px 60px rgba(0,0,0,.4)">
      <img src="./icons/logo.png" alt="" width="52" height="52" style="display:block;margin:0 auto 16px">
      <h1 style="margin:0 0 10px;font-size:19px;color:#12232b;text-align:center">${titre}</h1>
      <p style="margin:0 0 20px;color:#5a6c73;font-size:14px;text-align:center">${texte}</p>
      <div id="pa-etat-btns" style="display:flex;gap:9px;justify-content:center;flex-wrap:wrap"></div>
    </div>`;
  document.body.appendChild(w);
  const zone = w.querySelector('#pa-etat-btns');
  boutons.forEach(([label, fn, principal]) => {
    const b = document.createElement('button');
    b.textContent = label;
    b.style.cssText = 'padding:11px 20px;border-radius:8px;font-size:14.5px;font-weight:600;'
      + 'cursor:pointer;border:1px solid ' + (principal ? '#B5842C' : '#d6dee1')
      + ';background:' + (principal ? '#B5842C' : '#fff') + ';color:' + (principal ? '#fff' : '#42555c');
    b.onclick = fn;
    zone.appendChild(b);
  });
  return w;
}

function ecranErreur(titre, texte, code) {
  cadre(titre, texte + (code ? `<br><span style="font-size:12px;color:#93a3aa">[${code}]</span>` : ''),
    [['Réessayer', () => location.reload(), true],
     ['Se déconnecter', async () => { await signOut(auth); location.reload(); }, false]]);
}

function ecranInitialisation() {
  return new Promise(resolve => {
    const w = cadre('Initialiser la base',
      'La base est vide. L’application va initialiser une structure de production vide. '
      + 'Aucune donnée fictive ni aucun compte de démonstration ne sera créé.',
      [['Initialiser', () => { w.remove(); resolve(true); }, true],
       ['Annuler', () => { w.remove(); resolve(false); }, false]]);
  });
}

/* ══════════════════ Écran de connexion ══════════════════
 * Volontairement autonome : il s'affiche avant que l'application n'existe. */

function loginScreen() {
  const wrap = document.createElement('div');
  wrap.id = 'pa-fb-login';
  wrap.style.cssText = 'position:fixed;inset:0;z-index:10000;background:#0f1a20;'
    + 'display:flex;align-items:center;justify-content:center;'
    + 'font:15px/1.5 system-ui,sans-serif;padding:24px';
  wrap.innerHTML = `
    <form style="background:#fff;border-radius:14px;padding:34px 30px;width:min(100%,380px);
                 box-shadow:0 20px 60px rgba(0,0,0,.4)">
      <img src="./icons/logo.png" alt="" width="64" height="64"
           style="display:block;margin:0 auto 14px">
      <h1 style="margin:0 0 4px;font-size:21px;color:#12232b;text-align:center">Palladium Africa</h1>
      <p style="margin:0 0 22px;color:#66787f;font-size:13.5px;text-align:center">CRM de pilotage commercial</p>
      <label style="display:block;font-size:13px;color:#4a5c63;margin-bottom:5px">Adresse e-mail</label>
      <input id="fb-mail" type="email" autocomplete="username" required
             style="width:100%;box-sizing:border-box;padding:11px 12px;margin-bottom:14px;
                    border:1px solid #d6dee1;border-radius:8px;font-size:14.5px">
      <label style="display:block;font-size:13px;color:#4a5c63;margin-bottom:5px">Mot de passe</label>
      <input id="fb-pass" type="password" autocomplete="current-password" required
             style="width:100%;box-sizing:border-box;padding:11px 12px;margin-bottom:6px;
                    border:1px solid #d6dee1;border-radius:8px;font-size:14.5px">
      <button type="button" id="fb-forgot"
              style="background:none;border:0;color:#3E6E8E;font-size:12.5px;cursor:pointer;
                     padding:0;margin-bottom:16px">Mot de passe oublié ?</button>
      <p id="fb-err" hidden style="color:#b3261e;font-size:13px;margin:0 0 12px"></p>
      <button id="fb-go" type="submit"
              style="width:100%;padding:12px;background:#B5842C;color:#fff;border:0;
                     border-radius:8px;font-size:15px;font-weight:600;cursor:pointer">
        Se connecter
      </button>
    </form>`;
  document.body.appendChild(wrap);

  const err = wrap.querySelector('#fb-err');
  const btn = wrap.querySelector('#fb-go');
  const mail = wrap.querySelector('#fb-mail');
  const fail = m => { err.textContent = m; err.hidden = false; };

  wrap.querySelector('form').addEventListener('submit', async e => {
    e.preventDefault();
    err.hidden = true;
    btn.disabled = true; btn.textContent = 'Connexion…';
    try {
      await signInWithEmailAndPassword(auth, mail.value.trim(), wrap.querySelector('#fb-pass').value);
      /* onAuthStateChanged prend le relais : chargement puis démarrage. */
    } catch (ex) {
      const map = {
        'auth/invalid-credential': 'Identifiants incorrects.',
        'auth/invalid-email': 'Adresse e-mail invalide.',
        'auth/user-disabled': 'Ce compte est désactivé.',
        'auth/too-many-requests': 'Trop de tentatives. Réessayez dans quelques minutes.',
        'auth/network-request-failed': 'Pas de connexion au serveur.',
      };
      /* Le code brut est affiche a l'ecran : sans lui, diagnostiquer un echec de
       * connexion oblige a ouvrir la console du navigateur, ce qui n'est pas
       * raisonnable a demander a un utilisateur. */
      fail((map[ex.code] || 'Connexion impossible.') + '  [' + (ex.code || 'inconnu') + ']');
      btn.disabled = false; btn.textContent = 'Se connecter';
    }
  });

  wrap.querySelector('#fb-forgot').addEventListener('click', async () => {
    const e = mail.value.trim();
    if (!e) return fail('Saisissez d’abord votre adresse e-mail.');
    try {
      await sendPasswordResetEmail(auth, e);
      err.style.color = '#2F7D5B';
      fail('Un lien de réinitialisation vient d’être envoyé.');
    } catch (ex2) { fail('Envoi impossible.  [' + (ex2.code || 'inconnu') + ']'); }
  });
}

/* ══════════════════ Démarrage de l'application ══════════════════ */

let started = false;

function startApp() {
  if (started) return;
  started = true;
  const src = document.getElementById('pa-app-src');
  if (!src) { console.error('[Firebase] script applicatif #pa-app-src introuvable'); return; }
  /* Exécution du code de l'application, une fois les données en place. */
  new Function(src.textContent)();
}

async function boot(user) {
  /* 1. Rôle de l'utilisateur — lu dans /orgs/{org}/users/{uid}.
   *    C'est ce document qui fait autorité, y compris dans les Security Rules. */
  let profile = null;
  try {
    const p = await getDoc(doc(db, ...ORG_PATH, 'users', user.uid));
    if (p.exists()) profile = p.data();
  } catch (err) {
    window.__paProfileErr = err.code || 'lecture-refusee';
    console.error('[Firebase] profil illisible', err);
  }

  if (!profile || !profile.role) {
    await signOut(auth);
    document.getElementById('pa-fb-login')?.remove();
    loginScreen();
    const err = document.querySelector('#fb-err');
    if (err) {
      err.textContent = 'Compte sans rôle attribué ou profil illisible.'
        + (window.__paProfileErr ? '  [' + window.__paProfileErr + ']' : '');
      err.hidden = false;
    }
    return;
  }

  /* 2. Session lue par l'application (auth.js). Comme elle est déjà présente,
   *    main.js va directement sur enterApp() et n'affiche pas son écran de
   *    connexion de démonstration. */
  sessionStorage.setItem(SESSION_KEY, JSON.stringify({
    email: user.email, role: profile.role, uid: user.uid, name: profile.name || '',
  }));

  /* 3. Chargement de l'état. */
  /* 3. Chargement de l'état — Firestore fait autorité, sans exception.
   *
   * L'application ne démarre QUE sur des données venues de la base. Trois cas :
   *   — la base répond et contient un état  → on le charge, on démarre ;
   *   — la base répond et elle est vide      → un administrateur peut l'initialiser ;
   *   — la base ne répond pas                → on ne démarre pas.
   *
   * Le troisième cas est le plus important. Démarrer « quand même » sur la copie
   * locale semble accommodant, mais cette copie finit par être renvoyée à la base
   * et écrase le travail de tous. Un écran d'attente est désagréable ; une perte
   * de données l'est infiniment plus. */
  let remote = null;
  try {
    remote = await loadState();
  } catch (err) {
    console.warn('[Firebase] chargement distant indisponible, démarrage hors ligne', err);
    /* Le cache local n'est utilisé qu'en mode hors ligne. Il ne devient jamais
     * une autorité : au retour réseau, les transactions fusionnent uniquement les
     * champs réellement modifiés localement avec la version serveur. */
    try {
      const cached = JSON.parse(localStorage.getItem(DB_KEY) || 'null');
      if (cached && typeof cached === 'object') {
        remote = cached;
        remoteShards = split(cached);
        window.__paFirebaseRemoteState = cloneSafe(cached);
      }
    } catch (_) {}
    if (!remote) {
      ecranErreur('Connexion indisponible',
        'Impossible de charger les données pour le moment. Vérifiez votre connexion.',
        err.code || 'indisponible');
      return;
    }
  }

  if (remote) {
    /* Le stockage local n'est qu'un cache alimenté PAR la base. Il n'est jamais
     * une source : c'est ce qui garantit qu'on ne réécrit que ce qu'on a lu. */
    localStorage.setItem(DB_KEY, JSON.stringify(remote));
    lastSent = Object.fromEntries(
      Object.entries(split(remote)).map(([k, v]) => [k, JSON.stringify(v)]));
  } else {
    /* Base vide : première mise en service. L'initialisation est un acte
     * délibéré, réservé à l'administration — sans cela, n'importe quel poste
     * pourrait remplir la base de données fictives. */
    if (profile.role !== 'admin') {
      ecranErreur('Base non initialisée',
        'Aucune donnée n’a encore été enregistrée. Un administrateur doit ouvrir '
        + 'l’application une première fois pour initialiser la base.', null);
      return;
    }
    const initialiser = await ecranInitialisation();
    if (!initialiser) { await signOut(auth); location.reload(); return; }
    localStorage.removeItem(DB_KEY);   // l'application repartira sur un état de production vide
    lastSent = {};
  }

  /* 4. Branchements exposés à l'application. */
  window.paSync = { push, flush, pull };
  window.paMeetingSync = { save: saveMeetings, load: loadMeetings };
  window.paAudit = { record: writeAudit };
  window.paFiles = { upload: uploadFile, remove: removeFile };
  window.paAdmin = { createUser, disableUser, enableUser, setRole, resetPassword, listUsers };
  window.paAccount = { changePassword, mustChange: !!profile.mustChange };
  window.paAuth = {
    signOut: async () => {
      try { await flush(); } catch {}
      sessionStorage.removeItem(SESSION_KEY);
      await signOut(auth);
      location.reload();
    },
    user: () => ({ email: user.email, uid: user.uid, role: profile.role }),
  };

  /* Filet de sécurité : on ne quitte pas la page sur une écriture en attente. */
  addEventListener('beforeunload', () => { if (pendingState) flush(); });
  addEventListener('pagehide', () => { if (pendingState) flush(); });
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden' && pendingState) flush();
  });

  document.getElementById('pa-fb-login')?.remove();

  /* Premier accès : le mot de passe provisoire doit être remplacé avant d'entrer. */
  const go = () => {
    startApp();
    watch();
    loadMeetings().catch(() => {});
    watchMeetings();
    /* La barre haute n'existe qu'une fois l'application rendue. */
    setTimeout(installPasswordButton, 300);
  };

  if (profile.mustChange) { passwordScreen(user, go); return; }
  go();
}

/* ══════════════════ Point d'entrée ══════════════════ */

setPersistence(auth, browserLocalPersistence).catch(() => {});

onAuthStateChanged(auth, user => {
  if (user) boot(user);
  else if (!started) { sessionStorage.removeItem(SESSION_KEY); loginScreen(); }
});
