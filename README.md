# Palladium Africa CRM

CRM collaboratif Palladium Africa, publié temporairement via GitHub Pages et utilisant Firebase Authentication, Firestore et Storage.

## Règles de production

- **Comptes réels uniquement** : l'authentification est exclusivement Firebase Authentication + profil `/orgs/palladium/users/{uid}`.
- **Aucune donnée métier fictive** n'est créée au démarrage.
- **Firebase est la source de vérité** ; `localStorage` est uniquement un cache et les brouillons de formulaire restent locaux au compte/appareil.
- **Synchronisation silencieuse** : pas de notification et pas de rechargement de page pour une mise à jour distante.
- Une saisie en cours est protégée contre le remplacement par une mise à jour distante.
- Les écritures concurrentes utilisent une fusion transactionnelle sur les tranches existantes.
- Les fichiers Storage nouvellement déposés sont liés à l'UID propriétaire.

## Publication actuelle

L'application peut être servie par GitHub Pages sous une URL du type :

`https://polecommercial203-alt.github.io/NEW-BUSSINESS-PALLADIUM/`

Le dépôt peut rester public temporairement, mais aucun secret, compte de service ou donnée client ne doit y être committé.

## Firebase

Fichiers importants :

- `firebase-config.js` — configuration Web publique Firebase.
- `firestore.rules` — autorité d'accès Firestore.
- `storage.rules` — autorité d'accès Storage.
- `firebase.json` — configuration Firebase pour un futur Hosting/déploiement des règles.

## Tests locaux

```bash
node tests/sync-merge.test.js
node tests/production-safety.test.js
```

## Avant production réelle

Tester avec au minimum deux comptes réels :

1. création simultanée de fiches ;
2. modification simultanée ;
3. saisie pendant une synchronisation distante ;
4. hors connexion puis reconnexion ;
5. plusieurs onglets ;
6. permissions de chaque rôle ;
7. lecture Storage selon le rôle ;
8. migration des anciennes données Firebase ;
9. quotas et erreurs `429 Too Many Requests`.

Voir `AUDIT_PRODUCTION.md` et `docs/PRODUCTION-CHECKLIST.md`.
