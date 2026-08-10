# Security

## À ne jamais committer

- Firebase Admin SDK service-account JSON
- clés privées
- certificats privés
- mots de passe
- tokens
- secrets backend
- `.env` de production

## Signalement

Ne pas publier de données clients, prospects, documents commerciaux ou captures contenant des informations personnelles dans les issues ou pull requests publiques.

## Règles Firebase

Les règles Firestore et Storage doivent être considérées comme la frontière de sécurité. Le masquage ou filtrage réalisé uniquement dans le frontend ne constitue pas un contrôle d'accès suffisant.
