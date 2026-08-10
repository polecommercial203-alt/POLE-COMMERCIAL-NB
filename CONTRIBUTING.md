# Contribution

## Avant un commit

- Ne pas inclure de secrets.
- Tester l'application localement.
- Tester Firebase avec un environnement de développement.
- Vérifier les règles Firestore et Storage.
- Vérifier les scénarios de concurrence si une modification touche la synchronisation.

## Avant production

Une modification ne doit pas être considérée comme production-ready tant que les tests multi-utilisateurs, hors ligne/reconnexion et permissions ne sont pas passés.
