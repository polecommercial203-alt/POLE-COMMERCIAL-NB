# Comptes réels uniquement

Cette version ne contient plus de comptes utilisateurs de démonstration dans `seedState()`.

## Source d'autorité

Les comptes sont exclusivement issus de :

- Firebase Authentication
- `/orgs/palladium/users/{uid}` dans Firestore

## Migration

Lors de l'entrée de l'administrateur, la liste réelle est synchronisée automatiquement. Les anciennes entrées sans adresse e-mail sont considérées comme des comptes de démonstration et retirées du cache CRM, ainsi que les données métier qui leur étaient rattachées.

Les comptes Firebase réels ne sont pas supprimés.

## Important

Avant production, effectuer un backup Firestore et vérifier la liste des comptes réels. Le nettoyage automatique ne supprime que les utilisateurs sans e-mail dans l'ancien état CRM et les données explicitement rattachées à ces anciens profils.
