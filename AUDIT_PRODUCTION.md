# Palladium — Audit / corrections de production

Cette version est orientée production : aucun compte de démonstration ni jeu de données métier fictif n'est créé par le frontend. Firebase Auth/Firestore sont la source des utilisateurs.

## Corrections appliquées
- état initial métier vide ;
- suppression des fallbacks de données commerciales fictives ;
- comptes utilisateurs exclusivement Firebase Auth + Firestore ;
- nettoyage des anciennes données de démo connues lors de la synchronisation administrateur ;
- brouillons de qualification réellement sauvegardés localement par utilisateur ;
- restauration des brouillons ;
- double soumission du formulaire bloquée ;
- protection de la saisie pendant une synchronisation distante ;
- Storage : métadonnées ownerUid et lecture restreinte pour les rôles non direction ;
- Service Worker versionné pour la release production ;
- aucune notification/reload pour la synchronisation normale.

## À tester avant production
1. Deux utilisateurs créent des fiches simultanément.
2. Un utilisateur saisit une fiche pendant qu'un autre modifie le CRM.
3. Coupure réseau pendant une qualification puis reconnexion.
4. Plusieurs onglets.
5. Chaque rôle et ses permissions.
6. Lecture des documents Storage avec comptes de rôles différents.
7. Vérification des règles Firebase déployées.
