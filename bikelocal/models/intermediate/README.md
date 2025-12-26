# Intermediate Models - Modèles Intermédiaires

Ce dossier contient les modèles intermédiaires organisés par domaine fonctionnel. Ces modèles créent des vues métier enrichies à partir des données brutes du staging.

## Structure

```
intermediate/
├── sales/                    # Modèles liés aux ventes
├── inventory/               # Modèles liés à l'inventaire
└── operations/              # Modèles liés aux opérations
```

## Domaines Fonctionnels

### Sales (Ventes)
- **`int_sales__customer_orders`** : Résumé des commandes par client avec métriques d'achat
- **`int_sales__product_performance`** : Performance des ventes par produit avec analyses temporelles
- **`int_sales__store_performance`** : Analyse des ventes par magasin

### Inventory (Inventaire)
- **`int_inventory__product_stock`** : Niveaux de stock agrégés par produit
- **`int_inventory__low_stock_alerts`** : Alertes pour les produits en rupture ou stock faible

### Operations (Opérations)
- **`int_operations__staff_performance`** : Performance des employés avec métriques de vente
- **`int_operations__order_fulfillment`** : Métriques de traitement et livraison des commandes

## Caractéristiques

- **Matérialisation** : Tous les modèles sont matérialisés en `view`
- **Agrégations** : Calculs de métriques business (revenus, quantités, moyennes)
- **Jointures** : Enrichissement des données avec informations contextuelles
- **Métriques temporelles** : Analyses de tendances et périodes

## Utilisation

Ces modèles intermédiaires servent de base pour les modèles marts qui seront utilisés pour les rapports et analyses finales.