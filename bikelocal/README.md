# Local Bike Data Warehouse

Un entrepôt de données moderne pour l'analyse des ventes et performances de Local Bike, construit avec dbt et ClickHouse.

## 🏗️ Architecture

Ce projet dbt suit une architecture en couches pour une transformation de données robuste et maintenable :

```
📁 bikelocal/
├── 📁 models/
│   ├── 📁 staging/           # Données brutes nettoyées
│   ├── 📁 intermediate/      # Métriques business agrégées
│   └── 📁 marts/            # Tables de reporting finales
├── 📁 seeds/                # Données de référence statiques
├── 📁 tests/                # Tests de qualité des données
└── 📁 macros/               # Fonctions SQL réutilisables
```

### Couches de Transformation

#### 1. Staging Layer (9 modèles)
Nettoyage et standardisation des données brutes :
- **Sources** : CSV clients, produits, commandes, stocks
- **Matérialisation** : `view`
- **Responsabilités** : Types de données, noms de colonnes, filtres de qualité

#### 2. Intermediate Layer (11 modèles)
Calculs de métriques business complexes :
- **Agrégations** : Revenus, quantités, moyennes par entité
- **Jointures** : Enrichissement avec données contextuelles
- **Matérialisation** : `incremental` (materialized MergeTree tables dans ClickHouse, modèles préfixés `int_` — ex : `int_sales__category_revenue`). Ces modèles nécessitent un **`unique_key`** explicite (souvent composite, ex. `category_id, store_id, year_month`) pour un incrémental fiable.

#### 3. Marts Layer (14 modèles)
Tables optimisées pour l'analyse et le reporting :
- **Dimensions** : Clients, produits, magasins, employés, temps
- **Faits** : Ventes, inventaire, performances opérationnelles
- **Matérialisation** : `table`

## 🚀 Démarrage Rapide

### Prérequis
- Python 3.8+
- dbt 1.11.2
- ClickHouse 25.10+

### Installation
```bash
# Cloner le repository
git clone <repository-url>
cd bikelocal

# Installer les dépendances
pip install -r requirements.txt

# Configurer l'environnement dbt
cp profiles.yml.example ~/.dbt/profiles.yml
# Éditer profiles.yml avec vos credentials ClickHouse
```

### Exécution
```powershell
# Activer l'environnement virtuel (Windows PowerShell)
. .\.venv_dbt\Scripts\Activate.ps1

# Exécuter tous les modèles + tests (recommandé)
dbt build

# Ou exécuter uniquement les modèles
dbt run

# Tester la qualité des données (si vous n'avez pas utilisé dbt build)
dbt test

# Générer la documentation
dbt docs generate

# Démarrer le serveur de docs (par défaut port 8080). Si 8080 est indisponible, utilisez 8081 :
dbt docs serve --port 8080
# ou
dbt docs serve --port 8081
```

## 📌 Découverte & Gouvernance

- **Tags** : les rapports sont tagués `rpt` (ex: `tags: ['rpt']`) — permet de filtrer rapidement avec `dbt ls --select tag:rpt`.
- **Exposures** : les rapports `rpt_*` sont exposés via `models/exposures.yml` et lient chaque rapport à un dashboard Power BI et à un owner pour traçabilité.
- **CI** : un workflow GitHub Actions (`.github/workflows/dbt-ci.yml`) exécute `dbt build` et `dbt docs generate` sur PR pour prévenir les régressions.
- **Recommendation incremental** : certains rapports volumineux incluent `meta.incremental_recommendation: true` dans la documentation pour indiquer qu'une matérialisation `incremental` peut être envisagée.

### 🚀 Opérations - Mode incremental

**Note** : Depuis la migration vers ClickHouse, de nombreux modèles intermédiaires (`int_*`) sont matérialisés en `incremental` (MergeTree) dans la base `localbike_raw_intermediate`. Les marts consomment ces `int_*` pour fiabilité et performance (extraction optimisée, évite les limites SQL de ClickHouse). Les `int_*` exigent un `unique_key` explicite — souvent composite (ex. `category_id, store_id, year_month`).

- **Append-only** : Les rapports temporels (`rpt_*`) sont configurés en mode `incremental` pour n'ajouter que des périodes nouvelles (par `year_month`). Les calculs historiques ne sont pas modifiés automatiquement — pour corriger ou backfiller des périodes antérieures, exécutez un `dbt run --select <model> --full-refresh` ciblé.
- **Snapshots & updates** : Pour des rapports de snapshot (ex. inventaire, LTV), l'incrémental insère de nouveaux éléments (nouvelles customers, nouvelles combinaisons store/product). Les mises à jour d'un enregistrement existant nécessitent un `--full-refresh` sur le modèle concerné ou l'utilisation d'une stratégie de merge/replace en production.
- **Bonnes pratiques** : Planifier des jobs de backfill (p.ex. quotidien pour la période courante ou hebdomadaire pour les 2 derniers mois) pour prendre en charge les arrivées tardives et garantir la complétude des KPIs.

## 📊 Modèles Disponibles

### Dimensions (Tables de référence)
- `dim_customers` : Profils clients avec segmentation RFM
- `dim_products` : Catalogue produits avec catégories et marques
- `dim_staff` : Équipe avec hiérarchie managériale
- `dim_stores` : Magasins avec informations géographiques
- `dim_time` : Dimensions temporelles pour analyses

### Faits (Métriques business)
- `fct_sales` : Transactions de vente détaillées
- `fct_inventory` : Niveaux de stock par produit/magasin
- `fct_operations_performance` : Métriques de fulfillment et qualité de service
- `fct_product_profitability` : Rentabilité par produit
- `fct_staff_performance` : Performance des ventes par employé

### Rapports (Agrégats pour BI)
- `rpt_sales_summary` : Vue d'ensemble des ventes
- `rpt_customer_ltv` : Valeur vie client
- `rpt_inventory_status` : État des stocks

### KPI & descriptions des tables (Dimensions / Faits / Rapports)
Cette section décrit rapidement les KPI principaux exposés par chaque table — utile pour les auteurs Power BI et la validation métier.

#### Dimensions
- `dim_customers` : KPIs clés — **lifetime_value**, `rfm_segment`, `total_orders`, `avg_order_value`, `days_since_last_order`.
- `dim_products` : KPIs clés — `list_price`, `price_tier`, `estimated_cost_price`, `estimated_margin`, `product_category_group`.
- `dim_staff` : KPIs clés — `total_orders_processed`, `total_items_sold`, `total_sales_revenue`, `performance_tier`.
- `dim_stores` : KPIs / attributs — `region`, `store_type`, `store_name`, `city`, `state` (utiles pour segmentation et géographie).
- `dim_time` : Clés temporelles — `date_key`, `year`, `month`, `year_month`, `quarter` (utilisées pour toutes les agrégations temporelles).

#### Faits (exemples de KPIs exposés)
- `fct_sales` : **net_revenue**, `gross_revenue`, `total_discounts`, `total_items_sold`, `unique_customers`, `avg_order_value`, `revenue_by_period`.
- `fct_inventory` : `total_stock_quantity`, `avg_stock_per_store`, `months_of_stock_coverage`, `stock_turnover_rate`, `stock_optimization_status`.
- `fct_operations_performance` : SLA/KPIs — `on_time_rate`, `days_to_ship`, `orders_processed`, `revenue_per_order`, `fulfillment_status`.
- `fct_product_profitability` : `estimated_cost_price`, `estimated_margin`, `profit_margin_percentage`, `estimated_profit`, `revenue_impact`.
- `fct_staff_performance` : `total_sales_revenue`, `avg_order_value`, `revenue_rank_in_store`, `performance_tier`.
- `fct_category_performance` : `total_revenue`, `products_in_category`, `contribution_pct`, `category_ranking`.

#### Rapports (agrégats / KPI prêts à l'emploi)
- `rpt_sales_summary` : KPIs — `total_revenue`, `total_orders`, `revenue_growth_pct`, `top_categories`, `revenue_by_store`.
- `rpt_customer_ltv` : KPIs — `customer_id`, `lifetime_value`, `avg_order_value`, `ltv_segment`, `churn_risk`.
- `rpt_inventory_status` : KPIs — `product_id`, `total_stock_quantity`, `stores_with_stock`, `low_stock_flag`, `recommended_reorder_qty`.
- `rpt_category_growth_analysis` : KPIs — `year_month`, **`revenue_12m_rolling_avg`**, `revenue_ytd`, `revenue_growth_pct`, `growth_contribution_12m_pct`, `price_tier`.

> 💡 Astuce : les `rpt_*` sont conçus pour être consommés directement par des outils BI (Power BI) — ils contiennent des KPIs prêts à l'emploi et des clés de jointure vers les dimensions.

## 🛠️ Technologies

- **dbt** 1.11.2 : Orchestration des transformations
- **ClickHouse** 25.10+ : Base de données analytique haute performance
- **SQL** : Langage de transformation avec extensions ClickHouse
- **YAML** : Configuration et métadonnées

### Particularités ClickHouse
- Matérialiser les intermédiaires (`int_*`) en `MergeTree` permet de contourner des limitations (correlated subqueries, nested aggregates) et d'améliorer les performances.
- Éviter les agrégations imbriquées : utilisez des CTE / sous-requêtes groupées ou materialize des étapes intermédiaires.
- Utiliser des alias explicites pour les colonnes (`AS ...`) — cela évite des erreurs d'identifiant et facilite la validation dbt.
- S'assurer que l'utilisateur ClickHouse a les privilèges nécessaires (CREATE, INSERT, SELECT) sur les bases : `localbike_raw`, `localbike_raw_staging`, `localbike_raw_intermediate`, `localbike_raw_marts`.

## 📈 Utilisation avec Power BI

Les tables marts sont optimisées pour la modélisation constellation Power BI :

1. **Connexion** : Utiliser le connecteur ClickHouse Power BI
2. **Dimensions** : Tables `dim_*` comme tables de recherche
3. **Faits** : Tables `fct_*` comme mesures et KPIs
4. **Rapports** : Tables `rpt_*` pour tableaux de bord pré-calculés

### Schéma Recommandé
```
dim_customers ──┐
dim_products  ──┼─ fct_sales ── rpt_sales_summary
dim_staff     ──┘
dim_stores      │
dim_time      ──┘
```

## 🔧 Développement

### Structure des Branches
- `main` : Code de production
- `develop` : Développement actif
- `feature/*` : Nouvelles fonctionnalités

### Tests
> Note : le test d'unicité pour `int_sales__category_revenue` a été mis à jour pour vérifier l'unicité sur **(category_id, store_id, year_month)** — il reflète désormais la granularité mensuelle par magasin.

```bash
# Tests unitaires
dbt test

# Tests personnalisés
dbt run-operation custom_tests
```

### Déploiement
```bash
# Validation avant déploiement
dbt run --dry-run

# Déploiement en production
dbt run --target prod
```

## 📚 Ressources

- [Documentation dbt](https://docs.getdbt.com/docs/introduction)
- [Guide ClickHouse](https://clickhouse.com/docs/)
- [Power BI + ClickHouse](https://clickhouse.com/docs/en/integrations/powerbi)

## 🤝 Contribution

1. Créer une branche `feature/nom-fonctionnalite`
2. Commiter avec messages descriptifs
3. Ouvrir une Pull Request vers `develop`
4. Validation par les tests automatiques

---

**Local Bike** - Données au service de la performance commerciale 🚴‍♂️
