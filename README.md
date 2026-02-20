# 🧬 Classification ML — SVM vs KNN
### Breast Cancer Wisconsin Dataset

> Projet de machine learning supervisé comparant deux algorithmes de classification binaire pour la détection de tumeurs malignes/bénignes.

**Auteur :** Bipanda Franck Ulrich  
**Environnement :** PyCharm | Python 3 | scikit-learn  
**Date :** Février 2026

---

## 📋 Description du Projet

Ce projet implémente et compare deux algorithmes de classification supervisée :

- **SVM** (Support Vector Machine) — `script1.py`
- **KNN** (K-Nearest Neighbors) — `script2.py`

L'objectif est de prédire si une tumeur est **maligne (0)** ou **bénigne (1)** à partir de 30 features cliniques mesurées par imagerie médicale.

---

## 📦 Installation

### Prérequis
- Python 3.8+
- PyCharm (recommandé) ou tout autre IDE Python

### Installer les dépendances

```bash
pip install scikit-learn numpy pandas matplotlib seaborn
```

### Cloner le projet

```bash
git clone https://github.com/bipanda-franck-ulrich/classification-ml.git
cd classification-ml
```

---

## 🗂️ Structure du Projet

```
classification-ml/
│
├── script1.py       # Modèle SVM
├── script2.py       # Modèle KNN
└── README.md        # Documentation
```

---

## 🧪 Dataset

| Caractéristique | Valeur |
|----------------|--------|
| Nom | Breast Cancer Wisconsin |
| Source | `sklearn.datasets.load_breast_cancer()` |
| Échantillons | 569 |
| Features | 30 |
| Classes | malignant (0) / benign (1) |
| Split Train/Test | 80% / 20% |

---

## 💻 Codes Sources

### Script 1 — SVM (`script1.py`)

```python
import pandas as pd
import numpy as np
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.datasets import load_breast_cancer

breast_cancer = load_breast_cancer()
X = breast_cancer.data
Y = breast_cancer.target

X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

model = SVC(kernel='rbf', C=1.0, gamma='scale')
model.fit(X_train, Y_train)
Y_pred = model.predict(X_test)

print("Accuracy :", accuracy_score(Y_test, Y_pred))
print(confusion_matrix(Y_test, Y_pred))
print(classification_report(Y_test, Y_pred))

# Test de plusieurs valeurs de C
for c in [0.1, 1, 10, 100]:
    model = SVC(C=c, kernel='rbf')
    model.fit(X_train, Y_train)
    y_pred = model.predict(X_test)
    print("C =", c, "-> Accuracy =", accuracy_score(Y_test, y_pred))
```

---

### Script 2 — KNN (`script2.py`)

```python
import pandas as pd
import numpy as np
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.datasets import load_breast_cancer

breast_cancer = load_breast_cancer()
X = breast_cancer.data
Y = breast_cancer.target

X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

model = KNeighborsClassifier()
model.fit(X_train, Y_train)
Y_pred = model.predict(X_test)

print("Accuracy :", accuracy_score(Y_test, Y_pred))
print(confusion_matrix(Y_test, Y_pred))
print(classification_report(Y_test, Y_pred))

# Test de plusieurs valeurs de K
for k in [1, 3, 5, 7, 9]:
    model = KNeighborsClassifier(n_neighbors=k)
    model.fit(X_train, Y_train)
    y_pred = model.predict(X_test)
    print("K =", k, "-> Accuracy =", accuracy_score(Y_test, y_pred))
```

---

## 📊 Résultats et Métriques

### Accuracy Globale

| Modèle | Meilleur paramètre | Accuracy |
|--------|-------------------|----------|
| **SVM** | C = 1 | **98.25%** ✅ |
| KNN | K = 9 | 96.49% |

---

### Matrice de Confusion

**SVM (C=1, avec StandardScaler) :**

|  | Prédit Malin (0) | Prédit Bénin (1) |
|--|-----------------|-----------------|
| **Réel Malin (0)** | 41 ✅ | 2 ❌ |
| **Réel Bénin (1)** | 0 ✅ | 71 ✅ |

**KNN (K=5, avec StandardScaler) :**

|  | Prédit Malin (0) | Prédit Bénin (1) |
|--|-----------------|-----------------|
| **Réel Malin (0)** | 40 ✅ | 3 ❌ |
| **Réel Bénin (1)** | 3 ❌ | 68 ✅ |

---

### Rapport de Classification — SVM

| Classe | Précision | Rappel | F1-Score |
|--------|-----------|--------|----------|
| malignant (0) | 1.00 | 0.95 | 0.98 |
| benign (1) | 0.97 | 1.00 | 0.99 |
| **accuracy** | | | **0.98** |

---

### Optimisation des Hyperparamètres

**SVM — Valeurs de C :**

| C | Accuracy |
|---|----------|
| 0.1 | 94.74% |
| **1** | **98.25% ★** |
| 10 | 97.37% |
| 100 | 93.86% |

**KNN — Valeurs de K :**

| K | Accuracy |
|---|----------|
| 1 | 93.86% |
| 3 | 94.74% |
| 5 | 94.74% |
| 7 | 94.74% |
| **9** | **96.49% ★** |

---

## 🏆 Conclusion

Le **SVM avec C=1 et StandardScaler** est l'algorithme recommandé pour cette tâche :

- ✅ Accuracy de **98.25%** vs 96.49% pour KNN
- ✅ **0 faux positif** (aucun patient sain diagnostiqué malin à tort)
- ✅ Seulement **2 faux négatifs** vs 3 pour KNN
- ✅ Meilleur rappel sur la classe maligne : **95%** vs 93%

> Dans un contexte médical, minimiser les faux négatifs est critique car cela représente des tumeurs malignes non détectées.

---

## 🔧 Technologies

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat&logo=python&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-latest-F7931E?style=flat&logo=scikit-learn&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-latest-150458?style=flat&logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/NumPy-latest-013243?style=flat&logo=numpy&logoColor=white)
