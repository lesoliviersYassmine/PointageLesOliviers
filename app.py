import os
import hashlib
from datetime import datetime, date, time as tm, timedelta
import base64
import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import time
import io


# =========================
# Configuration de la page
# =========================
st.set_page_config(
    page_title="Système de Pointage du Personnel",
    page_icon="⏰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================
# Paramètres / Sécurité
# =========================
# Configuration SQLite
DB_PATH = "pointage_db.sqlite"
DEFAULT_ADMIN_USER = "admin"
DEFAULT_ADMIN_PASS = "admin123"

# =========================
# Connexion SQLite
# =========================

def get_connection():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        st.error(f"Erreur de connexion à SQLite: {e}")
        return None

def test_connection_background():
    try:
        conn = get_connection()
        if conn:
            conn.close()
            return True
        return False
    except Exception:
        return False

# =========================
# Authentification & Utilisateurs
# =========================

def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()

def create_users_table():
    conn = get_connection()
    if conn is None:
        return False

    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(20) DEFAULT 'user',
                    email VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Créer un admin par défaut si absent
            cur.execute("SELECT COUNT(*) FROM users WHERE username = ?", (DEFAULT_ADMIN_USER,))
            exists = cur.fetchone()[0]
            if exists == 0:
                cur.execute(
                    "INSERT INTO users (username, password_hash, role, email) VALUES (?, ?, ?, ?)",
                    (
                        DEFAULT_ADMIN_USER,
                        sha256(DEFAULT_ADMIN_PASS),
                        "admin",
                        f"{DEFAULT_ADMIN_USER}@example.com",
                    ),
                )
        return True
    except Exception as e:
        st.error(f"Erreur création table users: {e}")
        return False
    finally:
        if conn:
            conn.close()

def authenticate_user(username, password):
    conn = get_connection()
    if conn is None:
        return False

    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, username, role FROM users WHERE username = ? AND password_hash = ?",
                (username, sha256(password)),
            )
            user = cur.fetchone()
            return dict(user) if user else False
    except Exception as e:
        st.error(f"Erreur authentification: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_all_users():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            "SELECT id, username, role, email, created_at FROM users ORDER BY username",
            conn,
        )
    except Exception as e:
        st.error(f"Erreur récupération utilisateurs: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def create_user(username, password, role, email):
    conn = get_connection()
    if conn is None:
        return False
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO users (username, password_hash, role, email) VALUES (?, ?, ?, ?)",
                (username, sha256(password), role, email),
            )
        return True
    except Exception as e:
        st.error(f"Erreur création utilisateur: {e}")
        return False
    finally:
        if conn:
            conn.close()

# =========================
# Mise à jour du schéma de la base de données
# =========================

def update_database_schema():
    """Met à jour le schéma de la base de données avec les nouvelles colonnes et tables"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            
            # Vérifier si la colonne service existe dans tours_role_nuit
            cur.execute("PRAGMA table_info(tours_role_nuit)")
            columns = [col[1] for col in cur.fetchall()]
            if 'service' not in columns:
                cur.execute("ALTER TABLE tours_role_nuit ADD COLUMN service VARCHAR(100) NOT NULL DEFAULT 'General'")
                st.info("✅ Colonne service ajoutée à la table tours_role_nuit")
                
                # Supprimer et recréer la contrainte UNIQUE pour SQLite
                cur.execute("DROP INDEX IF EXISTS tours_role_nuit_date_tour_service_key")
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS tours_role_nuit_date_tour_service_key ON tours_role_nuit(date_tour, service)")
            
            # Vérifier si la colonne jours_travail existe dans personnels
            cur.execute("PRAGMA table_info(personnels)")
            columns_personnel = [col[1] for col in cur.fetchall()]
            if 'jours_travail' not in columns_personnel:
                cur.execute("ALTER TABLE personnels ADD COLUMN jours_travail VARCHAR(100) DEFAULT ''")
                st.info("✅ Colonne jours_travail ajoutée à la table personnels")
            
            # Vérifier si le poste 'Mixte' est dans les contraintes CHECK
            # Pour SQLite, il faut recréer la table pour modifier la contrainte CHECK
            cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='personnels'")
            table_sql = cur.fetchone()[0]
            if "CHECK (poste IN ('Jour', 'Nuit'))" in table_sql:
                # Sauvegarder les données
                cur.execute("""
                    CREATE TEMPORARY TABLE temp_personnels AS 
                    SELECT * FROM personnels
                """)
                
                # Supprimer l'ancienne table
                cur.execute("DROP TABLE personnels")
                
                # Recréer la table avec la nouvelle contrainte
                cur.execute("""
                    CREATE TABLE personnels (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        nom VARCHAR(100) NOT NULL,
                        prenom VARCHAR(100) NOT NULL,
                        service VARCHAR(100) NOT NULL,
                        poste VARCHAR(50) NOT NULL CHECK (poste IN ('Jour', 'Nuit', 'Mixte')),
                        heure_entree_prevue TIME NOT NULL,
                        heure_sortie_prevue TIME NOT NULL,
                        groupe_nuit VARCHAR(1) DEFAULT 'A' CHECK (groupe_nuit IN ('A', 'B')),
                        jours_travail VARCHAR(100) DEFAULT '',
                        actif BOOLEAN DEFAULT TRUE,
                        date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Restaurer les données
                cur.execute("""
                    INSERT INTO personnels 
                    (id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, groupe_nuit, actif, date_creation)
                    SELECT id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, groupe_nuit, actif, date_creation
                    FROM temp_personnels
                """)
                
                # Supprimer la table temporaire
                cur.execute("DROP TABLE temp_personnels")
                st.info("✅ Contrainte CHECK mise à jour pour inclure 'Mixte'")
                
        return True
    except Exception as e:
        st.error(f"Erreur mise à jour du schéma: {e}")
        return False
    finally:
        if conn:
            conn.close()

# =========================
# Modèle de données
# =========================

def create_tables():
    conn = get_connection()
    if conn is None:
        return False

    try:
        with conn:
            cur = conn.cursor()
            # Table personnels - MODIFIÉE pour supporter mixte
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS personnels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nom VARCHAR(100) NOT NULL,
                    prenom VARCHAR(100) NOT NULL,
                    service VARCHAR(100) NOT NULL,
                    poste VARCHAR(50) NOT NULL CHECK (poste IN ('Jour', 'Nuit', 'Mixte')),
                    heure_entree_prevue TIME NOT NULL,
                    heure_sortie_prevue TIME NOT NULL,
                    groupe_nuit VARCHAR(1) DEFAULT 'A' CHECK (groupe_nuit IN ('A', 'B')),
                    jours_travail VARCHAR(100) DEFAULT '',
                    actif BOOLEAN DEFAULT TRUE,
                    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Table tours de rôle pour le personnel de nuit
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tours_role_nuit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date_tour DATE NOT NULL,
                    service VARCHAR(100) NOT NULL,
                    groupe_actif VARCHAR(20) NOT NULL CHECK (groupe_actif IN ('A', 'B')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date_tour, service)
                )
                """
            )

            # Table groupes par service
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS groupes_nuit_par_service (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    service VARCHAR(100) NOT NULL UNIQUE,
                    groupe_actif VARCHAR(20) NOT NULL DEFAULT 'A' CHECK (groupe_actif IN ('A', 'B')),
                    derniere_maj TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Table congés
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS conges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    personnel_id INTEGER REFERENCES personnels(id) ON DELETE CASCADE,
                    date_debut DATE NOT NULL,
                    date_fin DATE NOT NULL,
                    type_conge VARCHAR(50) NOT NULL,
                    motif TEXT,
                    statut VARCHAR(20) DEFAULT 'En attente' CHECK (statut IN ('En attente', 'Approuvé', 'Rejeté')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Table: Quotas de congés par employé
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS quotas_conges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    personnel_id INTEGER UNIQUE REFERENCES personnels(id) ON DELETE CASCADE,
                    jours_alloues INTEGER DEFAULT 21,
                    jours_pris INTEGER DEFAULT 0,
                    jours_restants INTEGER DEFAULT 21,
                    annee INTEGER DEFAULT (strftime('%Y', CURRENT_DATE)),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Table pointages
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pointages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    personnel_id INTEGER REFERENCES personnels(id) ON DELETE CASCADE,
                    date_pointage DATE NOT NULL,
                    heure_arrivee TIME,
                    heure_depart TIME,
                    statut_arrivee VARCHAR(50) DEFAULT 'Present',
                    statut_depart VARCHAR(50) DEFAULT 'Present',
                    retard_minutes INTEGER DEFAULT 0,
                    depart_avance_minutes INTEGER DEFAULT 0,
                    motif_retard TEXT,
                    motif_depart_avance TEXT,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(personnel_id, date_pointage)
                )
                """
            )

            # Table retards
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS retards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    personnel_id INTEGER REFERENCES personnels(id) ON DELETE CASCADE,
                    date_retard DATE NOT NULL,
                    retard_minutes INTEGER NOT NULL,
                    motif TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Table absences
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS absences (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    personnel_id INTEGER REFERENCES personnels(id) ON DELETE CASCADE,
                    date_absence DATE NOT NULL,
                    motif TEXT,
                    justifie BOOLEAN DEFAULT FALSE,
                    certificat_justificatif BLOB,
                    type_certificat VARCHAR(10),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(personnel_id, date_absence)
                )
                """
            )

            # Données d'exemple s'il n'y a personne
            cur.execute("SELECT COUNT(*) FROM personnels")
            if cur.fetchone()[0] == 0:
                cur.execute(
                    """
                    INSERT INTO personnels (nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, groupe_nuit) VALUES
                    ('Dupont', 'Jean', 'Reception', 'Jour', '08:00:00', '16:00:00', 'A'),
                    ('Martin', 'Marie', 'Radiologie', 'Nuit', '20:00:00', '04:00:00', 'A'),
                    ('Bernard', 'Pierre', 'Urgence', 'Jour', '07:30:00', '15:30:00', 'A'),
                    ('Dubois', 'Sophie', 'Maternité', 'Nuit', '21:00:00', '05:00:00', 'B'),
                    ('Moreau', 'Luc', 'Administration', 'Jour', '09:00:00', '17:00:00', 'A'),
                    ('Leroy', 'Julie', 'Chirurgie', 'Mixte', '08:00:00', '16:00:00', 'A')
                    """
                )
                
                # Initialiser les quotas de congés pour les employés exemple
                cur.execute("SELECT id FROM personnels")
                employes = cur.fetchall()
                for emp_id in employes:
                    cur.execute(
                        "INSERT INTO quotas_conges (personnel_id) VALUES (?)",
                        (emp_id[0],)
                    )
        
        # Crée la table users et l'admin par défaut
        ok = create_users_table()
        
        # Met à jour le schéma avec les nouvelles colonnes
        update_database_schema()
        
        return True
    except Exception as e:
        st.error(f"Erreur création tables: {e}")
        return False
    finally:
        if conn:
            conn.close()
# =========================
# Fonctions utilitaires
# =========================
def _time_to_str(time_obj):
    """Convertit un objet time en string pour SQLite de manière robuste"""
    if isinstance(time_obj, tm):
        return time_obj.strftime("%H:%M:%S")
    elif isinstance(time_obj, str):
        # Si c'est déjà une string, vérifier le format
        if len(time_obj.split(':')) >= 2:
            return time_obj
        else:
            return f"{time_obj}:00:00"
    else:
        return str(time_obj)
    
def _as_time(value) -> tm:
    if isinstance(value, tm):
        return value
    elif isinstance(value, str):
        # Gérer les strings de temps
        for fmt in ("%H:%M:%S", "%H:%M:%S.%f", "%H:%M"):
            try:
                return datetime.strptime(value, fmt).time()
            except ValueError:
                continue
    # Si tout échoue, retourner une heure par défaut
    return tm(8, 0)

def update_sqlite_date_handling():
    """Corrige le problème de gestion des dates dans SQLite pour Python 3.12+"""
    import sqlite3
    import datetime
    
    def adapt_date_iso(val):
        """Convertit date en string ISO"""
        return val.isoformat()
    
    def convert_date(val):
        """Convertit string ISO en date"""
        return datetime.date.fromisoformat(val.decode())
    
    # Enregistrer les adaptateurs
    sqlite3.register_adapter(datetime.date, adapt_date_iso)
    sqlite3.register_converter("date", convert_date)

def get_nom_employe(personnel_id):
    """Récupère le nom complet d'un employé"""
    conn = get_connection()
    if conn is None:
        return "Employé"
    
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT nom, prenom FROM personnels WHERE id = ?",
                (personnel_id,)
            )
            result = cur.fetchone()
            if result:
                return f"{result['prenom']} {result['nom']}"
            return "Employé"
    except Exception as e:
        st.error(f"Erreur récupération nom employé: {e}")
        return "Employé"
    finally:
        if conn:
            conn.close()

def get_services_disponibles():
    conn = get_connection()
    if conn is None:
        return []
    try:
        df = pd.read_sql_query("SELECT DISTINCT service FROM personnels WHERE actif = 1 ORDER BY service", conn)
        return df['service'].tolist()
    except Exception as e:
        st.error(f"Erreur récupération services: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_services_nuit():
    """Récupère les services ayant du personnel de nuit"""
    conn = get_connection()
    if conn is None:
        return []
    try:
        df = pd.read_sql_query(
            "SELECT DISTINCT service FROM personnels WHERE actif = 1 AND poste = 'Nuit' ORDER BY service", 
            conn
        )
        return df['service'].tolist()
    except Exception as e:
        st.error(f"Erreur récupération services nuit: {e}")
        return []
    finally:
        if conn:
            conn.close()

def filtrer_personnel(recherche, filtre_service, groupe_nuit_actif=None, inclure_tous=False, date_pointage=None):
    """Filtre le personnel en excluant les employés en congé et les nuitiers qui pointent de jour"""
    if date_pointage is None:
        date_pointage = date.today()
    
    conn = get_connection()
    if conn is None:
        return {}
    
    try:
        # Construire la requête de base
        if inclure_tous:
            query = """
                SELECT id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, 
                       groupe_nuit, jours_travail, actif 
                FROM personnels 
                WHERE actif = 1 
            """
        else:
            query = """
                SELECT id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, 
                       groupe_nuit, jours_travail, actif 
                FROM personnels 
                WHERE actif = 1 
            """
            
            # Conditions pour exclure les groupes de nuit non actifs
            services_nuit = get_services_nuit()
            nuit_conditions = []
            
            for service in services_nuit:
                groupe_actif_service = get_groupe_nuit_actif_service(service)
                # Inclure seulement le personnel de nuit du groupe actif
                nuit_conditions.append(f"(service = '{service}' AND poste = 'Nuit' AND groupe_nuit = '{groupe_actif_service}')")
            
            if nuit_conditions:
                query += " AND (poste = 'Jour' OR poste = 'Mixte' OR " + " OR ".join(nuit_conditions) + ")"
            else:
                query += " AND (poste = 'Jour' OR poste = 'Mixte')"
        
        # Exclure les employés en congé
        query += """
            AND id NOT IN (
                SELECT personnel_id FROM conges 
                WHERE statut = 'Approuvé' 
                AND date_debut <= ? 
                AND date_fin >= ?
            )
            -- Inclure les employés de nuit même s'ils pointent de jour
            AND (
                poste != 'Nuit' 
                OR id NOT IN (
                    SELECT personnel_id FROM pointages 
                    WHERE date_pointage = ? 
                    AND heure_arrivee IS NOT NULL
                    AND strftime('%H:%M', heure_arrivee) BETWEEN '06:00' AND '18:00'
                )
                OR id IN (
                    SELECT personnel_id FROM pointages 
                    WHERE date_pointage = ? 
                    AND heure_arrivee IS NOT NULL
                    AND strftime('%H:%M', heure_arrivee) NOT BETWEEN '06:00' AND '18:00'
                )
            )
        """
        
        query += " ORDER BY service, nom, prenom"
        
        df = pd.read_sql_query(query, conn, params=(date_pointage, date_pointage, date_pointage, date_pointage))
        
        # Filtrer par recherche et service
        personnel_par_service = {}
        for _, row in df.iterrows():
            # Vérifier que la ligne contient les colonnes nécessaires
            if 'service' not in row or 'prenom' not in row or 'nom' not in row:
                continue
                
            service = row['service']
            
            # Appliquer le filtre de service
            if filtre_service != "Tous les services" and service != filtre_service:
                continue
                
            # Appliquer le filtre de recherche
            nom_complet = f"{row['prenom']} {row['nom']}".lower()
            poste = row.get('poste', '')
            
            if recherche and recherche.lower() not in nom_complet and recherche.lower() not in service.lower() and recherche.lower() not in poste.lower():
                continue
            
            if service not in personnel_par_service:
                personnel_par_service[service] = []
            personnel_par_service[service].append(row.to_dict())
            
        return personnel_par_service
    except Exception as e:
        st.error(f"Erreur récupération personnel par service: {e}")
        return {}
    finally:
        if conn:
            conn.close()
def get_pointage_employe_jour(personnel_id, date_pointage):
    conn = get_connection()
    if conn is None:
        return {}
    try:
        df = pd.read_sql_query(
            """
            SELECT * FROM pointages 
            WHERE personnel_id = ? AND date_pointage = ?
            """,
            conn,
            params=(personnel_id, date_pointage)
        )
        if not df.empty:
            return df.iloc[0].to_dict()
        return {}
    except Exception as e:
        st.error(f"Erreur récupération pointage: {e}")
        return {}
    finally:
        if conn:
            conn.close()
            
def test_pointage_direct():
    """Test direct de l'enregistrement en base de données"""
    conn = get_connection()
    if conn:
        try:
            # Test avec le premier employé
            cur = conn.cursor()
            cur.execute("SELECT id FROM personnels LIMIT 1")
            emp = cur.fetchone()
            
            if emp:
                emp_id = emp['id']
                heure_test = datetime.now().time().strftime('%H:%M:%S')
                
                # Insérer directement
                cur.execute(
                    """
                    INSERT OR REPLACE INTO pointages 
                    (personnel_id, date_pointage, heure_arrivee, statut_arrivee)
                    VALUES (?, ?, ?, ?)
                    """,
                    (emp_id, date.today(), heure_test, "Test manuel")
                )
                conn.commit()
                print(f"DEBUG: Test réussi - Employé {emp_id} pointé à {heure_test}")
                
                # Vérifier l'insertion
                cur.execute(
                    "SELECT * FROM pointages WHERE personnel_id = ? AND date_pointage = ?",
                    (emp_id, date.today())
                )
                result = cur.fetchone()
                if result:
                    print(f"DEBUG: Pointage vérifié - {dict(result)}")
                
        except Exception as e:
            print(f"DEBUG: Erreur test: {e}")
        finally:
            conn.close()

# Appelez cette fonction quelque part pour tester
    test_pointage_direct()

# =========================
# Requêtes métier
# =========================

def get_personnel():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            "SELECT id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, groupe_nuit, actif FROM personnels ORDER BY nom, prenom",
            conn,
        )
    except Exception as e:
        st.error(f"Erreur récupération personnel: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()
            
def get_personnel_non_pointe():
    """Récupère le personnel qui n'a pas pointé aujourd'hui, en excluant les congés, groupes non actifs et nuitiers qui pointent de jour"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        # Récupérer les services avec du personnel de nuit et leurs groupes actifs
        services_nuit = get_services_nuit()
        conditions = []
        
        for service in services_nuit:
            groupe_actif = get_groupe_nuit_actif_service(service)
            # Exclure le personnel de nuit du groupe non actif
            conditions.append(f"(p.service = '{service}' AND p.poste = 'Nuit' AND p.groupe_nuit = '{groupe_actif}')")
        
        # Construction de la requête
        query = """
            SELECT p.id, p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue
            FROM personnels p
            WHERE p.actif = 1 
            AND (
                p.poste = 'Jour' 
                OR p.poste = 'Mixte'
        """
        
        # Ajouter les conditions pour le personnel de nuit du groupe actif
        if conditions:
            query += " OR " + " OR ".join(conditions)
        
        query += """
            )
            AND p.id NOT IN (
                SELECT personnel_id FROM pointages 
                WHERE date_pointage = ? AND heure_arrivee IS NOT NULL
            )
            AND p.id NOT IN (
                SELECT personnel_id FROM conges 
                WHERE statut = 'Approuvé' 
                AND date_debut <= ? 
                AND date_fin >= ?
            )
            -- Exclure les employés de nuit qui ont pointé de jour
            AND NOT (
                p.poste = 'Nuit' 
                AND p.id IN (
                    SELECT personnel_id FROM pointages 
                    WHERE date_pointage = ? 
                    AND heure_arrivee IS NOT NULL
                    AND strftime('%H:%M', heure_arrivee) BETWEEN '06:00' AND '18:00'
                )
            )
            ORDER BY p.service, p.nom, p.prenom
        """
        
        return pd.read_sql_query(
            query,
            conn,
            params=(date.today(), date.today(), date.today(), date.today()),
        )
    except Exception as e:
        st.error(f"Erreur récupération personnel non pointé: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()
            
def get_nuitiers_pointant_de_jour():
    """Récupère les employés de nuit qui ont pointé pendant la journée"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        return pd.read_sql_query(
            """
            SELECT p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue,
                   pt.heure_arrivee, pt.heure_depart, pt.date_pointage,
                   CASE 
                       WHEN strftime('%H:%M', pt.heure_arrivee) BETWEEN '06:00' AND '18:00' THEN 'Journée'
                       ELSE 'Nuit'
                   END as periode_pointage
            FROM pointages pt
            JOIN personnels p ON pt.personnel_id = p.id
            WHERE p.poste = 'Nuit'
            AND pt.date_pointage = ?
            AND strftime('%H:%M', pt.heure_arrivee) BETWEEN '06:00' AND '18:00'
            ORDER BY p.service, p.nom, p.prenom
            """,
            conn,
            params=(date.today(),)
        )
    except Exception as e:
        st.error(f"Erreur récupération nuitiers pointant de jour: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def ajouter_personnel(nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, groupe_nuit="A", jours_travail=""):
    conn = get_connection()
    if conn is None:
        return False
    try:
        heure_entree_str = heure_entree_prevue.strftime('%H:%M:%S') if isinstance(heure_entree_prevue, tm) else str(heure_entree_prevue)
        heure_sortie_str = heure_sortie_prevue.strftime('%H:%M:%S') if isinstance(heure_sortie_prevue, tm) else str(heure_sortie_prevue)
        
        with conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO personnels (nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, groupe_nuit, jours_travail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (nom, prenom, service, poste, heure_entree_str, heure_sortie_str, groupe_nuit, jours_travail),
            )
            
            personnel_id = cur.lastrowid
            
            cur.execute(
                """
                INSERT INTO quotas_conges (personnel_id, jours_alloues, jours_restants)
                VALUES (?, ?, ?)
                """,
                (personnel_id, 25, 25)
            )
            
        return True
    except Exception as e:
        st.error(f"Erreur ajout personnel: {e}")
        return False
    finally:
        if conn:
            conn.close()

def modifier_personnel(personnel_id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, groupe_nuit, actif, jours_travail=""):
    conn = get_connection()
    if conn is None:
        return False
    try:
        personnel_id = int(personnel_id)
        
        heure_entree_str = heure_entree_prevue.strftime('%H:%M:%S') if isinstance(heure_entree_prevue, tm) else str(heure_entree_prevue)
        heure_sortie_str = heure_sortie_prevue.strftime('%H:%M:%S') if isinstance(heure_sortie_prevue, tm) else str(heure_sortie_prevue)
        
        with conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE personnels 
                SET nom = ?, prenom = ?, service = ?, poste = ?, 
                    heure_entree_prevue = ?, heure_sortie_prevue = ?, 
                    groupe_nuit = ?, actif = ?, jours_travail = ?
                WHERE id = ?
                """,
                (nom, prenom, service, poste, heure_entree_str, heure_sortie_str, groupe_nuit, actif, jours_travail, personnel_id),
            )
            
        return True
    except Exception as e:
        st.error(f"Erreur modification personnel: {e}")
        return False
    finally:
        if conn:
            conn.close()
def supprimer_personnel(personnel_id):
    """Désactive un employé (ne le supprime pas définitivement)"""
    conn = get_connection()
    if conn is None:
        return False
    try:
        personnel_id = int(personnel_id)
        
        with conn:
            cur = conn.cursor()
            # Désactiver l'employé au lieu de le supprimer pour garder l'historique
            cur.execute(
                "UPDATE personnels SET actif = 0 WHERE id = ?",
                (personnel_id,),
            )
            print(f"DEBUG: Employé {personnel_id} désactivé")
        return True
    except Exception as e:
        st.error(f"Erreur désactivation personnel: {e}")
        return False
    finally:
        if conn:
            conn.close()

def calculer_statut_arrivee(heure_pointage, heure_prevue):
    """
    Calcule le statut de pointage selon les règles spécifiques:
    - Plage normale: 15min avant à 5min avant l'heure prévue (07:45 à 07:55 pour 08:00)
    - En retard: après 5min avant l'heure prévue jusqu'à 29 minutes de retard
    - Absent: 30 minutes ou plus de retard (après 08:30 pour 08:00)
    """
    if not heure_pointage or not heure_prevue:
        return "Non pointé", 0, False
    
    heure_prevue = _as_time(heure_prevue)
    heure_pointage = _as_time(heure_pointage)
    
    # Convertir en datetime pour les calculs
    dt_prevue = datetime.combine(date.today(), heure_prevue)
    dt_pointage = datetime.combine(date.today(), heure_pointage)
    
    # Calcul de la différence en minutes
    difference_minutes = (dt_pointage - dt_prevue).total_seconds() / 60
    
    # Définition des plages horaires spécifiques
    debut_plage = dt_prevue - timedelta(minutes=15)  # 07:45 pour 08:00
    fin_plage = dt_prevue - timedelta(minutes=5)     # 07:55 pour 08:00
    limite_retard = dt_prevue + timedelta(minutes=30) # 08:30 pour 08:00
    
    if debut_plage <= dt_pointage <= fin_plage:
        return "Présent à l'heure", 0, False
    elif fin_plage < dt_pointage < limite_retard:
        retard = (dt_pointage - fin_plage).total_seconds() / 60
        return "En retard", int(retard), False
    elif dt_pointage >= limite_retard:
        return "Absent", 30, True  # Retourne 30 minutes de retard et marque comme absent
    elif dt_pointage < debut_plage:
        avance = (debut_plage - dt_pointage).total_seconds() / 60
        return "En avance", int(-avance), False
    
    return "Non pointé", 0, False

def est_en_conge(personnel_id, date_check):
    """Vérifie si l'employé est en congé à une date donnée"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(*) FROM conges 
                WHERE personnel_id = ? 
                AND statut = 'Approuvé'
                AND date_debut <= ? 
                AND date_fin >= ?
                """,
                (personnel_id, date_check, date_check)
            )
            count = cur.fetchone()[0]
            return count > 0
    except Exception as e:
        st.error(f"Erreur vérification congé: {e}")
        return False
    finally:
        if conn:
            conn.close()
            
def est_jour_de_nuit(personnel_id, date_check):
    """Vérifie si c'est un jour de nuit pour le personnel mixte"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT poste, jours_travail FROM personnels WHERE id = ?",
                (personnel_id,)
            )
            result = cur.fetchone()
            
            if result and result['poste'] == 'Mixte' and result['jours_travail']:
                # Vérifier si aujourd'hui est un jour de nuit
                jours_nuit = result['jours_travail'].split(',')
                jour_actuel = date_check.strftime('%A')
                # Convertir en français si nécessaire
                jours_fr = {
                    'Monday': 'Lundi', 'Tuesday': 'Mardi', 'Wednesday': 'Mercredi',
                    'Thursday': 'Jeudi', 'Friday': 'Vendredi', 'Saturday': 'Samedi',
                    'Sunday': 'Dimanche'
                }
                return jours_fr.get(jour_actuel) in jours_nuit
            
            return result and result['poste'] == 'Nuit'
    except Exception as e:
        st.error(f"Erreur vérification jour de nuit: {e}")
        return False
    finally:
        if conn:
            conn.close()

def enregistrer_pointage_arrivee(personnel_id, date_pointage, heure_arrivee, motif_retard=None, notes=None, est_absent=False):
    # Vérifier si l'employé est en congé
    if est_en_conge(personnel_id, date_pointage):
        st.error("❌ Cet employé est en congé aujourd'hui. Pointage impossible.")
        return False, 0
    
    conn = get_connection()
    if conn is None:
        return False, 0
    try:
        personnel_id = int(personnel_id)
        
        with conn:
            cur = conn.cursor()
            # Vérifier si c'est un employé de nuit
            cur.execute("SELECT poste FROM personnels WHERE id = ?", (personnel_id,))
            poste_result = cur.fetchone()
            poste = poste_result['poste'] if poste_result else 'Jour'
            
            # Heure prévue
            cur.execute("SELECT heure_entree_prevue FROM personnels WHERE id = ?", (personnel_id,))
            res = cur.fetchone()
            if not res:
                return False, 0
            heure_prevue = _as_time(res['heure_entree_prevue'])

            # Convertir l'heure d'arrivée en time object pour les calculs
            heure_arrivee_time = _as_time(heure_arrivee)

            # Pour les employés de nuit, ajuster la logique de calcul
            if poste == 'Nuit':
                # Pour les nuitiers, on utilise une logique différente
                statut_arrivee, retard_minutes, est_absent_calc = calculer_statut_arrivee_nuit(heure_arrivee_time, heure_prevue)
            else:
                # Pour les employés de jour, logique normale
                statut_arrivee, retard_minutes, est_absent_calc = calculer_statut_arrivee(heure_arrivee_time, heure_prevue)

            # Convertir l'heure en string pour SQLite
            heure_arrivee_str = heure_arrivee_time.strftime('%H:%M:%S')

            # Si le système détecte une absence, enregistrer dans la table absences
            if est_absent or est_absent_calc:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO absences (personnel_id, date_absence, motif, justifie)
                    VALUES (?, ?, ?, ?)
                    """,
                    (personnel_id, date_pointage, motif_retard or f"Absence automatique (retard de {retard_minutes} minutes)", False)
                )
                # MAINTENANT on enregistre quand même le pointage avec le statut "Absent"
                statut_arrivee = "Absent"
            
            # Enregistrer le retard si applicable (seulement si < 30 minutes)
            if retard_minutes > 0 and retard_minutes < 30:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO retards (personnel_id, date_retard, retard_minutes, motif)
                    VALUES (?, ?, ?, ?)
                    """,
                    (personnel_id, date_pointage, retard_minutes, motif_retard),
                )

            # Vérifier si un pointage existe déjà pour cette journée
            cur.execute(
                "SELECT id FROM pointages WHERE personnel_id = ? AND date_pointage = ?",
                (personnel_id, date_pointage)
            )
            existing = cur.fetchone()

            if existing:
                # Mettre à jour l'arrivée
                cur.execute(
                    """
                    UPDATE pointages 
                    SET heure_arrivee = ?, statut_arrivee = ?, retard_minutes = ?, 
                        motif_retard = ?, notes = COALESCE(?, notes)
                    WHERE id = ?
                    """,
                    (heure_arrivee_str, statut_arrivee, retard_minutes, motif_retard, notes, existing['id'])
                )
            else:
                # Nouveau pointage
                cur.execute(
                    """
                    INSERT INTO pointages (personnel_id, date_pointage, heure_arrivee, statut_arrivee, retard_minutes, motif_retard, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (personnel_id, date_pointage, heure_arrivee_str, statut_arrivee, retard_minutes, motif_retard, notes),
                )
                
        return True, retard_minutes
    except Exception as e:
        st.error(f"Erreur enregistrement pointage arrivée: {e}")
        return False, 0
    finally:
        if conn:
            conn.close()

def calculer_statut_arrivee_nuit(heure_pointage, heure_prevue):
    """Calcule le statut de pointage pour les employés de nuit avec des règles spécifiques"""
    if not heure_pointage or not heure_prevue:
        return "Non pointé", 0, False
    
    heure_prevue = _as_time(heure_prevue)
    heure_pointage = _as_time(heure_pointage)
    
    # Convertir en datetime pour les calculs
    dt_prevue = datetime.combine(date.today(), heure_prevue)
    dt_pointage = datetime.combine(date.today(), heure_pointage)
    
    # Pour les nuitiers, on considère qu'ils peuvent pointer à tout moment
    # mais on garde une logique de retard basée sur leur heure prévue
    
    difference_minutes = (dt_pointage - dt_prevue).total_seconds() / 60
    
    # Plages plus flexibles pour les nuitiers
    debut_plage = dt_prevue - timedelta(minutes=30)  # 30 minutes avant
    fin_plage = dt_prevue + timedelta(minutes=60)    # 1 heure après
    
    if debut_plage <= dt_pointage <= fin_plage:
        return "Présent à l'heure", 0, False
    elif dt_pointage > fin_plage:
        retard = (dt_pointage - fin_plage).total_seconds() / 60
        return "En retard", int(retard), False
    elif dt_pointage < debut_plage:
        return "En avance", 0, False
    
    return "Non pointé", 0, False

def enregistrer_pointage_depart(personnel_id, date_pointage, heure_depart, motif_depart_avance=None, notes=None):
    # Vérifier si l'employé est en congé
    if est_en_conge(personnel_id, date_pointage):
        st.error("❌ Cet employé est en congé aujourd'hui. Pointage impossible.")
        return False, 0
    
    conn = get_connection()
    if conn is None:
        return False, 0
    try:
        personnel_id = int(personnel_id)
        
        with conn:
            cur = conn.cursor()
            # Heure de sortie prévue
            cur.execute("SELECT heure_sortie_prevue FROM personnels WHERE id = ?", (personnel_id,))
            res = cur.fetchone()
            if not res:
                return False, 0
            heure_sortie_prevue = _as_time(res['heure_sortie_prevue'])

            # Calcul départ en avance
            depart_avance_minutes = 0
            statut_depart = "Present"
            
            # Convertir l'heure en string pour SQLite
            heure_depart_str = heure_depart.strftime('%H:%M:%S') if isinstance(heure_depart, tm) else str(heure_depart)
            
            # Calculer la différence en minutes
            heure_depart_time = _as_time(heure_depart)
            heure_sortie_prevue_time = _as_time(heure_sortie_prevue)
            
            dt_depart = datetime.combine(date.today(), heure_depart_time)
            dt_sortie_prevue = datetime.combine(date.today(), heure_sortie_prevue_time)
            
            delta_minutes = (dt_sortie_prevue - dt_depart).total_seconds() / 60
            
            # Départ en avance seulement si plus de 5 minutes
            if delta_minutes > 5:
                depart_avance_minutes = int(delta_minutes)
                statut_depart = "Départ anticipé"

            # Vérifier si un pointage existe déjà pour cette journée
            cur.execute(
                "SELECT id FROM pointages WHERE personnel_id = ? AND date_pointage = ?",
                (personnel_id, date_pointage)
            )
            existing = cur.fetchone()

            if existing:
                # Mettre à jour le départ
                cur.execute(
                    """
                    UPDATE pointages 
                    SET heure_depart = ?, statut_depart = ?, depart_avance_minutes = ?, 
                        motif_depart_avance = ?, notes = COALESCE(?, notes)
                    WHERE id = ?
                    """,
                    (heure_depart_str, statut_depart, depart_avance_minutes, motif_depart_avance, notes, existing['id'])
                )
            else:
                # Nouveau pointage (cas rare où on pointerait le départ sans l'arrivée)
                cur.execute(
                    """
                    INSERT INTO pointages (personnel_id, date_pointage, heure_depart, statut_depart, depart_avance_minutes, motif_depart_avance, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (personnel_id, date_pointage, heure_depart_str, statut_depart, depart_avance_minutes, motif_depart_avance, notes),
                )
        return True, depart_avance_minutes
    except Exception as e:
        st.error(f"Erreur enregistrement pointage départ: {e}")
        return False, 0
    finally:
        if conn:
            conn.close()

def get_pointages_periode(date_debut, date_fin):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT pt.id, p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue, p.heure_sortie_prevue,
                   pt.date_pointage, pt.heure_arrivee, pt.heure_depart, pt.statut_arrivee, pt.statut_depart, 
                   pt.retard_minutes, pt.depart_avance_minutes, pt.motif_retard, pt.motif_depart_avance, pt.notes
            FROM pointages pt
            JOIN personnels p ON pt.personnel_id = p.id
            WHERE pt.date_pointage BETWEEN ? AND ?
            ORDER BY pt.date_pointage DESC, p.nom, p.prenom
            """,
            conn,
            params=(date_debut, date_fin),
        )
    except Exception as e:
        st.error(f"Erreur récupération pointages: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()
def get_retards_periode(date_debut, date_fin):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue,
                   r.date_retard, r.retard_minutes, r.motif, r.created_at
            FROM retards r
            JOIN personnels p ON r.personnel_id = p.id
            WHERE r.date_retard BETWEEN ? AND ?
            ORDER BY r.date_retard DESC, r.retard_minutes DESC
            """,
            conn,
            params=(date_debut, date_fin),
        )
    except Exception as e:
        st.error(f"Erreur récupération retards: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_absences_du_jour():
    """Récupère les absences du jour en excluant les groupes de nuit non actifs et les nuitiers qui pointent de jour"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        # Construire la requête pour exclure les groupes de nuit non actifs
        services_nuit = get_services_nuit()
        conditions = []
        
        for service in services_nuit:
            groupe_actif = get_groupe_nuit_actif_service(service)
            conditions.append(f"(p.service = '{service}' AND p.poste = 'Nuit' AND p.groupe_nuit = '{groupe_actif}')")
        
        query = """
            SELECT p.id, p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue,
                   a.motif, a.justifie, a.created_at
            FROM personnels p
            LEFT JOIN absences a ON p.id = a.personnel_id AND a.date_absence = ?
            WHERE p.actif = 1 
            AND (
                p.poste = 'Jour' 
                OR p.poste = 'Mixte'
        """
        
        if conditions:
            query += " OR " + " OR ".join(conditions)
        
        query += """
            )
            AND p.id NOT IN (
                SELECT personnel_id FROM pointages WHERE date_pointage = ? AND heure_arrivee IS NOT NULL
            )
            AND p.id NOT IN (
                SELECT personnel_id FROM conges 
                WHERE statut = 'Approuvé' 
                AND date_debut <= ? 
                AND date_fin >= ?
            )
            -- Exclure les employés de nuit qui ont pointé de jour
            AND NOT (
                p.poste = 'Nuit' 
                AND p.id IN (
                    SELECT personnel_id FROM pointages 
                    WHERE date_pointage = ? 
                    AND heure_arrivee IS NOT NULL
                    AND strftime('%H:%M', heure_arrivee) BETWEEN '06:00' AND '18:00'
                )
            )
            ORDER BY p.nom, p.prenom
        """
        
        return pd.read_sql_query(
            query,
            conn,
            params=(date.today(), date.today(), date.today(), date.today(), date.today()),
        )
    except Exception as e:
        st.error(f"Erreur récupération absences du jour: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_absences_periode(date_debut, date_fin):
    """Récupère les absences avec l'ID correct"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT a.id, a.date_absence, p.nom, p.prenom, p.service, p.poste, 
                   p.heure_entree_prevue, a.motif, a.justifie, 
                   a.certificat_justificatif IS NOT NULL as has_certificat,
                   a.created_at
            FROM absences a
            JOIN personnels p ON a.personnel_id = p.id
            WHERE a.date_absence BETWEEN ? AND ?
            ORDER BY a.date_absence DESC, p.nom, p.prenom
            """,
            conn,
            params=(date_debut, date_fin),
        )
    except Exception as e:
        st.error(f"Erreur récupération absences: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_stats_mensuelles():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """
            SELECT 
                p.nom, p.prenom, p.service,
                COUNT(pt.id) as jours_presents,
                SUM(CASE WHEN pt.statut_arrivee = 'Retard' THEN 1 ELSE 0 END) as jours_retard,
                SUM(CASE WHEN pt.statut_depart = 'Départ anticipé' THEN 1 ELSE 0 END) as jours_depart_anticipé,
                COALESCE(SUM(pt.retard_minutes),0) as total_retard_minutes,
                COALESCE(SUM(pt.depart_avance_minutes),0) as total_depart_avance_minutes
            FROM personnels p
            LEFT JOIN pointages pt ON p.id = pt.personnel_id 
                AND pt.date_pointage >= date('now', 'start of month')
            WHERE p.actif = 1
            GROUP BY p.id, p.nom, p.prenom, p.service
            ORDER BY p.nom, p.prenom
            """,
            conn,
        )
    except Exception as e:
        st.error(f"Erreur stats mensuelles: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def marquer_absence_automatique():
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT p.id, p.nom, p.prenom, p.heure_entree_prevue
                FROM personnels p
                WHERE p.actif = 1 
                AND p.id NOT IN (
                    SELECT personnel_id FROM pointages WHERE date_pointage = ? AND heure_arrivee IS NOT NULL
                )
                AND p.id NOT IN (
                    SELECT personnel_id FROM conges 
                    WHERE statut = 'Approuvé' 
                    AND date_debut <= ? 
                    AND date_fin >= ?
                )
                """,
                (date.today(), date.today(), date.today())
            )
            employes_absents = cur.fetchall()
            
            maintenant = datetime.now().time()
            
            for emp in employes_absents:
                emp_id, nom, prenom, heure_prevue = emp['id'], emp['nom'], emp['prenom'], emp['heure_entree_prevue']
                heure_prevue = _as_time(heure_prevue)
                
                heure_limite = (datetime.combine(date.today(), heure_prevue) + timedelta(minutes=30)).time()
                
                if maintenant > heure_limite:
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO absences (personnel_id, date_absence, motif, justifie)
                        VALUES (?, ?, ?, ?)
                        """,
                        (emp_id, date.today(), "Absence non justifiée (automatique)", False)
                    )
        return True
    except Exception as e:
        st.error(f"Erreur marquage automatique des absences: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_personnel_par_service(groupe_nuit_actif=None):
    conn = get_connection()
    if conn is None:
        return {}
    try:
        query = """
            SELECT id, nom, prenom, service, poste, heure_entree_prevue, heure_sortie_prevue, 
                   groupe_nuit, jours_travail, actif 
            FROM personnels 
            WHERE actif = 1 
        """
        
        params = []
        if groupe_nuit_actif:
            services_nuit = get_services_nuit()
            conditions = []
            
            for service in services_nuit:
                groupe_actif_service = get_groupe_nuit_actif_service(service)
                if groupe_actif_service == groupe_nuit_actif:
                    conditions.append(f"(service = '{service}' AND (poste = 'Nuit' OR poste = 'Mixte') AND groupe_nuit = '{groupe_nuit_actif}')")
            
            if conditions:
                query += " AND (poste = 'Jour' OR " + " OR ".join(conditions) + ")"
            else:
                query += " AND (poste = 'Jour' OR poste = 'Mixte')"
        else:
            query += " AND (poste = 'Jour' OR poste = 'Mixte' OR poste = 'Nuit')"
            
        query += " ORDER BY service, nom, prenom"
        
        df = pd.read_sql_query(query, conn, params=params)
        
        personnel_par_service = {}
        for _, row in df.iterrows():
            service = row['service']
            if service not in personnel_par_service:
                personnel_par_service[service] = []
            personnel_par_service[service].append(row.to_dict())
            
        return personnel_par_service
    except Exception as e:
        st.error(f"Erreur récupération personnel par service: {e}")
        return {}
    finally:
        if conn:
            conn.close()

def get_pointages_du_jour():
    """Récupère les pointages du jour en excluant les groupes de nuit non actifs"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        # Construire la requête pour exclure les groupes de nuit non actifs
        services_nuit = get_services_nuit()
        conditions = []
        
        for service in services_nuit:
            groupe_actif = get_groupe_nuit_actif_service(service)
            conditions.append(f"(p.service = '{service}' AND p.poste = 'Nuit' AND p.groupe_nuit = '{groupe_actif}')")
        
        query = """
            SELECT p.id, p.nom, p.prenom, p.service, p.poste, p.heure_entree_prevue, p.heure_sortie_prevue,
                   pt.heure_arrivee, pt.heure_depart, pt.statut_arrivee, pt.statut_depart, 
                   pt.retard_minutes, pt.depart_avance_minutes, pt.motif_retard, pt.motif_depart_avance, pt.notes
            FROM pointages pt
            JOIN personnels p ON pt.personnel_id = p.id
            WHERE pt.date_pointage = ?
            AND (
                p.poste = 'Jour' 
                OR p.poste = 'Mixte'
        """
        
        if conditions:
            query += " OR " + " OR ".join(conditions)
        
        query += ") ORDER BY p.service, p.nom, p.prenom"
        
        return pd.read_sql_query(query, conn, params=(date.today(),))
    except Exception as e:
        st.error(f"Erreur récupération pointages du jour: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def enregistrer_absence(personnel_id, date_absence, motif, justifie=False, certificat_file=None):
    conn = get_connection()
    if conn is None:
        return False
    try:
        # Conversion de numpy.int64 en int Python standard
        personnel_id = int(personnel_id) if hasattr(personnel_id, 'item') else int(personnel_id)
        
        with conn:
            cur = conn.cursor()
            if certificat_file:
                # Lire directement les bytes du fichier uploadé
                file_data = certificat_file.getvalue()
                file_type = certificat_file.type.split('/')[-1]
                
                cur.execute(
                    """
                    INSERT INTO absences (personnel_id, date_absence, motif, justifie, certificat_justificatif, type_certificat)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (personnel_id, date_absence)
                    DO UPDATE SET 
                        motif = excluded.motif,
                        justifie = excluded.justifie,
                        certificat_justificatif = excluded.certificat_justificatif,
                        type_certificat = excluded.type_certificat
                    """,
                    (personnel_id, date_absence, motif, justifie, file_data, file_type),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO absences (personnel_id, date_absence, motif, justifie)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (personnel_id, date_absence)
                    DO UPDATE SET 
                        motif = excluded.motif,
                        justifie = excluded.justifie
                    """,
                    (personnel_id, date_absence, motif, justifie),
                )
        return True
    except Exception as e:
        st.error(f"Erreur enregistrement absence: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_certificat_absence(absence_id):
    conn = get_connection()
    if conn is None:
        return None, None
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT certificat_justificatif, type_certificat FROM absences WHERE id = ?",
                (absence_id,)
            )
            result = cur.fetchone()
            if result:
                return result['certificat_justificatif'], result['type_certificat']
            return None, None
    except Exception as e:
        st.error(f"Erreur récupération certificat: {e}")
        return None, None
    finally:
        if conn:
            conn.close()

# =========================
# FONCTIONS GESTION DES CONGES
# =========================

def get_quota_conges(personnel_id):
    """Récupère le quota de congés d'un employé - VERSION AMÉLIORÉE"""
    conn = get_connection()
    if conn is None:
        return None
    
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT jours_alloues, jours_pris, jours_restants FROM quotas_conges WHERE personnel_id = ?",
                (personnel_id,)
            )
            result = cur.fetchone()
            if result:
                return {
                    'jours_alloues': result['jours_alloues'],
                    'jours_pris': result['jours_pris'],
                    'jours_restants': result['jours_restants']
                }
            else:
                # Initialiser le quota si inexistant
                cur.execute(
                    "INSERT INTO quotas_conges (personnel_id, jours_alloues, jours_restants) VALUES (?, 25, 25)",
                    (personnel_id,)
                )
                return {
                    'jours_alloues': 25,
                    'jours_pris': 0,
                    'jours_restants': 25
                }
    except Exception as e:
        st.error(f"Erreur récupération quota congés: {e}")
        return None
    finally:
        if conn:
            conn.close()

def calculer_jours_conges(date_debut, date_fin):
    """Calcule le nombre de jours de congé entre deux dates"""
    return (date_fin - date_debut).days + 1

def verifier_disponibilite_conge(personnel_id, date_debut, date_fin):
    """Vérifie si l'employé n'a pas déjà des congés qui se chevauchent"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(*) FROM conges 
                WHERE personnel_id = ? 
                AND statut IN ('En attente', 'Approuvé')
                AND (
                    (date_debut BETWEEN ? AND ?) OR
                    (date_fin BETWEEN ? AND ?) OR
                    (date_debut <= ? AND date_fin >= ?)
                )
                """,
                (personnel_id, date_debut, date_fin, date_debut, date_fin, date_debut, date_fin)
            )
            count = cur.fetchone()[0]
            return count == 0
    except Exception as e:
        st.error(f"Erreur vérification disponibilité congé: {e}")
        return False
    finally:
        if conn:
            conn.close()

def demander_conge(personnel_id, date_debut, date_fin, type_conge, motif):
    """Enregistre une nouvelle demande de congé - VERSION CORRIGÉE"""
    conn = get_connection()
    if conn is None:
        return False, "Erreur de connexion"
    
    try:
        jours_demandes = (date_fin - date_debut).days + 1
        
        # Vérifier le quota disponible
        quota = get_quota_conges(personnel_id)
        if not quota or quota['jours_restants'] < jours_demandes:
            return False, f"Quota insuffisant. Jours restants: {quota['jours_restants'] if quota else 0}"
        
        # Vérifier les chevauchements
        if not verifier_disponibilite_conge(personnel_id, date_debut, date_fin):
            return False, "Période déjà couverte par une autre demande"
        
        with conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO conges (personnel_id, date_debut, date_fin, type_conge, motif, statut)
                VALUES (?, ?, ?, ?, ?, 'En attente')
                """,
                (personnel_id, date_debut, date_fin, type_conge, motif)
            )
        
        # Récupérer le nom de l'employé pour le message de confirmation
        nom_employe = get_nom_employe(personnel_id)
        return True, f"Demande de congé pour {nom_employe} enregistrée avec succès"
    except Exception as e:
        return False, f"Erreur: {str(e)}"
    finally:
        if conn:
            conn.close()

def approuver_conge(conge_id):
    """Approuve une demande de congé et met à jour le quota - VERSION CORRIGÉE"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            # Récupérer les infos du congé
            cur.execute(
                "SELECT personnel_id, date_debut, date_fin FROM conges WHERE id = ?",
                (conge_id,)
            )
            conge = cur.fetchone()
            
            if not conge:
                return False
            
            personnel_id, date_debut, date_fin = conge['personnel_id'], conge['date_debut'], conge['date_fin']
            jours_demandes = (datetime.strptime(date_fin, '%Y-%m-%d').date() - datetime.strptime(date_debut, '%Y-%m-%d').date()).days + 1
            
            # Mettre à jour le quota
            cur.execute(
                """
                UPDATE quotas_conges 
                SET jours_pris = jours_pris + ?,
                    jours_restants = jours_alloues - (jours_pris + ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE personnel_id = ?
                """,
                (jours_demandes, jours_demandes, personnel_id)
            )
            
            # Mettre à jour le statut du congé
            cur.execute(
                "UPDATE conges SET statut = 'Approuvé' WHERE id = ?",
                (conge_id,)
            )
        return True
    except Exception as e:
        st.error(f"Erreur approbation congé: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_quota_conges(personnel_id):
    """Récupère le quota de congés d'un employé"""
    conn = get_connection()
    if conn is None:
        return None
    
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT jours_alloues, jours_pris, jours_restants FROM quotas_conges WHERE personnel_id = ?",
                (personnel_id,)
            )
            result = cur.fetchone()
            if result:
                return {
                    'jours_alloues': result['jours_alloues'],
                    'jours_pris': result['jours_pris'],
                    'jours_restants': result['jours_restants']
                }
            else:
                # Initialiser le quota si inexistant
                cur.execute(
                    "INSERT INTO quotas_conges (personnel_id) VALUES (?)",
                    (personnel_id,)
                )
                return {
                    'jours_alloues': 21,
                    'jours_pris': 0,
                    'jours_restants': 21
                }
    except Exception as e:
        st.error(f"Erreur récupération quota congés: {e}")
        return None
    finally:
        if conn:
            conn.close()

def verifier_disponibilite_conge(personnel_id, date_debut, date_fin):
    """Vérifie si l'employé n'a pas déjà des congés qui se chevauchent - VERSION CORRIGÉE"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(*) FROM conges 
                WHERE personnel_id = ? 
                AND statut IN ('En attente', 'Approuvé')
                AND (
                    (date_debut BETWEEN ? AND ?) OR
                    (date_fin BETWEEN ? AND ?) OR
                    (date_debut <= ? AND date_fin >= ?)
                )
                """,
                (personnel_id, date_debut, date_fin, date_debut, date_fin, date_debut, date_fin)
            )
            count = cur.fetchone()[0]
            return count == 0
    except Exception as e:
        st.error(f"Erreur vérification disponibilité congé: {e}")
        return False
    finally:
        if conn:
            conn.close()

def approuver_conge(conge_id):
    """Approuve une demande de congé et met à jour le quota - VERSION CORRIGÉE"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            # Récupérer les infos du congé
            cur.execute(
                "SELECT personnel_id, date_debut, date_fin FROM conges WHERE id = ?",
                (conge_id,)
            )
            conge = cur.fetchone()
            
            if not conge:
                return False
            
            personnel_id, date_debut, date_fin = conge['personnel_id'], conge['date_debut'], conge['date_fin']
            jours_demandes = (datetime.strptime(date_fin, '%Y-%m-%d').date() - datetime.strptime(date_debut, '%Y-%m-%d').date()).days + 1
            
            # Mettre à jour le quota
            cur.execute(
                """
                UPDATE quotas_conges 
                SET jours_pris = jours_pris + ?,
                    jours_restants = jours_alloues - (jours_pris + ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE personnel_id = ?
                """,
                (jours_demandes, jours_demandes, personnel_id)
            )
            
            # Mettre à jour le statut du congé
            cur.execute(
                "UPDATE conges SET statut = 'Approuvé' WHERE id = ?",
                (conge_id,)
            )
        return True
    except Exception as e:
        st.error(f"Erreur approbation congé: {e}")
        return False
    finally:
        if conn:
            conn.close()

def rejeter_conge(conge_id):
    """Rejette une demande de congé"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE conges SET statut = 'Rejeté' WHERE id = ?",
                (conge_id,)
            )
        return True
    except Exception as e:
        st.error(f"Erreur rejet congé: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_conges_employe(personnel_id):
    """Récupère tous les congés d'un employé - VERSION CORRIGÉE"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        return pd.read_sql_query(
            """
            SELECT 
                c.id, 
                c.date_debut, 
                c.date_fin, 
                c.type_conge, 
                c.motif, 
                c.statut, 
                c.created_at,
                (julianday(c.date_fin) - julianday(c.date_debut) + 1) as duree_jours
            FROM conges c
            WHERE c.personnel_id = ?
            ORDER BY c.date_debut DESC
            """,
            conn,
            params=(personnel_id,)
        )
    except Exception as e:
        st.error(f"Erreur récupération congés employé: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_tous_les_conges(filtre_statut="Tous"):
    """Récupère tous les congés avec option de filtre par statut - VERSION CORRIGÉE"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT 
                c.id,
                p.nom, 
                p.prenom, 
                p.service, 
                c.date_debut, 
                c.date_fin, 
                c.type_conge, 
                c.motif, 
                c.statut, 
                c.created_at,
                (julianday(c.date_fin) - julianday(c.date_debut) + 1) as duree_jours
            FROM conges c
            JOIN personnels p ON c.personnel_id = p.id
        """
        
        params = []
        if filtre_statut != "Tous":
            query += " WHERE c.statut = ?"
            params.append(filtre_statut)
        
        query += " ORDER BY c.created_at DESC"
        
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"Erreur récupération tous les congés: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()
            
def modifier_pointage(personnel_id, date_pointage, nouvelle_heure_arrivee=None, nouvelle_heure_depart=None):
    """Modifie les heures de pointage d'un employé"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            
            # Récupérer les heures prévues
            cur.execute(
                "SELECT heure_entree_prevue, heure_sortie_prevue FROM personnels WHERE id = ?",
                (personnel_id,)
            )
            emp_data = cur.fetchone()
            
            if not emp_data:
                return False
                
            heure_entree_prevue = _as_time(emp_data['heure_entree_prevue'])
            heure_sortie_prevue = _as_time(emp_data['heure_sortie_prevue'])
            
            # Vérifier si un pointage existe
            cur.execute(
                "SELECT id, heure_arrivee, heure_depart FROM pointages WHERE personnel_id = ? AND date_pointage = ?",
                (personnel_id, date_pointage)
            )
            pointage = cur.fetchone()
            
            if not pointage:
                st.error("Aucun pointage trouvé pour cette date")
                return False
            
            # Préparer les updates
            updates = []
            params = []
            
            if nouvelle_heure_arrivee:
                # Convertir et calculer le nouveau statut
                nouvelle_heure_arrivee_time = _as_time(nouvelle_heure_arrivee)
                statut_arrivee, retard_minutes, est_absent = calculer_statut_arrivee(nouvelle_heure_arrivee_time, heure_entree_prevue)
                
                updates.append("heure_arrivee = ?")
                updates.append("statut_arrivee = ?")
                updates.append("retard_minutes = ?")
                
                params.extend([
                    nouvelle_heure_arrivee_time.strftime('%H:%M:%S'),
                    statut_arrivee,
                    retard_minutes
                ])
                
                # Mettre à jour la table retards si nécessaire
                if retard_minutes > 0 and retard_minutes < 30:
                    cur.execute(
                        """
                        INSERT OR REPLACE INTO retards 
                        (personnel_id, date_retard, retard_minutes, motif)
                        VALUES (?, ?, ?, ?)
                        """,
                        (personnel_id, date_pointage, retard_minutes, "Retard modifié manuellement")
                    )
            
            if nouvelle_heure_depart:
                # Convertir et calculer le départ anticipé
                nouvelle_heure_depart_time = _as_time(nouvelle_heure_depart)
                depart_avance_minutes = 0
                statut_depart = "Present"
                
                dt_depart = datetime.combine(date.today(), nouvelle_heure_depart_time)
                dt_sortie_prevue = datetime.combine(date.today(), heure_sortie_prevue)
                
                delta_minutes = (dt_sortie_prevue - dt_depart).total_seconds() / 60
                
                if delta_minutes > 5:
                    depart_avance_minutes = int(delta_minutes)
                    statut_depart = "Départ anticipé"
                
                updates.append("heure_depart = ?")
                updates.append("statut_depart = ?")
                updates.append("depart_avance_minutes = ?")
                
                params.extend([
                    nouvelle_heure_depart_time.strftime('%H:%M:%S'),
                    statut_depart,
                    depart_avance_minutes
                ])
            
            if updates:
                query = f"UPDATE pointages SET {', '.join(updates)} WHERE id = ?"
                params.append(pointage['id'])
                
                cur.execute(query, params)
                
            return True
            
    except Exception as e:
        st.error(f"Erreur modification pointage: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_conges_en_cours():
    """Récupère les congés en cours (aujourd'hui dans la période) - VERSION CORRIGÉE"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        return pd.read_sql_query(
            """
            SELECT 
                p.nom, 
                p.prenom, 
                p.service, 
                c.date_debut, 
                c.date_fin, 
                c.type_conge,
                (julianday(c.date_fin) - julianday(c.date_debut) + 1) as duree_jours,
                c.statut
            FROM conges c
            JOIN personnels p ON c.personnel_id = p.id
            WHERE c.statut = 'Approuvé'
            AND c.date_debut <= date('now')
            AND c.date_fin >= date('now')
            ORDER BY p.service, p.nom
            """,
            conn
        )
    except Exception as e:
        st.error(f"Erreur récupération congés en cours: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def modifier_quota_conges(personnel_id, nouveaux_jours_alloues):
    """Modifie le quota de congés d'un employé"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE quotas_conges 
                SET jours_alloues = ?,
                    jours_restants = ? - jours_pris,
                    updated_at = CURRENT_TIMESTAMP
                WHERE personnel_id = ?
                """,
                (nouveaux_jours_alloues, nouveaux_jours_alloues, personnel_id)
            )
        return True
    except Exception as e:
        st.error(f"Erreur modification quota: {e}")
        return False
    finally:
        if conn:
            conn.close()

# =========================
# FONCTIONS TOURS DE ROLE NUIT
# =========================

def definir_groupe_nuit_du_jour(service, groupe_actif):
    """Définit le groupe de nuit actif pour un service spécifique"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO tours_role_nuit (date_tour, service, groupe_actif)
                VALUES (?, ?, ?)
                ON CONFLICT (date_tour, service)
                DO UPDATE SET groupe_actif = excluded.groupe_actif
                """,
                (date.today(), service, groupe_actif)
            )
            
            # Mettre à jour également la table groupes_nuit_par_service
            cur.execute(
                """
                INSERT OR REPLACE INTO groupes_nuit_par_service (service, groupe_actif)
                VALUES (?, ?)
                """,
                (service, groupe_actif)
            )
        return True
    except Exception as e:
        st.error(f"Erreur définition groupe de nuit: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_groupe_nuit_actif_service(service):
    """Récupère le groupe de nuit actif pour un service spécifique"""
    conn = get_connection()
    if conn is None:
        return None
    
    try:
        with conn:
            cur = conn.cursor()
            # D'abord vérifier s'il y a une entrée pour aujourd'hui
            cur.execute(
                "SELECT groupe_actif FROM tours_role_nuit WHERE date_tour = ? AND service = ?",
                (date.today(), service)
            )
            result = cur.fetchone()
            
            if result:
                return result['groupe_actif']
            else:
                # Si pas d'entrée pour aujourd'hui, utiliser la configuration par défaut du service
                cur.execute(
                    "SELECT groupe_actif FROM groupes_nuit_par_service WHERE service = ?",
                    (service,)
                )
                result = cur.fetchone()
                return result['groupe_actif'] if result else 'A'
    except Exception as e:
        st.error(f"Erreur récupération groupe de nuit: {e}")
        return 'A'
    finally:
        if conn:
            conn.close()

def get_historique_tours_nuit(service=None):
    """Récupère l'historique des tours de rôle"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT date_tour, service, groupe_actif, created_at
            FROM tours_role_nuit
        """
        params = []
        
        if service:
            query += " WHERE service = ?"
            params.append(service)
            
        query += " ORDER BY date_tour DESC, service LIMIT 30"
        
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"Erreur récupération historique tours: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()
            
def modifier_historique_pointage(pointage_id, nouvelle_heure_arrivee=None, nouvelle_heure_depart=None, nouveau_statut=None, nouveau_motif=None):
    """Modifie manuellement un pointage - VERSION AVEC SUPPORT HEURE MANUELLE"""
    conn = get_connection()
    if conn is None:
        return False, "Erreur de connexion à la base de données"
    
    try:
        with conn:
            cur = conn.cursor()
            
            # Récupérer les informations complètes du pointage
            cur.execute("SELECT * FROM pointages WHERE id = ?", (pointage_id,))
            pointage_actuel = cur.fetchone()
            
            if not pointage_actuel:
                return False, "Pointage non trouvé"
            
            # Récupérer les informations de l'employé
            cur.execute("SELECT heure_entree_prevue, heure_sortie_prevue FROM personnels WHERE id = ?", (pointage_actuel['personnel_id'],))
            employe_info = cur.fetchone()
            
            if not employe_info:
                return False, "Employé non trouvé"
            
            # Préparer les mises à jour
            updates = []
            params = []
            
            # Gestion de l'heure d'arrivée
            if nouvelle_heure_arrivee is not None:
                if isinstance(nouvelle_heure_arrivee, str):
                    # Convertir la chaîne en objet time
                    nouvelle_heure_arrivee = parse_heure_manuelle(nouvelle_heure_arrivee)
                
                heure_prevue = _as_time(employe_info['heure_entree_prevue'])
                
                # Recalculer le statut et le retard
                statut_arrivee, retard_minutes, est_absent = calculer_statut_arrivee(nouvelle_heure_arrivee, heure_prevue)
                
                updates.append("heure_arrivee = ?")
                updates.append("statut_arrivee = ?")
                updates.append("retard_minutes = ?")
                
                params.extend([
                    nouvelle_heure_arrivee.strftime('%H:%M:%S'),
                    statut_arrivee,
                    retard_minutes
                ])
                
                # Mettre à jour la table retards
                cur.execute("DELETE FROM retards WHERE personnel_id = ? AND date_retard = ?", (pointage_actuel['personnel_id'], pointage_actuel['date_pointage']))
                
                if retard_minutes > 0 and retard_minutes < 30:
                    cur.execute("INSERT INTO retards (personnel_id, date_retard, retard_minutes, motif) VALUES (?, ?, ?, ?)",
                               (pointage_actuel['personnel_id'], pointage_actuel['date_pointage'], retard_minutes, nouveau_motif or "Retard modifié manuellement"))
            
            # Gestion de l'heure de départ
            if nouvelle_heure_depart is not None:
                if isinstance(nouvelle_heure_depart, str):
                    # Convertir la chaîne en objet time
                    nouvelle_heure_depart = parse_heure_manuelle(nouvelle_heure_depart)
                
                heure_sortie_prevue = _as_time(employe_info['heure_sortie_prevue'])
                
                # Calculer le départ anticipé
                depart_avance_minutes = 0
                statut_depart = "Present"
                
                dt_depart = datetime.combine(date.today(), nouvelle_heure_depart)
                dt_sortie_prevue = datetime.combine(date.today(), heure_sortie_prevue)
                
                delta_minutes = (dt_sortie_prevue - dt_depart).total_seconds() / 60
                
                if delta_minutes > 5:
                    depart_avance_minutes = int(delta_minutes)
                    statut_depart = "Départ anticipé"
                
                updates.append("heure_depart = ?")
                updates.append("statut_depart = ?")
                updates.append("depart_avance_minutes = ?")
                
                params.extend([
                    nouvelle_heure_depart.strftime('%H:%M:%S'),
                    statut_depart,
                    depart_avance_minutes
                ])
            
            # Gestion du statut manuel
            if nouveau_statut:
                updates.append("statut_arrivee = ?")
                params.append(nouveau_statut)
            
            # Gestion du motif
            if nouveau_motif is not None:
                updates.append("motif_retard = ?")
                params.append(nouveau_motif)
            
            # Appliquer les mises à jour
            if updates:
                query = f"UPDATE pointages SET {', '.join(updates)} WHERE id = ?"
                params.append(pointage_id)
                cur.execute(query, params)
                
            return True, "Pointage modifié avec succès"
            
    except Exception as e:
        return False, f"Erreur lors de la modification: {str(e)}"
    finally:
        if conn:
            conn.close()
            
def get_pointage_id_from_selection(selection_text):
    """Extrait l'ID du pointage à partir du texte de sélection"""
    try:
        if 'ID: ' in selection_text:
            return int(selection_text.split('ID: ')[1])
        return None
    except (ValueError, IndexError):
        return None

def rechercher_pointages_avances(nom=None, prenom=None, service=None, date_debut=None, date_fin=None, statut=None):
    """Recherche avancée dans les pointages - VERSION CORRIGÉE"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT 
                p.id as personnel_id,
                p.nom,
                p.prenom,
                p.service,
                p.poste,
                p.heure_entree_prevue,
                p.heure_sortie_prevue,
                pt.id as pointage_id,
                pt.date_pointage,
                pt.heure_arrivee,
                pt.heure_depart,
                pt.statut_arrivee,
                pt.statut_depart,
                pt.retard_minutes,
                pt.depart_avance_minutes,
                pt.motif_retard,
                pt.motif_depart_avance,
                pt.notes,
                pt.created_at
            FROM pointages pt
            JOIN personnels p ON pt.personnel_id = p.id
            WHERE 1=1
        """
        params = []
        
        if nom:
            query += " AND p.nom LIKE ?"
            params.append(f"%{nom}%")
        if prenom:
            query += " AND p.prenom LIKE ?"
            params.append(f"%{prenom}%")
        if service:
            query += " AND p.service = ?"
            params.append(service)
        if date_debut:
            query += " AND pt.date_pointage >= ?"
            params.append(date_debut)
        if date_fin:
            query += " AND pt.date_pointage <= ?"
            params.append(date_fin)
        if statut:
            query += " AND pt.statut_arrivee = ?"
            params.append(statut)
        
        query += " ORDER BY pt.date_pointage DESC, p.nom, p.prenom"
        
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"Erreur recherche avancée: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_groupes_par_service():
    """Récupère la configuration des groupes par service"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        return pd.read_sql_query(
            """
            SELECT g.service, g.groupe_actif, g.derniere_maj,
                   COUNT(p.id) as nb_employes
            FROM groupes_nuit_par_service g
            LEFT JOIN personnels p ON g.service = p.service AND p.poste = 'Nuit' AND p.actif = 1
            GROUP BY g.service, g.groupe_actif, g.derniere_maj
            ORDER BY g.service
            """,
            conn
        )
    except Exception as e:
        st.error(f"Erreur récupération groupes par service: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_personnel_nuit_par_service():
    """Récupère le personnel de nuit groupé par service et groupe"""
    conn = get_connection()
    if conn is None:
        return {}
    
    try:
        df = pd.read_sql_query(
            """
            SELECT p.nom, p.prenom, p.service, p.heure_entree_prevue, p.heure_sortie_prevue,
                   g.groupe_actif
            FROM personnels p
            LEFT JOIN groupes_nuit_par_service g ON p.service = g.service
            WHERE p.actif = 1 AND p.poste = 'Nuit'
            ORDER BY p.service, p.nom, p.prenom
            """,
            conn
        )
        
        personnel_par_service = {}
        for _, row in df.iterrows():
            service = row['service']
            if service not in personnel_par_service:
                personnel_par_service[service] = {'A': [], 'B': []}
            
            groupe = row['groupe_actif'] if row['groupe_actif'] in ['A', 'B'] else 'A'
            personnel_par_service[service][groupe].append(row.to_dict())
            
        return personnel_par_service
    except Exception as e:
        st.error(f"Erreur récupération personnel nuit: {e}")
        return {}
    finally:
        if conn:
            conn.close()
            
def parse_heure_manuelle(heure_texte):
    """Convertit une chaîne de caractères en objet time avec plusieurs formats supportés"""
    if not heure_texte:
        return None
    
    formats = [
        '%H:%M:%S',    # 08:30:00
        '%H:%M',       # 08:30
        '%Hh%M',       # 08h30
        '%Hh%M:%S',    # 08h30:00
        '%H.%M',       # 08.30
        '%H.%M.%S'     # 08.30.00
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(heure_texte, fmt).time()
        except ValueError:
            continue
    
    # Essayer de parser les formats avec des espaces
    heure_texte = heure_texte.replace(' ', '')
    for fmt in formats:
        try:
            return datetime.strptime(heure_texte, fmt).time()
        except ValueError:
            continue
    
    raise ValueError(f"Format d'heure non reconnu: {heure_texte}")
            
def supprimer_definitivement_personnel(personnel_id):
    """Supprime définitivement un employé et toutes ses données associées - VERSION CORRIGÉE"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            
            # Activer les contraintes de clé étrangère
            cur.execute("PRAGMA foreign_keys = ON")
            
            # Liste des tables à nettoyer dans l'ordre correct
            tables = [
                'retards', 
                'absences', 
                'pointages', 
                'conges', 
                'quotas_conges',
                'personnels'  # Doit être en dernier
            ]
            
            for table in tables:
                try:
                    if table == 'personnels':
                        # Supprimer l'employé lui-même
                        cur.execute(f"DELETE FROM {table} WHERE id = ?", (personnel_id,))
                    else:
                        # Supprimer les données associées
                        cur.execute(f"DELETE FROM {table} WHERE personnel_id = ?", (personnel_id,))
                except sqlite3.Error as e:
                    print(f"DEBUG: Erreur suppression table {table}: {e}")
                    # Dans certains cas, on peut ignorer certaines erreurs si la table n'existe pas
                    continue
            
            # Vérifier que l'employé a bien été supprimé
            cur.execute("SELECT COUNT(*) FROM personnels WHERE id = ?", (personnel_id,))
            count = cur.fetchone()[0]
            
            if count == 0:
                print(f"DEBUG: Employé {personnel_id} supprimé avec succès")
                return True
            else:
                print(f"DEBUG: Échec de la suppression de l'employé {personnel_id}")
                return False
                
    except Exception as e:
        print(f"DEBUG: Erreur suppression définitive: {e}")
        st.error(f"Erreur suppression définitive: {e}")
        return False
    finally:
        if conn:
            conn.close()
            
def justifier_absence(absence_id, certificat_file, motif_justification=None):
    """Enregistre un justificatif pour une absence"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            
            # Lire directement les bytes du fichier uploadé
            file_data = certificat_file.getvalue()
            file_type = certificat_file.type.split('/')[-1]
            
            # Vérifier le type de fichier
            if file_type not in ['jpeg', 'jpg', 'png', 'pdf']:
                st.error("❌ Format de fichier non supporté. Utilisez JPEG, PNG ou PDF.")
                return False
            
            # Mettre à jour l'absence avec le justificatif
            cur.execute(
                """
                UPDATE absences 
                SET justifie = TRUE, 
                    certificat_justificatif = ?,
                    type_certificat = ?,
                    motif = COALESCE(?, motif)
                WHERE id = ?
                """,
                (file_data, file_type, motif_justification, absence_id)
            )
            
            return True
    except Exception as e:
        st.error(f"Erreur lors de l'enregistrement du justificatif: {e}")
        return False
    finally:
        if conn:
            conn.close()
            
def debug_conges():
    """Fonction de débogage pour vérifier les congés dans la base"""
    conn = get_connection()
    if conn is None:
        return
    
    try:
        # Vérifier tous les congés
        st.write("### 📋 Tous les congés dans la base:")
        all_conges = pd.read_sql_query("SELECT * FROM conges", conn)
        st.dataframe(all_conges)
        
        # Vérifier les congés approuvés pour aujourd'hui
        st.write("### 🎯 Congés approuvés pour aujourd'hui:")
        today_conges = pd.read_sql_query(
            """
            SELECT c.*, p.nom, p.prenom 
            FROM conges c 
            JOIN personnels p ON c.personnel_id = p.id 
            WHERE c.statut = 'Approuvé' 
            AND c.date_debut <= date('now') 
            AND c.date_fin >= date('now')
            """, 
            conn
        )
        st.dataframe(today_conges)
        
    except Exception as e:
        st.error(f"Erreur débogage: {e}")
    finally:
        if conn:
            conn.close()


            
def afficher_justificatif_absence(absence_id):
    """Affiche ou permet de télécharger le justificatif d'absence"""
    conn = get_connection()
    if conn is None:
        return
    
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT certificat_justificatif, type_certificat FROM absences WHERE id = ?",
                (absence_id,)
            )
            result = cur.fetchone()
            
            if result and result['certificat_justificatif']:
                file_data = result['certificat_justificatif']
                file_type = result['type_certificat']
                
                # Créer un bouton de téléchargement
                if file_type in ['jpeg', 'jpg', 'png']:
                    # Pour les images, utiliser st.image avec les bytes directement
                    st.image(file_data, caption="Certificat médical", use_column_width=True)
                    st.download_button(
                        label="📥 Télécharger l'image",
                        data=file_data,
                        file_name=f"certificat_absence_{absence_id}.{file_type}",
                        mime=f"image/{file_type}"
                    )
                elif file_type == 'pdf':
                    # Pour les PDF
                    st.download_button(
                        label="📥 Télécharger le PDF",
                        data=file_data,
                        file_name=f"certificat_absence_{absence_id}.pdf",
                        mime="application/pdf"
                    )
                    st.info("📄 Document PDF - Cliquez sur télécharger pour visualiser")
                
                return True
            else:
                st.info("ℹ️ Aucun justificatif disponible")
                return False
                
    except Exception as e:
        st.error(f"Erreur lors de la récupération du justificatif: {e}")
        return False
    finally:
        if conn:
            conn.close()
            
def modifier_quota_conges_employe(personnel_id, nouveaux_jours_alloues):
    """Modifie le quota de congés d'un employé spécifique"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        with conn:
            cur = conn.cursor()
            # Récupérer le nombre de jours déjà pris
            cur.execute(
                "SELECT jours_pris FROM quotas_conges WHERE personnel_id = ?",
                (personnel_id,)
            )
            result = cur.fetchone()
            
            if result:
                jours_pris = result['jours_pris']
                jours_restants = nouveaux_jours_alloues - jours_pris
                
                # Mettre à jour le quota
                cur.execute(
                    """
                    UPDATE quotas_conges 
                    SET jours_alloues = ?,
                        jours_restants = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE personnel_id = ?
                    """,
                    (nouveaux_jours_alloues, jours_restants, personnel_id)
                )
                return True
            else:
                # Créer un nouveau quota si inexistant
                cur.execute(
                    """
                    INSERT INTO quotas_conges (personnel_id, jours_alloues, jours_restants)
                    VALUES (?, ?, ?)
                    """,
                    (personnel_id, nouveaux_jours_alloues, nouveaux_jours_alloues)
                )
                return True
    except Exception as e:
        st.error(f"Erreur modification quota congés: {e}")
        return False
    finally:
        if conn:
            conn.close()

# =========================
# Interface Streamlit
# =========================

def main():
    # Initialisation
    if not test_connection_background():
        st.error("❌ Impossible de se connecter à la base de données. Vérifiez la configuration.")
        return
    
    if not create_tables():
        st.error("❌ Erreur lors de l'initialisation des tables.")
        return
    
    # Authentification
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.user = None
        st.session_state.user_role = None
        st.session_state.user_id = None
    
    if not st.session_state.authenticated:
        show_login()
        return
    
    # Menu principal
    st.sidebar.title(f"👤 {st.session_state.user} ({st.session_state.user_role})")
    
    menu_options = [
        "🏠 Tableau de Bord",
        "⏰ Pointage du Jour", 
        "👥 Gestion du Personnel",
        "📋 Gestion des Absences",
        "📊 Historique des Pointages",
        "📈 Statistiques",
        "🌙 Tours de Rôle Nuit",
        "👥 Gestion des Utilisateurs"
    ]
    
    if st.session_state.user_role != "admin":
        menu_options.remove("👥 Gestion des Utilisateurs")
        menu_options.remove("👥 Gestion du Personnel")
        menu_options.remove("🌙 Tours de Rôle Nuit")
        menu_options.remove("📋 Gestion des Absences")  # Les non-admins ne peuvent pas gérer les absences
    
    choice = st.sidebar.selectbox("Navigation", menu_options)
    
    if choice == "🏠 Tableau de Bord":
        show_dashboard()
    elif choice == "⏰ Pointage du Jour":
        show_pointage_du_jour()
    elif choice == "👥 Gestion du Personnel":
        show_gestion_personnel()
    elif choice == "📋 Gestion des Absences":
        show_gestion_absences()  # CORRECTION ICI
    elif choice == "📊 Historique des Pointages":
        show_historique_pointages()
    elif choice == "📈 Statistiques":
        show_statistiques()
    elif choice == "🌙 Tours de Rôle Nuit" and st.session_state.user_role == "admin":
        show_tours_role_nuit()
    elif choice == "👥 Gestion des Utilisateurs" and st.session_state.user_role == "admin":
        show_gestion_utilisateurs()
    
    # Bouton de déconnexion
    if st.sidebar.button("🚪 Déconnexion"):
        st.session_state.authenticated = False
        st.session_state.user = None
        st.session_state.user_role = None
        st.session_state.user_id = None
        st.rerun()
        
def show_login():
    st.title("🔐 Connexion")
    with st.form("login_form"):
        username = st.text_input("Nom d'utilisateur")
        password = st.text_input("Mot de passe", type="password")
        submit = st.form_submit_button("Se connecter")
        
        if submit:
            user = authenticate_user(username, password)
            if user:
                st.session_state.authenticated = True
                st.session_state.user = user['username']  # username
                st.session_state.user_role = user['role']  # role
                st.session_state.user_id = user['id']  # id
                st.rerun()
            else:
                st.error("❌ Identifiants incorrects")

def show_dashboard():
    st.title("🏠 Tableau de Bord") 
    
    # Marquage automatique des absences
    if st.button("🔄 Vérifier les absences automatiques"):
        if marquer_absence_automatique():
            st.success("✅ Absences automatiques vérifiées")
        else:
            st.error("❌ Erreur lors de la vérification des absences")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    # Statistiques rapides
    personnel_df = get_personnel()
    pointages_du_jour = get_pointages_du_jour()
    absences_du_jour = get_absences_du_jour()
    conges_en_cours = get_conges_en_cours()
    non_pointes = get_personnel_non_pointe()
    
    with col1:
        st.metric("Total Personnel", len(personnel_df[personnel_df['actif'] == 1]))
    with col2:
        st.metric("Pointages Aujourd'hui", len(pointages_du_jour))
    with col3:
        st.metric("Absences Aujourd'hui", len(absences_du_jour))
    with col4:
        st.metric("Non Pointés", len(non_pointes))
    with col5:
        st.metric("En Congé", len(conges_en_cours))
    
    # Employés en congé aujourd'hui - AFFICHAGE DÉTAILLÉ
    st.subheader("🏖️ Employés en congé aujourd'hui")
    
    if not conges_en_cours.empty:
        # Afficher un résumé
        st.info(f"📊 {len(conges_en_cours)} employé(s) en congé aujourd'hui")
        
        # Afficher le tableau détaillé
        display_cols = ['nom', 'prenom', 'service', 'date_debut', 'date_fin', 'duree_jours', 'type_conge']
        display_df = conges_en_cours[display_cols].copy()
        
        # Renommer les colonnes pour un affichage plus clair
        display_df.columns = ['Nom', 'Prénom', 'Service', 'Début', 'Fin', 'Durée (jours)', 'Type']
        
        st.dataframe(display_df, use_container_width=True)
        
        # Ajouter un graphique pour visualiser les congés par service
        if len(conges_en_cours) > 1:
            st.subheader("📈 Répartition des congés par service")
            conges_par_service = conges_en_cours['service'].value_counts()
            fig = px.pie(
                values=conges_par_service.values,
                names=conges_par_service.index,
                title="Répartition des congés par service"
            )
            st.plotly_chart(fig)
    else:
        st.success("✅ Aucun employé en congé aujourd'hui")
    
    # Personnel non pointé
    st.subheader("❌ Personnel non pointé aujourd'hui")
    if not non_pointes.empty:
        st.dataframe(non_pointes[['nom', 'prenom', 'service', 'poste', 'heure_entree_prevue']], 
                    use_container_width=True)
        
        # Bouton pour pointer en masse
        if st.button("📝 Pointer tous comme absents", type="secondary"):
            for _, emp in non_pointes.iterrows():
                enregistrer_absence(emp['id'], date.today(), "Absence non pointée", False)
            st.success("✅ Tous les non-pointés marqués comme absents")
            time.sleep(1)
            st.rerun()
    else:
        st.success("✅ Tout le personnel a pointé aujourd'hui")
    
    # Derniers pointages
    st.subheader("📋 Derniers pointages aujourd'hui")
    if not pointages_du_jour.empty:
        st.dataframe(pointages_du_jour[['nom', 'prenom', 'service', 'heure_arrivee', 'statut_arrivee']], 
                    use_container_width=True)
    else:
        st.info("Aucun pointage enregistré aujourd'hui")

def show_pointage_du_jour():
    st.title("⏰ Pointage du Jour")
    
    # Ajouter une option pour afficher tous les personnels
    col_filtre1, col_filtre2, col_filtre3 = st.columns([2, 2, 1])
    with col_filtre1:
        recherche = st.text_input("🔍 Rechercher un employé", key="recherche_employe")
    with col_filtre2:
        services = ["Tous les services"] + get_services_disponibles()
        filtre_service = st.selectbox("Filtrer par service", services, key="filtre_service")
    with col_filtre3:
        afficher_tous = st.checkbox("Afficher tous", help="Afficher tous les employés sans filtre de groupe", key="afficher_tous")
    
    # Liste du personnel filtrée
    personnel_filtre = filtrer_personnel(recherche, filtre_service, None, afficher_tous)
    
    # Vérifier si le filtrage a retourné des résultats
    if not personnel_filtre:
        st.info("Aucun employé trouvé avec les critères de recherche")
        return
    
    # Afficher un message d'information si on affiche tous les employés
    if afficher_tous:
        st.info("👁️ Affichage de TOUS les employés (y compris les groupes de nuit non actifs)")
    
    # Utiliser un conteneur pour éviter les problèmes de rendu
    pointage_container = st.container()
    
    with pointage_container:
        for service, employes in personnel_filtre.items():
            if not employes:  # Vérifier si la liste d'employés n'est pas vide
                continue
                
            st.subheader(f"🏥 {service}")
            
            for emp in employes:
                # Vérifier que l'employé a un ID valide
                if 'id' not in emp:
                    continue
                    
                emp_id = emp['id']
                
                # Créer un expander pour chaque employé
                with st.expander(f"{emp['prenom']} {emp['nom']} - {emp['poste']}"):
                    pointage = get_pointage_employe_jour(emp_id, date.today())
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**Heure prévue:** {emp['heure_entree_prevue']} - {emp['heure_sortie_prevue']}")
                        
                        if pointage is not None and pointage.get('heure_arrivee'):
                            st.success(f"✅ Arrivée: {pointage['heure_arrivee']} ({pointage['statut_arrivee']})")
                            if pointage.get('retard_minutes', 0) > 0:
                                st.warning(f"⏰ Retard: {pointage['retard_minutes']} minutes")
                        else:
                            st.error("❌ Non pointé")
                    
                    with col2:
                        if pointage is not None and pointage.get('heure_depart'):
                            st.success(f"✅ Départ: {pointage['heure_depart']} ({pointage['statut_depart']})")
                            if pointage.get('depart_avance_minutes', 0) > 0:
                                st.warning(f"⏰ Départ anticipé: {pointage['depart_avance_minutes']} minutes")
                        else:
                            st.info("ℹ️ Départ non enregistré")
                    
                    # Formulaire de pointage simplifié
                    heure_actuelle = datetime.now().time()
                    st.write(f"**Heure actuelle:** {heure_actuelle.strftime('%H:%M:%S')}")
                    
                    col_btn1, col_btn2, col_btn3 = st.columns(3)
                    
                    with col_btn1:
                        if st.button("✅ Pointer l'arrivée", key=f"arr_{emp_id}"):
                            heure_reelle = datetime.now().time()
                            success, retard = enregistrer_pointage_arrivee(
                                emp_id, date.today(), heure_reelle, "", ""
                            )
                            if success:
                                st.success(f"✅ Arrivée enregistrée à {heure_reelle.strftime('%H:%M:%S')}")
                                time.sleep(0.5)
                                st.rerun()
                            else:
                                st.error("❌ Erreur lors de l'enregistrement")
                    
                    with col_btn2:
                        if st.button("🚪 Pointer le départ", key=f"dep_{emp_id}"):
                            heure_reelle = datetime.now().time()
                            success, avance = enregistrer_pointage_depart(
                                emp_id, date.today(), heure_reelle, "", ""
                            )
                            if success:
                                st.success(f"✅ Départ enregistré à {heure_reelle.strftime('%H:%M:%S')}")
                                time.sleep(0.5)
                                st.rerun()
                            else:
                                st.error("❌ Erreur lors de l'enregistrement")
                    
                    with col_btn3:
                        if st.button("❌ Marquer absent", key=f"abs_{emp_id}"):
                            success = enregistrer_absence(
                                emp_id, date.today(), "Absence non justifiée", False
                            )
                            if success:
                                st.success("✅ Absence enregistrée")
                                time.sleep(0.5)
                                st.rerun()
                            else:
                                st.error("❌ Erreur lors de l'enregistrement")
                    
                    # Afficher les informations supplémentaires pour le personnel de nuit
                    if emp['poste'] == 'Nuit':
                        st.info(f"🌙 Groupe de nuit: {emp.get('groupe_nuit', 'A')}")
                        
def show_gestion_absences():
    st.title("📋 Gestion des Absences")
    
    tab1, tab2, tab3 = st.tabs(["Absences à justifier", "Historique des absences", "Justifier une absence"])
    
    with tab1:
        st.subheader("📝 Absences non justifiées")
        
        # Récupérer les absences non justifiées
        conn = get_connection()
        if conn:
            try:
                absences_non_justifiees = pd.read_sql_query(
                    """
                    SELECT a.id, p.nom, p.prenom, p.service, a.date_absence, a.motif, a.created_at
                    FROM absences a
                    JOIN personnels p ON a.personnel_id = p.id
                    WHERE a.justifie = FALSE
                    ORDER BY a.date_absence DESC
                    """,
                    conn
                )
                
                if not absences_non_justifiees.empty:
                    for _, absence in absences_non_justifiees.iterrows():
                        with st.expander(f"{absence['prenom']} {absence['nom']} - {absence['date_absence']}"):
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write(f"**Service:** {absence['service']}")
                                st.write(f"**Date:** {absence['date_absence']}")
                                st.write(f"**Motif:** {absence['motif']}")
                            
                            with col2:
                                st.write(f"**Date de déclaration:** {absence['created_at']}")
                                if st.button("📤 Justifier cette absence", key=f"just_{absence['id']}"):
                                    st.session_state.absence_a_justifier = absence['id']
                                    st.rerun()
                else:
                    st.success("✅ Toutes les absences sont justifiées")
                    
            except Exception as e:
                st.error(f"Erreur lors de la récupération des absences: {e}")
            finally:
                conn.close()
    
    with tab2:
        st.subheader("📊 Historique des absences")
        
        col1, col2 = st.columns(2)
        with col1:
            date_debut = st.date_input("Date de début", value=date.today() - timedelta(days=30))
        with col2:
            date_fin = st.date_input("Date de fin", value=date.today())
        
        if st.button("🔍 Charger l'historique"):
            absences_df = get_absences_periode(date_debut, date_fin)
            
            if not absences_df.empty:
                for _, absence in absences_df.iterrows():
                    with st.expander(f"{absence['prenom']} {absence['nom']} - {absence['date_absence']}"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Service:** {absence['service']}")
                            st.write(f"**Date:** {absence['date_absence']}")
                            st.write(f"**Motif:** {absence['motif']}")
                            st.write(f"**Justifiée:** {'✅ Oui' if absence['justifie'] else '❌ Non'}")
                        
                        with col2:
                            st.write(f"**Poste:** {absence['poste']}")
                            st.write(f"**Heure prévue:** {absence['heure_entree_prevue']}")
                            if absence['has_certificat']:
                                if st.button("📄 Voir le justificatif", key=f"view_{absence['id']}"):
                                    afficher_justificatif_absence(absence['id'])
            else:
                st.info("Aucune absence dans la période sélectionnée")
    
    with tab3:
        st.subheader("📎 Justifier une absence")
        
        # Sélectionner l'absence à justifier
        absences_a_justifier = []
        conn = get_connection()
        if conn:
            try:
                absences_non_justifiees = pd.read_sql_query(
                    "SELECT a.id, p.nom, p.prenom, a.date_absence FROM absences a JOIN personnels p ON a.personnel_id = p.id WHERE a.justifie = FALSE",
                    conn
                )
                absences_a_justifier = absences_non_justifiees.apply(
                    lambda x: f"{x['prenom']} {x['nom']} - {x['date_absence']} (ID: {x['id']})", axis=1
                ).tolist()
            except:
                pass
            finally:
                conn.close()
        
        if absences_a_justifier:
            absence_selectionnee = st.selectbox("Sélectionner l'absence à justifier", absences_a_justifier)
            
            if absence_selectionnee:
                # Extraire l'ID de l'absence
                absence_id = int(absence_selectionnee.split('(ID: ')[1].replace(')', ''))
                
                with st.form(f"form_justifier_{absence_id}"):
                    st.write("### 📋 Formulaire de justification")
                    
                    motif_justification = st.text_area("Motif détaillé de l'absence", 
                                                     placeholder="Décrivez en détail la raison de l'absence...")
                    
                    certificat_file = st.file_uploader("📎 Certificat justificatif (JPEG, PNG, PDF)", 
                                                     type=['jpg', 'jpeg', 'png', 'pdf'],
                                                     help="Téléchargez un certificat médical ou un justificatif")
                    
                    if st.form_submit_button("✅ Enregistrer la justification"):
                        if certificat_file:
                            if justifier_absence(absence_id, certificat_file, motif_justification):
                                st.success("✅ Absence justifiée avec succès")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("❌ Erreur lors de l'enregistrement")
                        else:
                            st.warning("⚠️ Veuillez télécharger un justificatif")
        else:
            st.success("✅ Toutes les absences sont déjà justifiées")

def show_gestion_personnel():
    st.title("👥 Gestion du Personnel")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Liste du Personnel", "Ajouter un Employé", "Modifier un Employé", "Gestion des Congés", "Supprimer un Employé"])
    
    with tab1:
        st.subheader("📋 Liste du Personnel")
        personnel_df = get_personnel()
        if not personnel_df.empty:
            # Filtrer seulement les employés actifs
            personnel_actif = personnel_df[personnel_df['actif'] == 1]
            if not personnel_actif.empty:
                st.dataframe(
                    personnel_actif[['nom', 'prenom', 'service', 'poste', 'heure_entree_prevue', 'heure_sortie_prevue', 'groupe_nuit']],
                    use_container_width=True
                )
                
                # Option d'export
                csv_data = personnel_actif.to_csv(index=False)
                st.download_button(
                    "📥 Exporter en CSV",
                    csv_data,
                    "personnel.csv",
                    "text/csv"
                )
            else:
                st.info("Aucun employé actif")
        else:
            st.info("Aucun employé enregistré")
    
    with tab2:
        st.subheader("➕ Ajouter un Employé")
        with st.form("ajouter_personnel"):
            col1, col2 = st.columns(2)
            with col1:
                nom = st.text_input("Nom*", placeholder="Dupont")
                prenom = st.text_input("Prénom*", placeholder="Jean")
                service = st.text_input("Service*", placeholder="Réception")
            with col2:
                poste = st.selectbox("Poste*", ["Jour", "Nuit", "Mixte"])
                heure_entree = st.time_input("Heure d'entrée prévue*", value=tm(8, 0))
                heure_sortie = st.time_input("Heure de sortie prévue*", value=tm(16, 0))
            
            # Options spécifiques pour le personnel de nuit/mixte
            jours_travail = ""
            groupe_nuit = "A"
            
            if poste in ["Nuit", "Mixte"]:
                groupe_nuit = st.selectbox("Groupe de nuit", ["A", "B"])
            
            if poste == "Mixte":
                jours_options = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
                jours_selection = st.multiselect(
                    "Jours de travail de nuit",
                    options=jours_options,
                    help="Sélectionnez les jours où l'employé travaille de nuit"
                )
                jours_travail = ','.join(jours_selection)
            
            if st.form_submit_button("➕ Ajouter l'employé"):
                if nom and prenom and service:
                    if ajouter_personnel(nom, prenom, service, poste, heure_entree, heure_sortie, groupe_nuit, jours_travail):
                        st.success("✅ Employé ajouté avec succès")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Erreur lors de l'ajout de l'employé")
                else:
                    st.warning("⚠️ Veuillez remplir tous les champs obligatoires")
    
    with tab3:
        st.subheader("✏️ Modifier un Employé")
        personnel_actif = get_personnel()
        personnel_actif = personnel_actif[personnel_actif['actif'] == 1]
        
        if not personnel_actif.empty:
            employe_selection = st.selectbox(
                "Sélectionner un employé à modifier",
                personnel_actif.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']} (ID: {x['id']})", axis=1),
                key="modifier_employe"
            )
            
            if employe_selection:
                try:
                    # Extraire l'ID de l'employé
                    personnel_id = int(employe_selection.split('(ID: ')[1].replace(')', ''))
                    selected_index = personnel_actif[personnel_actif['id'] == personnel_id].index[0]
                    emp_data = personnel_actif.loc[selected_index]
                    
                    # Afficher les informations de base
                    st.write(f"**Employé:** {emp_data['prenom']} {emp_data['nom']}")
                    st.write(f"**Service:** {emp_data['service']}")
                    
                    with st.form("modifier_personnel_form"):
                        col1, col2 = st.columns(2)
                        with col1:
                            nom = st.text_input("Nom", value=emp_data['nom'])
                            prenom = st.text_input("Prénom", value=emp_data['prenom'])
                            service = st.text_input("Service", value=emp_data['service'])
                        with col2:
                            poste = st.selectbox("Poste", ["Jour", "Nuit", "Mixte"], 
                                               index=0 if emp_data['poste'] == "Jour" else 1 if emp_data['poste'] == "Nuit" else 2)
                            heure_entree = st.time_input("Heure d'entrée prévue", value=_as_time(emp_data['heure_entree_prevue']))
                            heure_sortie = st.time_input("Heure de sortie prévue", value=_as_time(emp_data['heure_sortie_prevue']))
                        
                        # Gestion des groupes et jours de travail
                        groupe_nuit = emp_data.get('groupe_nuit', 'A')
                        if poste in ["Nuit", "Mixte"]:
                            groupe_nuit = st.selectbox("Groupe de nuit", ["A", "B"], 
                                                     index=0 if groupe_nuit == "A" else 1)
                        
                        jours_travail = emp_data.get('jours_travail', '')
                        if poste == "Mixte":
                            jours_options = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
                            jours_actuels = jours_travail.split(',') if jours_travail else []
                            jours_selection = st.multiselect(
                                "Jours de travail de nuit",
                                options=jours_options,
                                default=[j for j in jours_actuels if j in jours_options]
                            )
                            jours_travail = ','.join(jours_selection)
                        
                        actif = st.checkbox("Actif", value=bool(emp_data['actif']))
                        
                        if st.form_submit_button("💾 Enregistrer les modifications"):
                            if modifier_personnel(personnel_id, nom, prenom, service, poste, heure_entree, heure_sortie, groupe_nuit, actif, jours_travail):
                                st.success("✅ Employé modifié avec succès")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("❌ Erreur lors de la modification")
                
                except (IndexError, ValueError) as e:
                    st.error("❌ Erreur lors de la sélection de l'employé")
                    print(f"DEBUG: Erreur extraction ID - {e}")
        else:
            st.info("Aucun employé actif à modifier")
    
    with tab4:
        st.subheader("📅 Gestion des Congés du Personnel")
        
        personnel_actif = get_personnel()
        personnel_actif = personnel_actif[personnel_actif['actif'] == 1]
        
        if not personnel_actif.empty:
            employe_selection = st.selectbox(
                "Sélectionner un employé",
                personnel_actif.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']} (ID: {x['id']})", axis=1),
                key="conges_employe"
            )
            
            if employe_selection:
                try:
                    personnel_id = int(employe_selection.split('(ID: ')[1].replace(')', ''))
                    selected_index = personnel_actif[personnel_actif['id'] == personnel_id].index[0]
                    emp_data = personnel_actif.loc[selected_index]
                    
                    st.write(f"**Employé:** {emp_data['prenom']} {emp_data['nom']}")
                    st.write(f"**Service:** {emp_data['service']}")
                    
                    # Afficher les quotas de congés
                    quota = get_quota_conges(personnel_id)
                    if quota:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Jours alloués", quota['jours_alloues'])
                        with col2:
                            st.metric("Jours pris", quota['jours_pris'])
                        with col3:
                            st.metric("Jours restants", quota['jours_restants'])
                    
                    # Formulaire pour déclarer un nouveau congé
                    st.subheader("➕ Nouveau congé")
                    with st.form("nouveau_conge_form"):
                        col1, col2 = st.columns(2)
                        with col1:
                            date_debut = st.date_input("Date de début", value=date.today())
                            type_conge = st.selectbox("Type de congé", ["Congé annuel", "Maladie", "Familial", "Exceptionnel", "Maternité", "Paternité"])
                        with col2:
                            date_fin = st.date_input("Date de fin", value=date.today() + timedelta(days=7))
                            motif_conge = st.text_area("Motif du congé", placeholder="Raison du congé...")
                        
                        statut_conge = st.selectbox("Statut", ["En attente", "Approuvé", "Rejeté"])
                        
                        if st.form_submit_button("📤 Déclarer le congé"):
                            if date_debut and date_fin and type_conge and motif_conge:
                                if date_debut <= date_fin:
                                    success, message = demander_conge(personnel_id, date_debut, date_fin, type_conge, motif_conge)
                                    
                                    if success:
                                        st.success(f"✅ {message}")
                                        
                                        # Si le congé est approuvé directement
                                        if statut_conge == "Approuvé":
                                            # Récupérer l'ID du congé créé
                                            conn = get_connection()
                                            if conn:
                                                try:
                                                    with conn:
                                                        cur = conn.cursor()
                                                        cur.execute(
                                                            "SELECT id FROM conges WHERE personnel_id = ? ORDER BY created_at DESC LIMIT 1",
                                                            (personnel_id,)
                                                        )
                                                        conge_result = cur.fetchone()
                                                        if conge_result:
                                                            if approuver_conge(conge_result['id']):
                                                                st.success("✅ Congé approuvé automatiquement")
                                                            else:
                                                                st.warning("⚠️ Congé créé mais erreur lors de l'approbation")
                                                except Exception as e:
                                                    st.error(f"Erreur approbation congé: {e}")
                                                finally:
                                                    if conn:
                                                        conn.close()
                                        elif statut_conge == "Rejeté":
                                            # Rejeter le congé
                                            conn = get_connection()
                                            if conn:
                                                try:
                                                    with conn:
                                                        cur = conn.cursor()
                                                        cur.execute(
                                                            "SELECT id FROM conges WHERE personnel_id = ? ORDER BY created_at DESC LIMIT 1",
                                                            (personnel_id,)
                                                        )
                                                        conge_result = cur.fetchone()
                                                        if conge_result:
                                                            if rejeter_conge(conge_result['id']):
                                                                st.success("✅ Congé rejeté")
                                                except Exception as e:
                                                    st.error(f"Erreur rejet congé: {e}")
                                                finally:
                                                    if conn:
                                                        conn.close()
                                    else:
                                        st.error(f"❌ {message}")
                                else:
                                    st.error("❌ La date de fin doit être après la date de début")
                            else:
                                st.warning("⚠️ Veuillez remplir tous les champs")
                    
                    # Afficher l'historique des congés de l'employé
                    st.subheader("📋 Historique des congés")
                    conges_employe = get_conges_employe(personnel_id)
                    
                    if not conges_employe.empty:
                        for _, conge in conges_employe.iterrows():
                            with st.expander(f"{conge['date_debut']} au {conge['date_fin']} - {conge['statut']}"):
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.write(f"**Type:** {conge['type_conge']}")
                                    st.write(f"**Durée:** {conge['duree_jours']} jours")
                                    st.write(f"**Statut:** {conge['statut']}")
                                with col2:
                                    st.write(f"**Motif:** {conge['motif']}")
                                    st.write(f"**Date demande:** {conge['created_at']}")
                                
                                # Options pour les administrateurs
                                if st.session_state.user_role == "admin" and conge['statut'] == "En attente":
                                    col_btn1, col_btn2 = st.columns(2)
                                    with col_btn1:
                                        if st.button("✅ Approuver", key=f"app_{conge['id']}"):
                                            if approuver_conge(conge['id']):
                                                st.success("✅ Congé approuvé")
                                                time.sleep(1)
                                                st.rerun()
                                    with col_btn2:
                                        if st.button("❌ Rejeter", key=f"rej_{conge['id']}"):
                                            if rejeter_conge(conge['id']):
                                                st.success("✅ Congé rejeté")
                                                time.sleep(1)
                                                st.rerun()
                    else:
                        st.info("Aucun congé enregistré pour cet employé")
                
                except (IndexError, ValueError) as e:
                    st.error("❌ Erreur lors de la sélection de l'employé")
        else:
            st.info("Aucun employé actif")
    
    with tab5:
        st.subheader("🗑️ Supprimer un Employé")
        personnel_actif = get_personnel()
        personnel_actif = personnel_actif[personnel_actif['actif'] == 1]
        
        if not personnel_actif.empty:
            employe_selection = st.selectbox(
                "Sélectionner un employé à supprimer",
                personnel_actif.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']} (ID: {x['id']})", axis=1),
                key="supprimer_employe"
            )
            
            if employe_selection:
                try:
                    personnel_id = int(employe_selection.split('(ID: ')[1].replace(')', ''))
                    selected_index = personnel_actif[personnel_actif['id'] == personnel_id].index[0]
                    emp_data = personnel_actif.loc[selected_index]
                    
                    st.write(f"**Employé sélectionné:** {emp_data['prenom']} {emp_data['nom']}")
                    st.write(f"**Service:** {emp_data['service']}")
                    st.write(f"**Poste:** {emp_data['poste']}")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if st.button("🚫 Désactiver l'employé", type="secondary",
                                   help="L'employé sera désactivé mais conservé dans l'historique"):
                            if supprimer_personnel(personnel_id):
                                st.success("✅ Employé désactivé avec succès")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("❌ Erreur lors de la désactivation")
                    
                    with col2:
                        if st.button("🗑️ Supprimer définitivement", type="primary",
                                   help="ATTENTION: Suppression complète de toutes les données"):
                            st.warning("⚠️ **ACTION IRRÉVERSIBLE** ⚠️")
                            st.warning("Cette action supprimera TOUTES les données de l'employé :")
                            st.warning("- Pointages, Absences, Congés, Retards, Quotas")
                            
                            confirmation = st.text_input("Tapez 'SUPPRIMER' pour confirmer")
                            
                            if st.button("✅ Confirmer la suppression", 
                                       disabled=confirmation.upper() != "SUPPRIMER",
                                       type="primary"):
                                if supprimer_definitivement_personnel(personnel_id):
                                    st.success("✅ Employé supprimé définitivement")
                                    time.sleep(2)
                                    st.rerun()
                                else:
                                    st.error("❌ Erreur lors de la suppression")
                
                except (IndexError, ValueError) as e:
                    st.error("❌ Erreur lors de la sélection de l'employé")
        else:
            st.info("Aucun employé actif à supprimer")
            
def show_historique_pointages():
    st.title("📊 Historique des Pointages")
    
    # Initialisation des états
    if 'hist_data_loaded' not in st.session_state:
        st.session_state.hist_data_loaded = False
        st.session_state.hist_date_debut = date.today() - timedelta(days=7)
        st.session_state.hist_date_fin = date.today()
        st.session_state.pointages_df = pd.DataFrame()
        st.session_state.retards_df = pd.DataFrame()
        st.session_state.absences_df = pd.DataFrame()
    
    col1, col2 = st.columns(2)
    with col1:
        date_debut = st.date_input("Date de début", value=st.session_state.hist_date_debut)
    with col2:
        date_fin = st.date_input("Date de fin", value=st.session_state.hist_date_fin)
    
    # Bouton de chargement
    if st.button("🔍 Charger l'historique"):
        with st.spinner("Chargement des données..."):
            st.session_state.pointages_df = get_pointages_periode(date_debut, date_fin)
            st.session_state.retards_df = get_retards_periode(date_debut, date_fin)
            st.session_state.absences_df = get_absences_periode(date_debut, date_fin)
            st.session_state.hist_data_loaded = True
            st.session_state.hist_date_debut = date_debut
            st.session_state.hist_date_fin = date_fin
    
    # Affichage des données si chargées
    if st.session_state.hist_data_loaded:
        display_historique_data()
    else:
        st.info("👆 Cliquez sur 'Charger l'historique' pour afficher les données")

def display_historique_data():
    """Affiche les données historiques une fois chargées"""
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Pointages", "Modifier Pointage", "Recherche Avancée", "Retards", "Absences"])
    
    with tab1:
        display_pointages_tab()
    
    with tab2:
        display_modification_tab()
    
    with tab3:
        display_recherche_tab()
    
    with tab4:
        display_retards_tab()
    
    with tab5:
        display_absences_tab()

def display_pointages_tab():
    """Affiche l'onglet des pointages"""
    st.subheader("📋 Liste des pointages")
    
    if not st.session_state.pointages_df.empty:
        # Afficher avec plus de détails
        display_df = st.session_state.pointages_df[[
            'nom', 'prenom', 'service', 'date_pointage', 
            'heure_arrivee', 'heure_depart', 'statut_arrivee', 
            'statut_depart', 'retard_minutes', 'motif_retard'
        ]].copy()
        
        # Renommer les colonnes pour un affichage plus clair
        display_df.columns = ['Nom', 'Prénom', 'Service', 'Date', 'Heure Arrivée', 
                             'Heure Départ', 'Statut Arrivée', 'Statut Départ', 
                             'Retard (min)', 'Motif']
        
        st.dataframe(display_df, use_container_width=True, height=400)
        
        # Option d'export (en dehors de tout formulaire)
        csv_data = display_df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            "📥 Exporter en CSV",
            csv_data,
            f"pointages_{st.session_state.hist_date_debut}_{st.session_state.hist_date_fin}.csv",
            "text/csv",
            key="export_pointages"
        )
        
        # Statistiques rapides
        st.subheader("📈 Statistiques")
        col1, col2, col3 = st.columns(3)
        with col1:
            total_pointages = len(st.session_state.pointages_df)
            st.metric("Total pointages", total_pointages)
        with col2:
            retards_count = len(st.session_state.pointages_df[st.session_state.pointages_df['retard_minutes'] > 0])
            st.metric("Retards", retards_count)
        with col3:
            absences_count = len(st.session_state.pointages_df[st.session_state.pointages_df['statut_arrivee'] == 'Absent'])
            st.metric("Absences", absences_count)
            
    else:
        st.info("Aucun pointage dans la période sélectionnée")

def display_retards_tab():
    """Affiche l'onglet des retards"""
    st.subheader("⏰ Retards")
    
    if not st.session_state.retards_df.empty:
        st.dataframe(st.session_state.retards_df, use_container_width=True, height=400)
        
        # Statistiques des retards
        st.subheader("📈 Statistiques des retards")
        col1, col2, col3 = st.columns(3)
        with col1:
            total_retards = len(st.session_state.retards_df)
            st.metric("Total retards", total_retards)
        with col2:
            total_minutes = st.session_state.retards_df['retard_minutes'].sum()
            st.metric("Total minutes", f"{total_minutes} min")
        with col3:
            moyenne_retard = st.session_state.retards_df['retard_minutes'].mean()
            st.metric("Moyenne retard", f"{moyenne_retard:.1f} min")
        
        # Bouton d'export (en dehors de tout formulaire)
        csv_data = st.session_state.retards_df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            "📥 Exporter les retards",
            csv_data,
            "retards.csv",
            "text/csv",
            key="export_retards"
        )
            
    else:
        st.info("Aucun retard dans la période sélectionnée")

def display_absences_tab():
    """Affiche l'onglet des absences"""
    st.subheader("📋 Absences")
    
    if not st.session_state.absences_df.empty:
        # Afficher les absences avec possibilité de voir les justificatifs
        for index, absence in st.session_state.absences_df.iterrows():
            if 'id' in absence and pd.notna(absence['id']):
                absence_id = int(absence['id'])
                with st.expander(f"{absence['prenom']} {absence['nom']} - {absence['date_absence']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Service:** {absence['service']}")
                        st.write(f"**Date:** {absence['date_absence']}")
                        st.write(f"**Poste:** {absence['poste']}")
                        st.write(f"**Heure prévue:** {absence['heure_entree_prevue']}")
                    with col2:
                        st.write(f"**Motif:** {absence['motif']}")
                        st.write(f"**Justifiée:** {'✅ Oui' if absence['justifie'] else '❌ Non'}")
                        st.write(f"**Certificat:** {'📎 Disponible' if absence['has_certificat'] else '❌ Aucun'}")
                        
                        # Bouton pour voir le justificatif
                        if absence['has_certificat']:
                            if st.button("👁️ Voir le justificatif", key=f"cert_{absence_id}"):
                                afficher_justificatif_absence(absence_id)
        
        # Option d'export (en dehors de tout formulaire)
        st.download_button(
            "📥 Exporter les absences en CSV",
            st.session_state.absences_df.to_csv(index=False, encoding='utf-8-sig'),
            "absences.csv",
            "text/csv",
            key="export_absences"
        )
        
        # Statistiques des absences
        st.subheader("📊 Statistiques des absences")
        col1, col2 = st.columns(2)
        with col1:
            total_absences = len(st.session_state.absences_df)
            st.metric("Total absences", total_absences)
        with col2:
            absences_justifiees = len(st.session_state.absences_df[st.session_state.absences_df['justifie'] == True])
            st.metric("Absences justifiées", absences_justifiees)
            
    else:
        st.info("Aucune absence dans la période sélectionnée")

def display_modification_tab():
    """Affiche l'onglet de modification des pointages - VERSION CORRIGÉE"""
    st.subheader("✏️ Modification des pointages")
    
    if st.session_state.pointages_df.empty:
        st.info("Aucun pointage à modifier")
        return
    
    # Sélection du pointage
    pointages_list = []
    for _, row in st.session_state.pointages_df.iterrows():
        pointage_id = row.get('id', 'N/A')
        heure_arrivee = row.get('heure_arrivee', 'N/A')
        statut = row.get('statut_arrivee', 'N/A')
        pointages_list.append(f"{row['prenom']} {row['nom']} - {row['date_pointage']} - Arr: {heure_arrivee} - {statut} - ID: {pointage_id}")
    
    selected_pointage = st.selectbox("Sélectionner un pointage à modifier", pointages_list, key="pointage_select")
    
    if selected_pointage:
        try:
            pointage_id = int(selected_pointage.split('ID: ')[1])
            selected_data = st.session_state.pointages_df[st.session_state.pointages_df['id'] == pointage_id].iloc[0]
            
            # Afficher les informations de l'employé
            st.write(f"**Employé:** {selected_data['prenom']} {selected_data['nom']}")
            st.write(f"**Date:** {selected_data['date_pointage']}")
            st.write(f"**Service:** {selected_data['service']}")
            
            # Formulaire de modification
            with st.form(f"modify_form_{pointage_id}"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("🕒 Arrivée")
                    
                    # Champ texte pour l'heure manuelle
                    heure_arrivee_actuelle = selected_data.get('heure_arrivee', '')
                    heure_arrivee_texte = st.text_input(
                        "Heure d'arrivée (HH:MM:SS)", 
                        value=heure_arrivee_actuelle,
                        placeholder="08:30:00",
                        key=f"arr_text_{pointage_id}"
                    )
                    
                    # Bouton pour utiliser l'heure actuelle - CORRECTION ICI
                    if st.form_submit_button("⌚ Utiliser heure actuelle (Arrivée)"):
                        heure_actuelle = datetime.now().strftime("%H:%M:%S")
                        st.session_state[f"arr_text_{pointage_id}"] = heure_actuelle
                        st.rerun()
                    
                    statut_actuel = selected_data.get('statut_arrivee', 'Non pointé')
                    nouveau_statut = st.selectbox(
                        "Statut arrivée",
                        ["Présent à l'heure", "En retard", "Absent", "Non pointé"],
                        index=0 if statut_actuel == "Présent à l'heure" else 
                              1 if statut_actuel == "En retard" else 
                              2 if statut_actuel == "Absent" else 3,
                        key=f"stat_{pointage_id}"
                    )
                
                with col2:
                    st.subheader("🚪 Départ")
                    
                    # Champ texte pour l'heure manuelle
                    heure_depart_actuelle = selected_data.get('heure_depart', '')
                    heure_depart_texte = st.text_input(
                        "Heure de départ (HH:MM:SS)", 
                        value=heure_depart_actuelle,
                        placeholder="16:30:00",
                        key=f"dep_text_{pointage_id}"
                    )
                    
                    # Bouton pour utiliser l'heure actuelle - CORRECTION ICI
                    if st.form_submit_button("⌚ Utiliser heure actuelle (Départ)"):
                        heure_actuelle = datetime.now().strftime("%H:%M:%S")
                        st.session_state[f"dep_text_{pointage_id}"] = heure_actuelle
                        st.rerun()
                    
                    depart_actuel = selected_data.get('statut_depart', 'Present')
                    nouveau_depart_statut = st.selectbox(
                        "Statut départ",
                        ["Present", "Départ anticipé"],
                        index=0 if depart_actuel == "Present" else 1,
                        key=f"dep_stat_{pointage_id}"
                    )
                
                new_motif = st.text_area(
                    "Motif (retard/absence)", 
                    value=selected_data.get('motif_retard', ''), 
                    placeholder="Ex: Problème de transport, raison familiale...",
                    key=f"mot_{pointage_id}"
                )
                
                # Afficher les informations de référence
                st.info(f"**Heure prévue d'entrée:** {selected_data['heure_entree_prevue']}")
                st.info(f"**Heure prévue de sortie:** {selected_data['heure_sortie_prevue']}")
                
                # Validation du format de l'heure
                heure_valide = True
                if heure_arrivee_texte:
                    try:
                        datetime.strptime(heure_arrivee_texte, '%H:%M:%S')
                    except ValueError:
                        st.error("❌ Format d'heure d'arrivée invalide. Utilisez HH:MM:SS")
                        heure_valide = False
                
                if heure_depart_texte:
                    try:
                        datetime.strptime(heure_depart_texte, '%H:%M:%S')
                    except ValueError:
                        st.error("❌ Format d'heure de départ invalide. Utilisez HH:MM:SS")
                        heure_valide = False
                
                # Bouton d'enregistrement principal
                submitted = st.form_submit_button("💾 Enregistrer les modifications", disabled=not heure_valide)
                
                if submitted:
                    # Convertir les heures texte en objets time
                    nouvelle_heure_arrivee = None
                    nouvelle_heure_depart = None
                    
                    if heure_arrivee_texte:
                        try:
                            nouvelle_heure_arrivee = datetime.strptime(heure_arrivee_texte, '%H:%M:%S').time()
                        except ValueError:
                            st.error("❌ Format d'heure d'arrivée invalide")
                            return
                    
                    if heure_depart_texte:
                        try:
                            nouvelle_heure_depart = datetime.strptime(heure_depart_texte, '%H:%M:%S').time()
                        except ValueError:
                            st.error("❌ Format d'heure de départ invalide")
                            return
                    
                    success, message = modifier_historique_pointage(
                        pointage_id, nouvelle_heure_arrivee, nouvelle_heure_depart, nouveau_statut, new_motif
                    )
                    
                    if success:
                        st.success("✅ " + message)
                        # Recharger les données
                        st.session_state.pointages_df = get_pointages_periode(
                            st.session_state.hist_date_debut, st.session_state.hist_date_fin
                        )
                        st.rerun()
                    else:
                        st.error("❌ " + message)
            
            # Section de suppression (en dehors du formulaire)
            st.markdown("---")
            st.subheader("🗑️ Suppression du pointage")
            
            if st.button("🗑️ Supprimer ce pointage", key=f"del_{pointage_id}", type="secondary"):
                st.session_state.show_delete_confirm = pointage_id
            
            if st.session_state.get('show_delete_confirm') == pointage_id:
                st.warning("⚠️ Cette action est irréversible!")
                confirm_text = st.text_input("Tapez 'SUPPRIMER' pour confirmer", key=f"confirm_text_{pointage_id}")
                
                col_confirm1, col_confirm2 = st.columns(2)
                with col_confirm1:
                    if st.button("✅ Confirmer la suppression", key=f"confirm_del_{pointage_id}", 
                               disabled=confirm_text.upper() != "SUPPRIMER"):
                        conn = get_connection()
                        if conn:
                            try:
                                with conn:
                                    cur = conn.cursor()
                                    cur.execute("DELETE FROM pointages WHERE id = ?", (pointage_id,))
                                    cur.execute("DELETE FROM retards WHERE personnel_id = ? AND date_retard = ?", 
                                               (selected_data['personnel_id'], selected_data['date_pointage']))
                                st.success("✅ Pointage supprimé avec succès")
                                # Recharger les données
                                st.session_state.pointages_df = get_pointages_periode(
                                    st.session_state.hist_date_debut, st.session_state.hist_date_fin
                                )
                                st.session_state.show_delete_confirm = None
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Erreur lors de la suppression: {e}")
                            finally:
                                conn.close()
                
                with col_confirm2:
                    if st.button("❌ Annuler", key=f"cancel_del_{pointage_id}"):
                        st.session_state.show_delete_confirm = None
                        st.rerun()
                        
        except Exception as e:
            st.error(f"Erreur lors de la sélection: {str(e)}")

def display_recherche_tab():
    """Affiche l'onglet de recherche avancée"""
    st.subheader("🔍 Recherche avancée")
    
    # Initialiser les résultats de recherche
    if 'recherche_resultats' not in st.session_state:
        st.session_state.recherche_resultats = pd.DataFrame()
    
    # Formulaire de recherche
    with st.form("recherche_avancee_form"):
        col1, col2 = st.columns(2)
        with col1:
            nom_recherche = st.text_input("Nom", key="rech_nom")
            prenom_recherche = st.text_input("Prénom", key="rech_prenom")
            service_recherche = st.selectbox("Service", ["Tous"] + get_services_disponibles(), key="rech_service")
        with col2:
            date_debut_recherche = st.date_input("Date début", value=st.session_state.hist_date_debut, key="rech_debut")
            date_fin_recherche = st.date_input("Date fin", value=st.session_state.hist_date_fin, key="rech_fin")
            statut_recherche = st.selectbox("Statut", ["Tous", "Présent à l'heure", "En retard", "Absent"], key="rech_statut")
        
        # Utiliser st.form_submit_button() au lieu de st.button()
        if st.form_submit_button("🔍 Rechercher"):
            resultats = rechercher_pointages_avances(
                nom_recherche if nom_recherche else None,
                prenom_recherche if prenom_recherche else None,
                service_recherche if service_recherche != "Tous" else None,
                date_debut_recherche,
                date_fin_recherche,
                statut_recherche if statut_recherche != "Tous" else None
            )
            st.session_state.recherche_resultats = resultats
    
    # Afficher les résultats (en dehors du formulaire)
    if not st.session_state.recherche_resultats.empty:
        st.dataframe(st.session_state.recherche_resultats, use_container_width=True, height=400)
        
        # Bouton d'export (en dehors du formulaire)
        csv_data = st.session_state.recherche_resultats.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            "📥 Exporter les résultats",
            csv_data,
            "recherche_pointages.csv",
            "text/csv",
            key="export_recherche"
        )
    elif st.session_state.get('recherche_effectuee', False):
        st.info("Aucun résultat trouvé")

def display_retards_tab():
    """Affiche l'onglet des retards"""
    st.subheader("⏰ Retards")
    
    if not st.session_state.retards_df.empty:
        st.dataframe(st.session_state.retards_df, use_container_width=True, height=400)
        
        # Statistiques des retards
        st.subheader("📈 Statistiques des retards")
        col1, col2, col3 = st.columns(3)
        with col1:
            total_retards = len(st.session_state.retards_df)
            st.metric("Total retards", total_retards)
        with col2:
            total_minutes = st.session_state.retards_df['retard_minutes'].sum()
            st.metric("Total minutes", f"{total_minutes} min")
        with col3:
            moyenne_retard = st.session_state.retards_df['retard_minutes'].mean()
            st.metric("Moyenne retard", f"{moyenne_retard:.1f} min")
            
        # Graphique des retards par service
        retards_par_service = st.session_state.retards_df.groupby('service')['retard_minutes'].sum().reset_index()
        if not retards_par_service.empty:
            fig = px.bar(
                retards_par_service,
                x='service',
                y='retard_minutes',
                title="Retards par service (minutes)",
                labels={'service': 'Service', 'retard_minutes': 'Minutes de retard'}
            )
            st.plotly_chart(fig)
            
    else:
        st.info("Aucun retard dans la période sélectionnée")

def display_absences_tab():
    """Affiche l'onglet des absences"""
    st.subheader("📋 Absences")
    
    if not st.session_state.absences_df.empty:
        # Afficher les absences avec possibilité de voir les justificatifs
        for index, absence in st.session_state.absences_df.iterrows():
            if 'id' in absence and pd.notna(absence['id']):
                absence_id = int(absence['id'])
                with st.expander(f"{absence['prenom']} {absence['nom']} - {absence['date_absence']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Service:** {absence['service']}")
                        st.write(f"**Date:** {absence['date_absence']}")
                        st.write(f"**Poste:** {absence['poste']}")
                        st.write(f"**Heure prévue:** {absence['heure_entree_prevue']}")
                    with col2:
                        st.write(f"**Motif:** {absence['motif']}")
                        st.write(f"**Justifiée:** {'✅ Oui' if absence['justifie'] else '❌ Non'}")
                        st.write(f"**Certificat:** {'📎 Disponible' if absence['has_certificat'] else '❌ Aucun'}")
                        
                        # Bouton pour voir le justificatif
                        if absence['has_certificat']:
                            if st.button("👁️ Voir le justificatif", key=f"cert_{absence_id}"):
                                afficher_justificatif_absence(absence_id)
        
        # Option d'export
        st.download_button(
            "📥 Exporter les absences en CSV",
            st.session_state.absences_df.to_csv(index=False, encoding='utf-8-sig'),
            "absences.csv",
            "text/csv",
            key="export_absences"
        )
        
        # Statistiques des absences
        st.subheader("📊 Statistiques des absences")
        col1, col2 = st.columns(2)
        with col1:
            total_absences = len(st.session_state.absences_df)
            st.metric("Total absences", total_absences)
        with col2:
            absences_justifiees = len(st.session_state.absences_df[st.session_state.absences_df['justifie'] == True])
            st.metric("Absences justifiées", absences_justifiees)
            
    else:
        st.info("Aucune absence dans la période sélectionnée")
def show_statistiques():
    st.title("📈 Statistiques")
    
    stats_df = get_stats_mensuelles()
    
    if not stats_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_retard = stats_df['total_retard_minutes'].sum()
            st.metric("Total retard (min)", total_retard)
        
        with col2:
            total_depart_avance = stats_df['total_depart_avance_minutes'].sum()
            st.metric("Total départ anticipé (min)", total_depart_avance)
        
        with col3:
            moy_retard = stats_df['jours_retard'].mean()
            st.metric("Moyenne retards/jour", f"{moy_retard:.1f}")
        
        # Graphique des retards par service
        fig = px.bar(
            stats_df.groupby('service')['jours_retard'].sum().reset_index(),
            x='service',
            y='jours_retard',
            title="Nombre de retards par service"
        )
        st.plotly_chart(fig)
        
        # Tableau détaillé
        st.subheader("📋 Statistiques détaillées par employé")
        st.dataframe(stats_df, use_container_width=True)
    else:
        st.info("Aucune statistique disponible pour le mois en cours")

def show_gestion_conges():
    st.title("📅 Gestion des Congés")
    
    if st.session_state.user_role == "admin":
        tab1, tab2, tab3 = st.tabs(["Demander Congé", "Gestion Demandes", "Gestion Quotas"])
    else:
        tab1, tab2 = st.tabs(["Demander Congé", "Mes Demandes"])
    
    with tab1:
        st.subheader("➕ Nouvelle demande de congé")
        
        # Pour les administrateurs, permettre de sélectionner l'employé
        if st.session_state.user_role == "admin":
            personnel_df = get_personnel()
            if not personnel_df.empty:
                employe_selection = st.selectbox(
                    "Sélectionner un employé",
                    personnel_df.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']}", axis=1)
                )
                
                if employe_selection:
                    selected_index = personnel_df[
                        personnel_df.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']}" == employe_selection, axis=1)
                    ].index[0]
                    emp_data = personnel_df.loc[selected_index]
                    personnel_id = emp_data['id']
                    nom_employe = f"{emp_data['prenom']} {emp_data['nom']}"
                else:
                    personnel_id = None
                    nom_employe = ""
            else:
                personnel_id = None
                nom_employe = ""
                st.info("Aucun personnel enregistré")
        else:
            # Pour les utilisateurs normaux, utiliser leur propre ID
            personnel_id = st.session_state.user_id
            # Récupérer le nom de l'employé
            conn = get_connection()
            if conn:
                try:
                    with conn:
                        cur = conn.cursor()
                        cur.execute(
                            "SELECT nom, prenom FROM personnels WHERE id = ?",
                            (personnel_id,)
                        )
                        result = cur.fetchone()
                        if result:
                            nom_employe = f"{result['prenom']} {result['nom']}"
                        else:
                            nom_employe = "Utilisateur"
                except:
                    nom_employe = "Utilisateur"
                finally:
                    conn.close()
            else:
                nom_employe = "Utilisateur"
        
        if personnel_id:
            quota = get_quota_conges(personnel_id)
            if not quota or quota['jours_restants'] <= 0:
                st.warning("⚠️ Plus de jours de congé disponibles.")
            
            with st.form("demande_conge"):
                # Afficher le nom de l'employé
                st.write(f"**Employé:** {nom_employe}")
                
                col1, col2 = st.columns(2)
                with col1:
                    date_debut = st.date_input("Date de début*", min_value=date.today())
                    type_conge = st.selectbox("Type de congé*", ["Congé annuel", "Maladie", "Familial", "Exceptionnel"])
                with col2:
                    date_fin = st.date_input("Date de fin*", min_value=date.today())
                    motif = st.text_area("Motif*")
                
                if date_debut and date_fin:
                    jours_demandes = (date_fin - date_debut).days + 1
                    st.info(f"**Jours demandés**: {jours_demandes}")
                    
                    if quota and jours_demandes > quota['jours_restants']:
                        st.error(f"❌ Jours restants insuffisants: {quota['jours_restants']}")
                
                if st.form_submit_button("📤 Soumettre la demande", disabled=not quota or quota['jours_restants'] <= 0):
                    if date_debut <= date_fin:
                        success, message = demander_conge(personnel_id, date_debut, date_fin, type_conge, motif)
                        if success:
                            st.success(f"✅ {message}")
                        else:
                            st.error(f"❌ {message}")
                    else:
                        st.error("❌ La date de fin doit être après la date de début")
        else:
            st.warning("Veuillez sélectionner un employé")
    
    # Deuxième onglet - Mes Demandes pour tous les utilisateurs
    if st.session_state.user_role == "admin":
        tab_name = "Gestion Demandes"
    else:
        tab_name = "Mes Demandes"
    
    with tab2:
        st.subheader("📋 " + tab_name)
        
        if st.session_state.user_role == "admin":
            filtre_statut = st.selectbox("Filtrer par statut", ["Tous", "En attente", "Approuvé", "Rejeté"])
            demandes = get_tous_les_conges(filtre_statut if filtre_statut != "Tous" else "Tous")
        else:
            # Pour les utilisateurs normaux, afficher seulement leurs demandes
            demandes = get_conges_employe(st.session_state.user_id)
        
        if not demandes.empty:
            for _, demande in demandes.iterrows():
                if st.session_state.user_role == "admin":
                    titre = f"{demande['prenom']} {demande['nom']} - {demande['date_debut']} au {demande['date_fin']}"
                else:
                    titre = f"{demande['date_debut']} au {demande['date_fin']} - {demande['statut']}"
                
                with st.expander(titre):
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.session_state.user_role == "admin":
                            st.write(f"**Employé:** {demande['prenom']} {demande['nom']}")
                            st.write(f"**Service:** {demande['service']}")
                        st.write(f"**Type:** {demande['type_conge']}")
                        st.write(f"**Jours:** {demande['duree_jours']}")
                        st.write(f"**Statut:** {demande['statut']}")
                    with col2:
                        st.write(f"**Motif:** {demande['motif']}")
                        st.write(f"**Date demande:** {demande['created_at']}")
                    
                    if st.session_state.user_role == "admin" and demande['statut'] == "En attente":
                        col_btn1, col_btn2 = st.columns(2)
                        with col_btn1:
                            if st.button("✅ Approuver", key=f"app_{demande['id']}"):
                                if approuver_conge(demande['id']):
                                    st.success("Demande approuvée")
                                    st.rerun()
                        with col_btn2:
                            if st.button("❌ Rejeter", key=f"rej_{demande['id']}"):
                                if rejeter_conge(demande['id']):
                                    st.success("Demande rejetée")
                                    st.rerun()
        else:
            st.info("Aucune demande de congé")
    
    # Onglet réservé aux administrateurs pour la gestion des quotas
    if st.session_state.user_role == "admin":
        with tab3:
            st.subheader("⚙️ Gestion des quotas de congés")
            
            personnel_df = get_personnel()
            if not personnel_df.empty:
                employe_selection = st.selectbox(
                    "Sélectionner un employé",
                    personnel_df.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']}", axis=1),
                    key="quota_select"
                )
                
                if employe_selection:
                    selected_index = personnel_df[
                        personnel_df.apply(lambda x: f"{x['prenom']} {x['nom']} - {x['service']}" == employe_selection, axis=1)
                    ].index[0]
                    
                    emp_data = personnel_df.loc[selected_index]
                    quota = get_quota_conges(emp_data['id'])
                    
                    if quota:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Jours alloués", quota['jours_alloues'])
                        with col2:
                            st.metric("Jours pris", quota['jours_pris'])
                        with col3:
                            st.metric("Jours restants", quota['jours_restants'])
                        
                        nouveau_quota = st.number_input(
                            "Nouveau quota de jours",
                            min_value=0,
                            max_value=365,
                            value=quota['jours_alloues'],
                            key=f"quota_{emp_data['id']}"
                        )
                        
                        if st.button("💾 Modifier le quota", key=f"mod_quota_{emp_data['id']}"):
                            if modifier_quota_conges(emp_data['id'], nouveau_quota):
                                st.success("✅ Quota modifié avec succès")
                                st.rerun()
                    else:
                        st.error("❌ Impossible de récupérer le quota")
            else:
                st.info("Aucun personnel enregistré")

def show_tours_role_nuit():
    st.title("🌙 Gestion des Tours de Rôle de Nuit")
    
    if st.session_state.user_role != "admin":
        st.warning("⛔ Accès réservé aux administrateurs")
        return
    
    tab1, tab2, tab3 = st.tabs(["Configuration", "Personnel par Service", "Historique"])
    
    with tab1:
        st.subheader("📅 Définir les groupes actifs pour aujourd'hui")
        
        services_nuit = get_services_nuit()
        
        if not services_nuit:
            st.info("Aucun service avec du personnel de nuit")
        else:
            for service in services_nuit:
                groupe_actuel = get_groupe_nuit_actif_service(service)
                
                col1, col2 = st.columns([2, 1])
                with col1:
                    st.write(f"**{service}**")
                with col2:
                    nouveau_groupe = st.radio(
                        f"Groupe actif pour {service}",
                        ["A", "B"],
                        index=0 if groupe_actuel == "A" else 1,
                        key=f"groupe_{service}",
                        horizontal=True
                    )
                
                if st.button(f"💾 Enregistrer pour {service}", key=f"btn_{service}"):
                    if definir_groupe_nuit_du_jour(service, nouveau_groupe):
                        st.success(f"✅ Groupe {nouveau_groupe} défini comme actif pour {service}")
                    else:
                        st.error("❌ Erreur lors de l'enregistrement")
                
                st.divider()
    
    with tab2:
        st.subheader("👥 Personnel de nuit par service et groupe")
        
        personnel_par_service = get_personnel_nuit_par_service()
        
        if not personnel_par_service:
            st.info("Aucun personnel de nuit")
        else:
            for service, groupes in personnel_par_service.items():
                with st.expander(f"🏥 {service}"):
                    tab_a, tab_b = st.tabs(["Groupe A", "Groupe B"])
                    
                    with tab_a:
                        if groupes['A']:
                            df_a = pd.DataFrame(groupes['A'])
                            st.dataframe(df_a[['nom', 'prenom', 'heure_entree_prevue', 'heure_sortie_prevue']], 
                                       use_container_width=True)
                        else:
                            st.info("Aucun employé dans le groupe A")
                    
                    with tab_b:
                        if groupes['B']:
                            df_b = pd.DataFrame(groupes['B'])
                            st.dataframe(df_b[['nom', 'prenom', 'heure_entree_prevue', 'heure_sortie_prevue']], 
                                       use_container_width=True)
                        else:
                            st.info("Aucun employé dans le groupe B")
    
    with tab3:
        st.subheader("📊 Historique des tours de rôle")
        
        services_nuit = get_services_nuit()
        service_selection = st.selectbox(
            "Sélectionner un service",
            ["Tous les services"] + services_nuit
        )
        
        historique = get_historique_tours_nuit(service_selection if service_selection != "Tous les services" else None)
        
        if not historique.empty:
            st.dataframe(historique, use_container_width=True)
            
            # Statistiques des tours
            if service_selection == "Tous les services":
                stats_tours = historique.groupby(['service', 'groupe_actif']).size().unstack(fill_value=0)
                st.subheader("📈 Répartition des tours par service")
                st.dataframe(stats_tours, use_container_width=True)
                
                fig = px.bar(
                    stats_tours.reset_index().melt(id_vars='service', var_name='Groupe', value_name='Count'),
                    x='service',
                    y='Count',
                    color='Groupe',
                    title="Répartition des tours de rôle par service"
                )
                st.plotly_chart(fig)
            else:
                stats_tours = historique['groupe_actif'].value_counts()
                fig = px.pie(
                    values=stats_tours.values,
                    names=stats_tours.index,
                    title=f"Répartition des tours de rôle pour {service_selection}"
                )
                st.plotly_chart(fig)
        else:
            st.info("Aucun historique de tours de rôle disponible")

def show_gestion_utilisateurs():
    st.title("👥 Gestion des Utilisateurs")
    
    if st.session_state.user_role != "admin":
        st.warning("⛔ Accès réservé aux administrateurs")
        return
    
    tab1, tab2 = st.tabs(["Liste des Utilisateurs", "Ajouter un Utilisateur"])
    
    with tab1:
        st.subheader("📋 Liste des utilisateurs")
        users_df = get_all_users()
        if not users_df.empty:
            st.dataframe(users_df, use_container_width=True)
        else:
            st.info("Aucun utilisateur enregistré")
    
    with tab2:
        st.subheader("➕ Ajouter un nouvel utilisateur")
        with st.form("ajouter_utilisateur"):
            col1, col2 = st.columns(2)
            with col1:
                username = st.text_input("Nom d'utilisateur*")
                email = st.text_input("Email")
            with col2:
                password = st.text_input("Mot de passe*", type="password")
                role = st.selectbox("Rôle", ["user", "admin"])
            
            if st.form_submit_button("➕ Ajouter l'utilisateur"):
                if username and password:
                    if create_user(username, password, role, email):
                        st.success("✅ Utilisateur ajouté avec succès")
                    else:
                        st.error("❌ Erreur lors de l'ajout de l'utilisateur")
                else:
                    st.warning("⚠️ Veuillez remplir tous les champs obligatoires")

# =========================
# Point d'entrée principal
# =========================

if __name__ == "__main__":
    
    update_sqlite_date_handling()
    # Initialisation des états de session
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "user" not in st.session_state:
        st.session_state.user = None
    if "user_role" not in st.session_state:
        st.session_state.user_role = None
    if "user_id" not in st.session_state:
        st.session_state.user_id = None
    if "show_stats" not in st.session_state:
        st.session_state.show_stats = False
    
    # Vérification de la connexion à la base de données
    if not test_connection_background():
        st.error("❌ Impossible de se connecter à la base de données. Vérifiez la configuration.")
        st.stop()
    
    # Initialisation des tables
    if not create_tables():
        st.error("❌ Erreur lors de l'initialisation des tables de la base de données.")
        st.stop()
    
    # Lancement de l'application
    main()