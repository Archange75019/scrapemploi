import os
import re
import time
import uuid

import pymongo
import requests
import json
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId
from typing import List, Dict, Any

load_dotenv()

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_API_URL = os.getenv("MISTRAL_API_URL")
MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME_OFFERS = os.getenv("COLLECTION_NAME", "job_offers")
COLLECTION_NAME_CVS = os.getenv("COLLECTION_CV", "resume")
MONGO_CV = os.getenv("MONGO_CV")
DB_CV = os.getenv("DB_CV")
COLLECTION_CV = os.getenv("COLLECTION_CV")
# Connexion à MongoDB
client = MongoClient(MONGODB_URI)
db = client[DB_NAME]
offers_collection = db[COLLECTION_NAME_OFFERS]
cv_collection = db[COLLECTION_NAME_CVS]

def check_mongodb_connections():
    try:
        # --- Connexion à service-job (offres) ---
        print("🔍 Vérification de la connexion à 'service-job'...")
        offers_client = MongoClient(MONGODB_URI)
        offers_client.server_info()  # Test de connexion
        offers_db = offers_client[DB_NAME]
        offers_collection = offers_db[COLLECTION_NAME_OFFERS]
        print(f"✅ Connexion OK à 'service-job' (Base: {DB_NAME}, Collection: {COLLECTION_NAME_OFFERS})")

        # --- Connexion à service-profile (CVs) ---
        print("\n🔍 Vérification de la connexion à 'service-profile'...")
        cv_client = MongoClient(MONGO_CV)
        cv_client.server_info()  # Test de connexion
        cv_db = cv_client[DB_CV]
        cv_collection = cv_db[COLLECTION_CV]
        print(f"✅ Connexion OK à 'service-profile' (Base: {DB_CV}, Collection: {COLLECTION_CV})")

        # --- Test d'insertion dans les deux collections ---
        # Test dans job_offers
        test_offer = {"test": "test_offer", "from": "connection_check"}
        offer_result = offers_collection.insert_one(test_offer)
        print(f"✅ Test INSERT OK dans 'job_offers' (ID: {offer_result.inserted_id})")
        offers_collection.delete_one({"_id": offer_result.inserted_id})  # Nettoyage

        # Test dans resume
        test_cv = {"test": "test_cv", "from": "connection_check"}
        cv_result = cv_collection.insert_one(test_cv)
        print(f"✅ Test INSERT OK dans 'resume' (ID: {cv_result.inserted_id})")
        cv_collection.delete_one({"_id": cv_result.inserted_id})  # Nettoyage

        # --- Affichage des compteurs ---
        offers_count = offers_collection.count_documents({})
        cv_count = cv_collection.count_documents({})
        print(f"\n📊 **Offres disponibles** : {offers_count}")
        print(f"📊 **CVs existants** : {cv_count}")

        return offers_collection, cv_collection

    except Exception as e:
        print(f"❌ ERREUR MongoDB : {e}")
        print(f"   → Vérifiez MONGODB_URI, MONGO_CV, DB_NAME, DB_CV, et les droits d'écriture.")
        raise


def call_mistral_api(prompt: str, max_tokens: int = 2000, temperature: float = 0.7, log_func=None) -> str:
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "mistral-tiny",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature
    }
    try:
        # Augmenter le délai entre les appels
        time.sleep(5)
        response = requests.post(MISTRAL_API_URL, headers=headers, json=data, timeout=60)
        if response.status_code != 200:
            if log_func:
                log_func(f"Erreur API Mistral: {response.status_code} - {response.text}", "error")
            return ""
        result = response.json()
        if "choices" in result and len(result["choices"]) > 0:
            return result["choices"][0]["message"]["content"]
        else:
            if log_func:
                log_func("Erreur Mistral, pas de choix retourné", "error")
            return ""
    except requests.exceptions.Timeout:
        if log_func:
            log_func("Timeout lors de l'appel à l'API Mistral", "error")
        return ""
    except Exception as e:
        if log_func:
            log_func(f"Erreur Mistral: {e}", "error")
        return ""

def get_offers(start_page=1, end_page=5000, max_jobs_per_page=None):
    try:
        total_offers = offers_collection.count_documents({})
        print(f"Nombre total d'offres : {total_offers}")

        if max_jobs_per_page is None:
            offers = list(offers_collection.find({}))
            print(f"Récupéré {len(offers)} offres.")
            return offers

        offers = []
        for page in range(start_page, end_page + 1):
            skip = (page - 1) * max_jobs_per_page
            page_offers = list(offers_collection.find({}).skip(skip).limit(max_jobs_per_page))
            offers.extend(page_offers)
            print(f"Récupéré {len(page_offers)} offres pour la page {page}.")

        return offers

    except Exception as e:
        print(f"Erreur lors de la récupération des offres : {e}")
        return []

def convert_objectid_to_str(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convertit les champs ObjectId en chaînes de caractères."""
    if isinstance(data, dict):
        new_data = {}
        for key, value in data.items():
            if isinstance(value, ObjectId):
                new_data[key] = str(value)
            elif isinstance(value, (dict, list)):
                new_data[key] = convert_objectid_to_str(value)
            else:
                new_data[key] = value
        return new_data
    elif isinstance(data, list):
        return [convert_objectid_to_str(item) for item in data]
    else:
        return data

def extract_json_from_response(resp: str) -> List[Dict[str, Any]]:
    if not resp:
        return []
    # Nettoyer les blocs ```json
    json_pattern = re.compile(r'```json\s*(.*?)\s*```', re.DOTALL)
    matches = json_pattern.findall(resp)
    all_cvs = []
    for idx, match in enumerate(matches):
        json_str = match.strip()  # Nettoyer les espaces
        try:
            data = json.loads(json_str)
            if isinstance(data, list):
                all_cvs.extend(data)
            elif isinstance(data, dict):
                all_cvs.append(data)
        except json.JSONDecodeError as e:
            print(f"Erreur de parsing (bloc {idx+1}): {e}")
            print(f"Contenu problématique: '{json_str[:100]}...'")
            continue
    # Si aucun bloc ```json, essayer de parser directement la réponse
    if not all_cvs:
        try:
            data = json.loads(resp)
            if isinstance(data, list):
                all_cvs.extend(data)
            elif isinstance(data, dict):
                all_cvs.append(data)
        except json.JSONDecodeError:
            pass
    return all_cvs

def is_valid_cv(cv: Dict) -> bool:
    required_fields = ["userId", "basics"]
    for field in required_fields:
        if field not in cv:
            return False
    if not isinstance(cv["basics"], dict):
        return False
    return True

def generate_adapted_cvs(offer: Dict[str, Any], log_func=None) -> List[Dict[str, Any]]:
    """
    Génère 3 CVs adaptés à l'offre d'emploi.
    Retourne une liste de dictionnaires structurés.
    """
    offer_str = convert_objectid_to_str(offer)

    # Extraire les informations clés de l'offre
    titre = offer_str.get("titre", "Poste")
    mission  = offer_str.get("mission", "")
    profil = offer_str.get("profilRecherche", "")
    profilRecherche = offer_str.get("profilRecherche")
    offer_skills = offer_str.get("skills", [])
    print(f"🔍 OFFRE: '{titre}' | Mission: {len(mission)}c | Profil: {len(profil)}c")
    prompt = f"""RETOURNE **UNIQUEMENT** 3 CVs JSON strict valides sans texte ni avant ni après séparés par `````` pour:

OFFRE:
Titre: {titre}
Description: {mission}
profilRecherche: {profilRecherche}
Compétences: {', '.join(offer_skills[:10]) if offer_skills else 'Non spécifié'}
IMPORTANT: Dans basics.label, utilise EXACTEMENT "{titre}" pour chaque CV!
Va sur internet, linkedin pour générer des CVs réaliste en terme de compétences, experiences, education, certifications et summary
Génère 3 CVs UNIQUES  sous forme de tableau JSON (junior, intermédiaire, senior) en respectant cette structure EXACTE:
**Instructions strictes** :
1. **CV Junior** : 0-2 ans d'expérience, compétences basiques, summary motivé.
2. **CV Intermédiaire** : 3-5 ans d'expérience, compétences techniques, summary orienté résultats.
3. **CV Senior** : 6+ ans d'expérience, compétences avancées, summary orienté leadership.
**STRUCTURE EXACTE pour chaque CV (copie-colle): **
```json
[
{{
    "userId": "{uuid.uuid4()}",
    "basics": {{
        "name": "Prénom Nom",
        "label": "Titre du poste",
        "email": "email@example.com",
        "telephone": "0612345678",
        "summary": "Résumé professionnel"
    }},
    "work": [
        {{
            "name": "Entreprise",
            "position": "Poste",
            "startDate": "2020-01-15",
            "typeContrat": "CDI",
            "endDate": "Present",
            "summary": "Détails de l'expérience",
            "location": "Ville, Pays"
        }}
    ],
    "education": [
        {{
            "institution": "École",
            "area": "Domaine",
            "studyType": "Diplôme",
            "startDate": "2015-09-01",
            "endDate": "2018-06-30"
        }}
    ],
    "skills": ["compétence1", "compétence2"],
    "certifications": [],
    "languages": [{{"language": "fr", "fluency": "Native Speaker"}}],
    "_class": "com.scrapper.serviceprofile.model.Resume",
    "domain": "Tech"
}},
{{
    "userId": "{uuid.uuid4()}",,
    "basics": {{
        "name": "Prénom Nom",
        "label": "Titre du poste",
        "email": "email@example.com",
        "telephone": "0612345678",
        "summary": "Résumé professionnel"
    }},
    "work": [
        {{
            "name": "Entreprise",
            "position": "Poste",
            "startDate": "2020-01-15",
            "typeContrat": "CDI",
            "endDate": "Present",
            "summary": "Détails de l'expérience",
            "location": "Ville, Pays"
        }}
    ],
    "education": [
        {{
            "institution": "École",
            "area": "Domaine",
            "studyType": "Diplôme",
            "startDate": "2015-09-01",
            "endDate": "2018-06-30"
        }}
    ],
    "skills": ["compétence1", "compétence2"],
    "certifications": [],
    "languages": [{{"language": "fr", "fluency": "Native Speaker"}}],
    "_class": "com.scrapper.serviceprofile.model.Resume",
    "domain": "domaine"
}},
{{
    "userId": "{uuid.uuid4()}",,
    "basics": {{
        "name": "Prénom Nom",
        "label": "Titre du poste",
        "email": "email@example.com",
        "telephone": "0612345678",
        "summary": "Résumé professionnel"
    }},
    "work": [
        {{
            "name": "Entreprise",
            "position": "Poste",
            "startDate": "2020-01-15",
            "typeContrat": "CDI",
            "endDate": "Present",
            "summary": "Détails de l'expérience",
            "location": "Ville, Pays"
        }}
    ],
    "education": [
        {{
            "institution": "École",
            "area": "Domaine",
            "studyType": "Diplôme",
            "startDate": "2015-09-01",
            "endDate": "2018-06-30"
        }}
    ],
    "skills": ["compétence1", "compétence2"],
    "certifications": [],
    "languages": [{{"language": "fr", "fluency": "Native Speaker"}}],
    "_class": "com.scrapper.serviceprofile.model.Resume",
    "domain": "Tech"
}}
]
```
**
Crée CV1 (junior), CV2 (intermédiaire), CV3 (senior) en suivant ce format."""



    print("→ Appel à l'API Mistral...")
    resp = call_mistral_api(prompt, max_tokens=4000, temperature=0.7, log_func=log_func)

    if not resp:
        if log_func:
            log_func("Aucune réponse de l'API Mistral", "error")
        return []

    print(f"→ Réponse reçue ({len(resp)} caractères)")

    # Parser la réponse
    cvs = extract_json_from_response(resp)




    return cvs

def store_cvs_in_mongodb(cvs: List[Dict[str, Any]], offer_id: str):
    for cv in cvs:
        if not is_valid_cv(cv):
            print(f"⚠️ CV invalide : {cv}")
            continue
        # Vérifier si le CV existe déjà
        existing = cv_collection.find_one({"userId": cv["userId"]})
        if existing:
            print(f"ℹ️ CV déjà en base (ID: {cv['userId']})")
            continue
        # Insérer le CV
        cv_collection.insert_one(cv)
        print(f"✅ CV inséré (ID: {cv['userId']})")

def process_offers(offers: List[Dict[str, Any]], limit: int = None):
    if limit:
        offers = offers[:limit]
        print(f"⚠️ Mode test: {limit} offres")

    total = len(offers)
    success = 0
    failed = 0

    for i, offer in enumerate(offers, 1):
        offer_id = str(offer['_id'])
        title = (offer.get('titre') or offer.get('title', 'Sans titre'))[:50]

        print(f"\n{'='*80}")
        print(f"[{i:2d}/{total}] {title}")
        print(f"ID: {offer_id}")
        print('='*80)

        try:
            cvs = generate_adapted_cvs(offer)
            if cvs:
                inserted = store_cvs_in_mongodb(cvs, offer_id)
                success += 1          # ← 8 espaces (2 tabs) ICI
                print(f"✅ {len(cvs)} CVs OK")
            else:
                failed += 1
                print("❌ Aucun CV généré")
        except KeyboardInterrupt:
            print("\n⏹️ Arrêt demandé")
            break
        except Exception as e:
            failed += 1
            print(f"💥 Erreur: {e}")

    print(f"\n{'='*80}")
    print(f"RÉSULTATS: {success} ✅ | {failed} ❌ | Total: {total}")
    print('='*80)

if __name__ == "__main__":
    print("\n" + "="*80)
    print("DÉMARRAGE DU SCRIPT DE GÉNÉRATION DE CVS")
    print("="*80 + "\n")

    # Vérification des connexions MongoDB
    offers_collection, cv_collection = check_mongodb_connections()

    # Récupération des offres
    all_offers = get_offers()

    print(f"\n📊 Nombre total d'offres: {len(all_offers)}")
    print("\n⚠ MODE TEST: 3 offres")

    # Traitement des offres
    process_offers(all_offers, limit=4000)

    print("\n" + "="*80)
    print("SCRIPT TERMINÉ")
    print("="*80)
