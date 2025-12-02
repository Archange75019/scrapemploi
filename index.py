import requests
import threading
import time
import random
import json
import re
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from bson import ObjectId
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv()
# --- Configuration ---
START_PAGE = int(os.getenv("START_PAGE", 1))
END_PAGE = int(os.getenv("END_PAGE", 50000))
MAX_JOBS_PER_PAGE = os.getenv("MAX_JOBS_PER_PAGE", None)
MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
FRANCETRAVAIL_CLIENT_ID = os.getenv("FRANCETRAVAIL_CLIENT_ID")
FRANCETRAVAIL_CLIENT_SECRET = os.getenv("FRANCETRAVAIL_CLIENT_SECRET")
FRANCETRAVAIL_GRANT_TYPE = os.getenv("FRANCETRAVAIL_GRANT_TYPE")
FRANCETRAVAIL_SCOPE = os.getenv("FRANCETRAVAIL_SCOPE")
FRANCETRAVAIL_REALM = os.getenv("FRANCETRAVAIL_REALM")

# --- Classes utilitaires ---
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

# --- Fonctions MongoDB ---
def init_mongodb(uri=MONGODB_URI, db_name=DB_NAME, collection_name=COLLECTION_NAME):
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[db_name]
        collection = db[collection_name]
        print(f"✅ Connexion MongoDB réussie (Base: {db_name}, Collection: {collection_name})")
        return collection
    except ConnectionFailure as e:
        print(f"❌ Erreur connexion MongoDB: {e}")
        return None
    except Exception as e:
        print(f"❌ Erreur MongoDB: {e}")
        return None

def save_to_mongodb(collection, job_info):
    if collection is None:
        print("⚠️ MongoDB non disponible - pas de sauvegarde en base")
        return False
    try:
        existing_offer = collection.find_one({"idOffre": job_info["idOffre"]})
        if existing_offer:
            print(f"ℹ️ Offre déjà en base (ID: {job_info['idOffre']})")
            return True
        result = collection.insert_one(job_info)
        print(f"✅ Offre sauvegardée MongoDB (ID: {result.inserted_id})")
        return True
    except DuplicateKeyError:
        print(f"ℹ️ Offre déjà existante en base (ID: {job_info['idOffre']})")
        return True
    except Exception as e:
        print(f"❌ Erreur sauvegarde MongoDB: {e}")
        return False

# --- Fonctions France Travail ---
def get_francetravail_token(client_id, client_secret, grant_type, scope, realm):
    url = f"https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm={realm}"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": grant_type,
        "scope": scope,
    }
    try:
        response = requests.post(url, headers=headers, data=data, timeout=10)
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        print(f"❌ Erreur récupération token France Travail: {e}")
        return None

def search_francetravail_offers_all(token, min_creation_date=None, max_creation_date=None):
    if min_creation_date is None:
        date_obj = datetime.now() - timedelta(days=30)
        min_creation_date = date_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    if max_creation_date is None:
        max_creation_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    if "T" not in min_creation_date:
        min_creation_date = f"{min_creation_date}T00:00:00Z"
    if "T" not in max_creation_date:
        max_creation_date = f"{max_creation_date}T23:59:59Z"

    all_results = []
    range_start = 0
    range_limit = 150

    while True:
        url = f"https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search?minCreationDate={min_creation_date}&maxCreationDate={max_creation_date}&range={range_start}-{range_start + range_limit - 1}"
        headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            resultats = data.get("resultats", [])
            if not resultats:
                break
            all_results.extend(resultats)
            if len(resultats) < range_limit:
                break
            range_start += range_limit
        except Exception as e:
            print(f"❌ Erreur API France Travail: {e}")
            break
    return all_results

def convert_francetravail_to_hellowork(ft_offer):
    now_str = datetime.now().strftime('%d/%m/%Y')
    return {
        "site": "France Travail",
        "idOffre": ft_offer.get("id", "N/A"),
        "titre": ft_offer.get("intitule", "N/A"),
        "entreprise": ft_offer.get("entreprise", {}).get("nom", "N/A"),
        "lien": ft_offer.get("origineOffre", {}).get("urlOrigine", "N/A"),
        "localisation": ft_offer.get("lieuTravail", {}).get("libelle", "N/A"),
        "typeContrat": ft_offer.get("typeContratLibelle", "N/A"),
        "datePublication": datetime.fromisoformat(ft_offer.get("dateCreation")).strftime('%d/%m/%Y') if ft_offer.get("dateCreation") else "N/A",
        "salaire": ft_offer.get("salaire", {}).get("libelle", "Non spécifié"),
        "mission": ft_offer.get("description", "N/A"),
        "profilRecherche": "; ".join([c.get("libelle", "") for c in ft_offer.get("competences", [])]) if ft_offer.get("competences") else "Non spécifié",
        "about": ft_offer.get("entreprise", {}).get("description", "Non spécifié"),
        "dateInscriptionBase": now_str,
        "pageSource": "France Travail",
    }

def save_francetravail_offers_to_mongodb(offers, collection):
    for offer in offers:
        hw_offer = convert_francetravail_to_hellowork(offer)
        save_to_mongodb(collection, hw_offer)

# --- Fonctions FreeWork ---
def create_stealth_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--start-maximized')
    chrome_options.add_argument('--disable-notifications')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    })
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def parse_date_publication(date_text):
    date_text = date_text.lower().strip()
    now = datetime.now()
    heures_match = re.search(r'(\d+)\s*heure', date_text)
    jours_match = re.search(r'(\d+)\s*jour', date_text)
    if heures_match:
        heures = int(heures_match.group(1))
        date_publication = now - timedelta(hours=heures)
    elif jours_match:
        jours = int(jours_match.group(1))
        date_publication = now - timedelta(days=jours)
    elif 'aujourd\'hui' in date_text or 'hui' in date_text:
        date_publication = now
    elif 'hier' in date_text:
        date_publication = now - timedelta(days=1)
    else:
        date_publication = now
    return date_publication.strftime('%d/%m/%Y')

def scrape_freework_page(driver, page_num):
    url = f"https://www.free-work.com/fr/tech-it/jobs?page={page_num}&locations=fr~~~"
    print(f"🔍 Chargement: {url}")
    try:
        driver.get(url)
        time.sleep(random.uniform(4, 6))
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/3);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/fr/tech-it/'][href*='/job-mission/']"))
        )
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        job_links = []
        all_links = soup.find_all('a', href=re.compile(r'/fr/tech-it/.*?/job-mission/'))
        for link in all_links:
            href = link.get('href')
            if href and not href.startswith('http'):
                full_url = f"https://www.free-work.com{href}"
                if full_url not in job_links:
                    job_links.append(full_url)
        print(f"✅ {len(job_links)} offres trouvées sur la page {page_num}")
        return job_links, str(page_num), str(len(job_links))
    except Exception as e:
        print(f"❌ Erreur scraping page FreeWork: {e}")
        return [], None, None

def extract_freework_job_info(job_url, driver, page_num, idx, mongo_collection=None):
    job_info = {"site": "FreeWork"}
    url_parts = job_url.split('/')
    job_info["idOffre"] = f"FW-{url_parts[-1]}" if len(url_parts) > 0 else f"FW-{page_num}-{idx}"
    print(f"\n   🆔 ID Offre: {job_info['idOffre']}")
    try:
        if mongo_collection is not None:
            existing_offer = mongo_collection.find_one({"idOffre": job_info["idOffre"]})
            if existing_offer:
                print(f"   ℹ️ Offre déjà en base - Ignorée")
                return {}
        driver.get(job_url)
        time.sleep(random.uniform(3, 5))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        # Titre
        h1_elem = soup.find("h1")
        if h1_elem:
            em_elem = h1_elem.find("em")
            if em_elem:
                em_elem.decompose()  # Supprime la balise <em> et son contenu
            job_info["titre"] = h1_elem.get_text(strip=True)
        else:
            job_info["titre"] = 'N/A'
        print(f"   📌 Titre: {job_info['titre']}")
        # Entreprise
        entreprise_elems = soup.select("p.font-semibold.text-sm")
        if entreprise_elems and len(entreprise_elems) > 0:
            entreprise_elem = entreprise_elems[0]  # Prend le premier élément
            job_info["entreprise"] = entreprise_elem.get_text(strip=True)
        else:
            job_info["entreprise"] = 'N/A'
        print(f"   🏢 Entreprise: {job_info['entreprise']}")
        # Lien
        job_info["lien"] = job_url
        # Type de contrat
        tags_div = soup.find("div", class_="tags relative w-full")
        contrats = []
        if tags_div:
            contrat_elems = tags_div.find_all("span", class_=re.compile(r"tag"))
            for elem in contrat_elems:
                contrat_text = elem.get_text(strip=True)
                contrats.append(contrat_text)

        contrats = list(set(contrats))
        job_info["typeContrat"] = ", ".join(contrats) if contrats else "N/A"
        print(f"   📄 Contrat: {job_info['typeContrat']}")
        # Localisation
        location_blocks = soup.find_all("div", class_="flex items-center py-1")
        localisation = "N/A"
        for block in reversed(location_blocks):
            if block.find("svg"):
                localisation = block.get_text(separator=" ", strip=True)
                break
        job_info["localisation"] = localisation
        # Date de publication
        date_elem = soup.find("time") or soup.find(string=re.compile(r'\d+\s+(jour|heure)'))
        if date_elem:
            date_text = date_elem.get_text() if hasattr(date_elem, 'get_text') else str(date_elem)
            job_info["datePublication"] = parse_date_publication(date_text)
        else:
            job_info["datePublication"] = datetime.now().strftime('%d/%m/%Y')
        print(f"   📅 Date: {job_info['datePublication']}")
        job_info["dateInscriptionBase"] = datetime.now().strftime('%d/%m/%Y')
        # Salaire
        salary_blocks = soup.find_all("div", class_="flex items-center py-1")
        salaire = "Non spécifié"
        for block in salary_blocks:
            text = block.get_text(separator=" ", strip=True)
            if re.search(r"\b\d+[kK]?\s*€", text):
                salaire = text
                break
        job_info["salaire"] = salaire
        # Mission
        mission_elem = soup.find("div", class_=re.compile(r"description|content|prose"))
        job_info["mission"] = mission_elem.get_text(strip=True) if mission_elem else 'Non spécifié'
        print(f"   📋 Mission: {job_info['mission'][:60]}...")
        # Profil recherché
        profil_elem = soup.find("h2", string=re.compile(r"Profil|Compétence"))
        if profil_elem:
            profil_section = profil_elem.find_next_sibling()
            job_info["profilRecherche"] = profil_section.get_text(strip=True) if profil_section else 'Non spécifié'
        else:
            job_info["profilRecherche"] = 'Non spécifié'
        print(f"   👤 Profil: {job_info['profilRecherche'][:50]}...")
        # À propos
        about_elem = soup.find("div", class_="mt-4 line-clamp-3")
        about_text = "Non spécifié"
        if about_elem:
            about_text = about_elem.get_text(separator=" ", strip=True)
        else:
            # Si la classe exacte n'est pas trouvée, essayer une recherche plus large
            about_elem = soup.find("div", class_=re.compile(r"mt-4"))
            if about_elem:
                about_text = about_elem.get_text(separator=" ", strip=True)
        about_text = about_text.replace("\r\n", " ").replace("\n", " ").strip()
        about_text = ' '.join(about_text.split())
        job_info["about"] = about_text
        print(f"   📄 About: {job_info['about'][:100]}...")
        job_info["pageSource"] = "Free-Work"
        # Sauvegarde MongoDB
        if mongo_collection is not None:
            save_to_mongodb(mongo_collection, job_info)
        return job_info
    except Exception as e:
        print(f"   ⚠️ Erreur extraction FreeWork: {e}")
        return {}

def scrape_freework(start_page=1, end_page=1, mongodb_uri=MONGODB_URI, db_name=DB_NAME, collection_name=COLLECTION_NAME):
    print("🚀 Démarrage du scraping FreeWork")
    mongo_collection = init_mongodb(mongodb_uri, db_name, collection_name)
    if mongo_collection is None:
        print("⚠️ MongoDB non disponible, les données ne seront pas sauvegardées")
    driver = create_stealth_driver()
    total_jobs_count = 0
    mongo_saved_count = 0
    try:
        for page_num in range(start_page, end_page + 1):
            print(f"\n{'='*50}")
            print(f"PAGE FREEWORK {page_num}/{end_page}")
            print(f"{'='*50}")
            job_links, current_page, items_per_page = scrape_freework_page(driver, page_num)
            if not job_links:
                print(f"⚠️ Aucune offre trouvée sur la page {page_num}")
                break
            for idx, job_url in enumerate(job_links, 1):
                print(f"\n   📋 Offre {idx}/{len(job_links)}")
                job_info = extract_freework_job_info(job_url, driver, page_num, idx, mongo_collection)
                if job_info and job_info.get('idOffre'):
                    mongo_saved_count += 1
                total_jobs_count += 1
                time.sleep(random.uniform(2, 4))
        print(f"\n{'='*50}")
        print(f"📊 RÉSUMÉ DU SCRAPING FREEWORK")
        print(f"{'='*50}")
        print(f"Pages scrapées: {end_page - start_page + 1}")
        print(f"Total offres traitées: {total_jobs_count}")
        print(f"Offres sauvegardées MongoDB: {mongo_saved_count}")
        print(f"{'='*50}")
    except KeyboardInterrupt:
        print("\n⚠️ Scraping interrompu par l'utilisateur")
    except Exception as e:
        print(f"\n❌ Erreur critique FreeWork: {e}")
    finally:
        driver.quit()
        print("✅ Navigateur FreeWork fermé")

# --- Fonctions HelloWork ---
def scrape_hellowork_page(driver, page_num, max_retries=3):
    for attempt in range(max_retries):
        try:
            url = f"https://www.hellowork.com/fr-fr/emploi/recherche.html?p={page_num}"
            print(f"   🔍 Chargement: {url}")
            driver.get(url)
            wait_time = random.uniform(4, 6)
            time.sleep(wait_time)
            if "403 Forbidden" in driver.page_source or "403" in driver.title:
                print(f"   ❌ Erreur 403 - Tentative {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(10, 15))
                    continue
                return []
            wait = WebDriverWait(driver, 15)
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul[aria-label='liste des offres']")))
            except:
                print(f"   ⚠️ Timeout chargement")
                if attempt < max_retries - 1:
                    continue
                return []
            total_height = driver.execute_script("return document.body.scrollHeight")
            scroll_step = total_height // 4
            for i in range(1, 4):
                driver.execute_script(f"window.scrollTo(0, {scroll_step * i});")
                time.sleep(random.uniform(0.5, 1.5))
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            job_elements = soup.select("li div[data-id-storage-target='item']")
            if len(job_elements) == 0:
                job_elements = soup.select("div[data-id-storage-item-id]")
            print(f"   ✅ {len(job_elements)} offres trouvées")
            return job_elements
        except Exception as e:
            print(f"   ❌ Erreur: {e}")
            if attempt < max_retries - 1:
                time.sleep(10)
            else:
                return []
    return []

def extract_hellowork_job_info(job_element, driver, mongo_collection=None):
    job_info = {"site": "HelloWork"}
    idOffre = job_element.get('data-id-storage-item-id', 'N/A')
    job_info["idOffre"] = idOffre
    print(f"      🆔 ID Offre: {idOffre}")
    title_elem = job_element.select_one('h3.tw-inline > p.tw-typo-l')
    job_info["titre"] = title_elem.get_text(strip=True) if title_elem else 'N/A'
    print(f"      📌 Titre: {job_info['titre']}")
    entreprise_elem = job_element.select_one('h3.tw-inline > p.tw-typo-s')
    job_info["entreprise"] = entreprise_elem.get_text(strip=True) if entreprise_elem else 'N/A'
    print(f"      🏢 Entreprise: {job_info['entreprise']}")
    link_elem = job_element.select_one("a[data-cy='offerTitle']")
    if link_elem and 'href' in link_elem.attrs:
        job_info["lien"] = f"https://www.hellowork.com{link_elem['href']}"
    else:
        job_info["lien"] = 'N/A'
    if mongo_collection is not None:
        existing_offer = mongo_collection.find_one({"lien": job_info["lien"]})
        if existing_offer:
            print(f"      ℹ️ Offre déjà en base (ID: {idOffre}) - Ignorée")
            return {}
    localisation_elem = job_element.select_one("div[data-cy='localisationCard']")
    job_info["localisation"] = localisation_elem.get_text(strip=True) if localisation_elem else 'N/A'
    print(f"      📍 Localisation: {job_info['localisation']}")
    contrat_elem = job_element.select_one("div[data-cy='contractCard']")
    job_info["typeContrat"] = contrat_elem.get_text(strip=True) if contrat_elem else 'N/A'
    print(f"      📄 Contrat: {job_info['typeContrat']}")
    date_elem = job_element.select_one("div.tw-typo-s.tw-text-grey-500.tw-pl-1.tw-pt-1")
    if date_elem:
        date_text = date_elem.get_text(strip=True)
        job_info["datePublication"] = parse_date_publication(date_text)
        print(f"      📅 Date: {job_info['datePublication']}")
    else:
        job_info["datePublication"] = 'N/A'
    job_info["dateInscriptionBase"] = datetime.now().strftime('%d/%m/%Y')
    print(f"      🗓️ Inscription: {job_info['dateInscriptionBase']}")
    if job_info["lien"] != 'N/A':
        detailed_info = get_hellowork_detailed_job_info(driver, job_info["lien"])
        job_info.update(detailed_info)
    else:
        job_info["salaire"] = 'Non spécifié'
        job_info["mission"] = 'Non spécifié'
        job_info["profilRecherche"] = 'Non spécifié'
        job_info["about"] = 'Non spécifié'
    if mongo_collection is not None:
        save_to_mongodb(mongo_collection, job_info)
    return job_info

def get_hellowork_detailed_job_info(driver, job_url):
    print(f"      🔍 Accès à la page détaillée...")
    try:
        driver.get(job_url)
        time.sleep(random.uniform(3, 5))
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.tw-flex.tw-flex-col.tw-gap-4.sm\\:tw-gap-6.tw-col-span-full.lg\\:tw-col-span-8"))
            )
        except Exception as e:
            print(f"      ⚠️ Erreur chargement : {e}")
            return {}
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        detailed_info = {}
        salaire_elem = soup.select_one('button[data-cy="salary-tag-button"]')
        detailed_info["salaire"] = salaire_elem.get_text(strip=True) if salaire_elem else 'Non spécifié'
        print(f"      💰 Salaire: {detailed_info['salaire']}")
        mission_elem = soup.select_one('div[data-truncate-text-target="content"]')
        if mission_elem:
            detailed_info["mission"] = mission_elem.get_text(strip=True)
            print(f"      📋 Mission: {detailed_info['mission'][:60]}...")
        else:
            detailed_info["mission"] = 'Non spécifié'
            print(f"      📋 Mission: Non spécifié")
        collapsed_div = soup.select_one('div[role="region"][aria-labelledby="collapsed-btn"]')
        if collapsed_div:
            paragraphs = collapsed_div.select('p.tw-typo-long-m')
            if len(paragraphs) >= 2:
                detailed_info["profilRecherche"] = paragraphs[0].get_text(strip=True)
                detailed_info["about"] = paragraphs[1].get_text(strip=True)
                print(f"      👤 Profil: {detailed_info['profilRecherche'][:50]}...")
                print(f"      🏢 About: {detailed_info['about'][:50]}...")
            elif len(paragraphs) == 1:
                detailed_info["profilRecherche"] = paragraphs[0].get_text(strip=True)
                detailed_info["about"] = 'Non spécifié'
                print(f"      👤 Profil: {detailed_info['profilRecherche'][:50]}...")
            else:
                detailed_info["profilRecherche"] = 'Non spécifié'
                detailed_info["about"] = 'Non spécifié'
        else:
            detailed_info["profilRecherche"] = 'Non spécifié'
            detailed_info["about"] = 'Non spécifié'
        print(f"      ✅ Détails extraits")
        return detailed_info
    except Exception as e:
        print(f"      ⚠️ Erreur extraction : {e}")
        return {}

def scrape_hellowork(start_page=1, end_page=1, max_jobs_per_page=None, mongodb_uri=MONGODB_URI, db_name=DB_NAME, collection_name=COLLECTION_NAME):
    print("🚀 Démarrage du scraping HelloWork")
    mongo_collection = init_mongodb(mongodb_uri, db_name, collection_name)
    if mongo_collection is None:
        print("❌ MongoDB non disponible, arrêt du scraping HelloWork")
        return
    driver = create_stealth_driver()
    total_jobs_count = 0
    mongo_saved_count = 0
    try:
        for page_num in range(start_page, end_page + 1):
            print(f"\n==== PAGE HELLOWORK {page_num}/{end_page} ====")
            jobs = scrape_hellowork_page(driver, page_num)
            if not jobs:
                print(f"⚠️ Aucune offre sur la page {page_num}, arrêt du scraping")
                break
            if jobs and max_jobs_per_page is not None and isinstance(max_jobs_per_page, int):
                jobs = jobs[:max_jobs_per_page]
            for idx, job in enumerate(jobs, 1):
                print(f"Offre {idx}/{len(jobs)}")
                job_info = extract_hellowork_job_info(job, driver, mongo_collection)
                if job_info.get('idOffre') != 'N/A':
                    mongo_saved_count += 1
                job_info["pageSource"] = page_num
                total_jobs_count += 1
                time.sleep(random.uniform(2, 4))
        print(f"Pages scrapées: {end_page - start_page + 1}")
        print(f"Total offres: {total_jobs_count}")
        print(f"Sauvegardées MongoDB: {mongo_saved_count}")
    except Exception as e:
        print(f"❌ Erreur critique HelloWork: {e}")
    finally:
        driver.quit()
        print("✅ Navigateur HelloWork fermé")

# --- Fonctions principales ---
def scrape_francetravail(francetravail_client_id, francetravail_client_secret, mongo_collection=None):
    print("🚀 Démarrage du scraping France Travail")
    token = get_francetravail_token(
        client_id=francetravail_client_id,
        client_secret=francetravail_client_secret,
        grant_type=FRANCETRAVAIL_GRANT_TYPE,
        scope=FRANCETRAVAIL_SCOPE,
        realm=FRANCETRAVAIL_REALM,
    )
    if not token:
        print("❌ Impossible de récupérer le token France Travail")
        return
    offers = search_francetravail_offers_all(token)
    if offers and mongo_collection is not None:
        save_francetravail_offers_to_mongodb(offers, mongo_collection)
        print(f"✅ {len(offers)} offres France Travail sauvegardées")
    else:
        print("⚠️ Aucune offre France Travail trouvée ou pas de connexion MongoDB")

def run_scraping():
    mongo_collection = init_mongodb(MONGODB_URI, DB_NAME, COLLECTION_NAME)
    threads = []
    # HelloWork
    hellowork_thread = threading.Thread(
        target=scrape_hellowork,
        kwargs={
            "start_page": START_PAGE,
            "end_page": END_PAGE,
            "max_jobs_per_page": MAX_JOBS_PER_PAGE,
            "mongodb_uri": MONGODB_URI,
            "db_name": DB_NAME,
            "collection_name": COLLECTION_NAME,
        }
    )
    # France Travail
    francetravail_thread = threading.Thread(
        target=scrape_francetravail,
        kwargs={
            "francetravail_client_id": FRANCETRAVAIL_CLIENT_ID,
            "francetravail_client_secret": FRANCETRAVAIL_CLIENT_SECRET,
            "mongo_collection": mongo_collection,
        }
    )
    # FreeWork
    freework_thread = threading.Thread(
        target=scrape_freework,
        kwargs={
            "start_page": START_PAGE,
            "end_page": END_PAGE,
            "mongodb_uri": MONGODB_URI,
            "db_name": DB_NAME,
            "collection_name": COLLECTION_NAME,
        }
    )
    # Lancement des threads
    threads.append(hellowork_thread)
    threads.append(francetravail_thread)
    threads.append(freework_thread)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("🎉 Scraping terminé !")

if __name__ == "__main__":
    run_scraping()
