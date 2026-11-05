#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IDF Transports Live — serveur local + collecteur automatique
=============================================================

Un seul fichier, aucune dépendance à installer (Python 3.9+).

  python idf_transports_live.py            -> lance le tableau de bord sur http://localhost:8765
  python idf_transports_live.py --once     -> fait UNE collecte puis s'arrête (mode GitHub Actions)

Ce que fait le serveur :
  1. Il sert l'appli (idf_transports_live.html, à placer dans le même dossier).
  2. Il relaie les appels vers l'API PRIM d'Île-de-France Mobilités (avec ta clé),
     vers Vélib' (GBFS) et vers le référentiel des lignes IDFM.
  3. Toutes les N minutes (10 par défaut), il collecte et historise :
       - l'état de chaque ligne (métro, RER, Transilien, tram, câble, bus),
       - les perturbations en cours,
       - les disponibilités Vélib' (global + par station).
     Stockage : base SQLite (idf_historique.db) + fichiers CSV dans le dossier data/.
  4. Il expose des liens CSV fixes : http://localhost:8765/export/lignes.csv, etc.
"""

import argparse
import csv
import gzip
import io
import json
import math
import os
import re
import sqlite3
import statistics
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

VERSION = "2.2"
DOSSIER = os.path.dirname(os.path.abspath(__file__))
FICHIER_CONFIG = os.path.join(DOSSIER, "config.json")
FICHIER_DB = os.path.join(DOSSIER, "idf_historique.db")
FICHIER_HTML = os.path.join(DOSSIER, "idf_transports_live.html")
FICHIER_REF = os.path.join(DOSSIER, "cache_referentiel_lignes.json")

PRIM = "https://prim.iledefrance-mobilites.fr/marketplace"
URL_ET = PRIM + "/estimated-timetable"
URL_PERTURBATIONS = PRIM + "/disruptions_bulk/disruptions/v2"
URL_STOP = PRIM + "/stop-monitoring"
URL_ITINERAIRE = PRIM + "/v2/navitia/journeys"
VELIB = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
ODS = "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets"
URL_REF_LIGNES = ODS + "/referentiel-des-lignes/exports/json"
URL_ARRETS_LIGNES = ODS + "/arrets-lignes/records"
GEOCODEURS = ["https://data.geopf.fr/geocodage/search", "https://api-adresse.data.gouv.fr/search/"]
ODS_CATALOGUE = "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets"
SNCF_PONCTUALITE = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/ponctualite-mensuelle-transilien/exports/json"
JOURS_FERIES = "https://calendrier.api.gouv.fr/jours-feries/metropole.json"
VACANCES = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-calendrier-scolaire/exports/json"
FICHIER_AFFLUENCE = os.path.join(DOSSIER, "cache_affluence.json")
FICHIER_PONCTUALITE = os.path.join(DOSSIER, "cache_ponctualite.json")

CONFIG_DEFAUT = {
    "prim_api_key": "v22vs5IvwMO7NO6dOBdxt2Qf03guEAl",
    "intervalle_min": 10,
    "port": 8765,
    "historiser_bus": "agrege",       # "agrege" (1 ligne par collecte), "lignes" (chaque ligne de bus), "non"
    "velib_stations_toutes_min": 10,  # fréquence d'historisation station par station
    "dossier_csv": "data",
}

# Niveaux SIRI d'occupation considérés comme « chargés » (voyageurs debout ou plein)
NIVEAUX_CHARGES = {"standingavailable", "standingroomonly", "crushedstandingroomonly", "full",
                   "notacceptingpassengers", "fewseatsavailable_standing", "high", "red"}

# Seuil (minutes) sous lequel une course dont le prochain arrêt est proche est considérée « en circulation »
SEUIL_CIRCULATION = {"metro": 4, "tram": 5, "bus": 6, "rer": 8, "train": 10, "cable": 5, "autre": 6}

VERROU_DB = threading.Lock()
ETAT = {
    "derniere_collecte": None, "prochaine_collecte": None, "en_cours": False,
    "live": None, "erreurs": [], "ref": {}, "ref_maj": None,
    "velib_cache": None, "velib_cache_ts": 0,
    "affluence": None, "affluence_etat": "non chargé", "ponctualite": None, "calendrier": None,
}


# ---------------------------------------------------------------- utilitaires

def maintenant_utc():
    return datetime.now(timezone.utc)


def iso_local(d=None):
    """Horodatage local (heure de la machine), format 2026-11-04 19:50:00."""
    d = d or maintenant_utc()
    return d.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def charger_config():
    cfg = dict(CONFIG_DEFAUT)
    if os.path.exists(FICHIER_CONFIG):
        try:
            with open(FICHIER_CONFIG, encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception:
            pass
    if os.environ.get("PRIM_API_KEY"):
        cfg["prim_api_key"] = os.environ["PRIM_API_KEY"].strip()
    return cfg


def sauver_config(cfg):
    with open(FICHIER_CONFIG, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


CONFIG = charger_config()


def http_get(url, params=None, cle=False, timeout=60, brut=False):
    if params:
        url = url + ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("Accept-Encoding", "gzip")
    req.add_header("User-Agent", "IDF-Transports-Live/" + VERSION)
    if cle:
        if not CONFIG.get("prim_api_key"):
            raise RuntimeError("Clé API PRIM absente : renseigne-la dans l'onglet Réglages.")
        req.add_header("apikey", CONFIG["prim_api_key"])
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            data = gzip.decompress(data)
    if brut:
        return data
    return json.loads(data.decode("utf-8"))


def val(x):
    if isinstance(x, dict):
        return x.get("value")
    if isinstance(x, list) and x:
        return val(x[0])
    return x


def parse_t(s):
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        s = re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], s)
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


def id_ligne(ref):
    """'STIF:Line::C01742:' ou 'line:IDFM:C01742' -> 'C01742'"""
    if not ref:
        return None
    m = re.search(r"(C\d{5})", str(ref))
    return m.group(1) if m else str(ref)


def nettoyer_html(t):
    t = re.sub(r"<br\s*/?>", "\n", t or "", flags=re.I)
    t = re.sub(r"<[^>]+>", "", t)
    for a, b in (("&nbsp;", " "), ("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"), ("&#39;", "'"), ("&quot;", '"')):
        t = t.replace(a, b)
    return re.sub(r"\n{3,}", "\n\n", t).strip()


def type_jour(d):
    wd = d.astimezone().weekday()
    return "semaine" if wd < 5 else ("samedi" if wd == 5 else "dimanche")


# ---------------------------------------------------------------- référentiel des lignes

def famille(mode, submode, short, network, name):
    mode = (mode or "").lower()
    sub = (submode or "").lower()
    short = (short or "").strip().upper()
    net = (network or "").lower()
    if mode == "metro":
        return "metro"
    if mode == "tram":
        return "tram"
    if mode in ("bus", "coach", "trolleybus"):
        return "bus"
    if mode in ("cableway", "funicular", "telecabin", "cable", "gondola") or "câble" in (name or "").lower():
        return "cable"
    if mode == "rail":
        if short in ("A", "B", "C", "D", "E") or "rer" in net or "rer" in sub:
            return "rer"
        return "train"
    return "autre"


def charger_referentiel(forcer=False):
    """Télécharge (1 fois par jour) le référentiel des lignes IDFM et le met en cache."""
    if not forcer and ETAT["ref"] and ETAT["ref_maj"] and time.time() - ETAT["ref_maj"] < 86400:
        return ETAT["ref"]
    brut = None
    if not forcer and os.path.exists(FICHIER_REF) and time.time() - os.path.getmtime(FICHIER_REF) < 86400:
        with open(FICHIER_REF, encoding="utf-8") as f:
            brut = json.load(f)
    if brut is None:
        try:
            brut = http_get(URL_REF_LIGNES, timeout=120)
            with open(FICHIER_REF, "w", encoding="utf-8") as f:
                json.dump(brut, f, ensure_ascii=False)
        except Exception as e:
            journal_erreur("Référentiel des lignes : " + str(e))
            if os.path.exists(FICHIER_REF):
                with open(FICHIER_REF, encoding="utf-8") as f:
                    brut = json.load(f)
            else:
                brut = []
    ref = {}
    for r in brut:
        r = {k.lower(): v for k, v in r.items()}
        lid = id_ligne(r.get("id_line"))
        if not lid:
            continue
        short = r.get("shortname_line") or r.get("name_line") or lid
        fam = famille(r.get("transportmode"), r.get("transportsubmode"), short,
                      r.get("networkname"), r.get("name_line"))
        ref[lid] = {
            "id": lid, "nom": r.get("name_line") or short, "court": short, "famille": fam,
            "mode": r.get("transportmode"), "sous_mode": r.get("transportsubmode"),
            "exploitant": r.get("operatorname"), "reseau": r.get("networkname"),
            "couleur": "#" + (r.get("colourweb_hexacolor") or "888888").lstrip("#"),
            "couleur_texte": "#" + (r.get("textcolourweb_hexacolor") or "FFFFFF").lstrip("#"),
            "statut": r.get("status"),
        }
    ETAT["ref"] = ref
    ETAT["ref_maj"] = time.time()
    return ref


def info_ligne(lid, nom_publie=None):
    ref = ETAT["ref"].get(lid)
    if ref:
        return ref
    return {"id": lid, "nom": nom_publie or lid, "court": nom_publie or lid, "famille": "autre",
            "exploitant": None, "reseau": None, "couleur": "#888888", "couleur_texte": "#FFFFFF"}


# ---------------------------------------------------------------- analyse des horaires temps réel (SIRI ET)

def analyser_et(data, now):
    """Agrège la requête globale estimated-timetable en indicateurs par ligne."""
    lignes = {}
    stats = {"courses_brutes": 0, "doublons": 0, "retards_aberrants": 0, "courses_perimees": 0,
             "courses_sans_horaire": 0, "lignes_inconnues": set()}
    vus = set()
    livraisons = (data.get("Siri", {}).get("ServiceDelivery", {}) or {}).get("EstimatedTimetableDelivery", []) or []
    fen_fin = now + timedelta(minutes=60)
    for liv in livraisons:
        for cadre in liv.get("EstimatedJourneyVersionFrame", []) or []:
            for vj in cadre.get("EstimatedVehicleJourney", []) or []:
                lid = id_ligne(val(vj.get("LineRef")))
                if not lid:
                    continue
                stats["courses_brutes"] += 1
                if lid not in ETAT["ref"]:
                    stats["lignes_inconnues"].add(lid)
                ref_course = val(vj.get("DatedVehicleJourneyRef")) or val((vj.get("FramedVehicleJourneyRef") or {}).get("DatedVehicleJourneyRef"))
                if ref_course:
                    if (lid, ref_course) in vus:          # même course publiée deux fois
                        stats["doublons"] += 1
                        continue
                    vus.add((lid, ref_course))
                enreg = parse_t(val(vj.get("RecordedAtTime")))
                if enreg and enreg < now - timedelta(minutes=30):
                    stats["courses_perimees"] += 1
                L = lignes.setdefault(lid, {"courses": 0, "rames": 0, "retards": [], "supprimees": 0,
                                            "passages": {}, "nom_publie": val(vj.get("PublishedLineName")),
                                            "occupation": {}})
                appels = (vj.get("EstimatedCalls") or {}).get("EstimatedCall") or []
                if isinstance(appels, dict):
                    appels = [appels]
                temps, retard_prochain, annule = [], None, bool(vj.get("Cancellation"))
                retard_vu = False
                occ = val(vj.get("Occupancy"))
                if not occ:
                    for c in appels:
                        occ = val(c.get("Occupancy")) or val(c.get("ExpectedDepartureOccupancy"))
                        if isinstance(occ, dict):
                            occ = occ.get("OccupancyLevel") or occ.get("Occupancy")
                        if occ:
                            break
                if occ:
                    occ = str(occ)
                    L["occupation"][occ] = L["occupation"].get(occ, 0) + 1
                direction = val(vj.get("DirectionName")) or val(vj.get("DirectionRef")) or ""
                for c in appels:
                    exp = parse_t(c.get("ExpectedDepartureTime") or c.get("ExpectedArrivalTime"))
                    aim = parse_t(c.get("AimedDepartureTime") or c.get("AimedArrivalTime"))
                    t = exp or aim
                    statut = (c.get("DepartureStatus") or c.get("ArrivalStatus") or "").lower()
                    if statut == "cancelled":
                        annule = True
                    if not t:
                        continue
                    temps.append(t)
                    if not retard_vu and t >= now and exp and aim:
                        retard_vu = True
                        retard_prochain = (exp - aim).total_seconds() / 60.0
                        if abs(retard_prochain) > 180:      # écart irréaliste (course d'un autre jour, erreur de saisie)
                            stats["retards_aberrants"] += 1
                            retard_prochain = None
                    if now <= t <= fen_fin and exp:
                        arret = val(c.get("StopPointRef")) or ""
                        L["passages"].setdefault((arret, direction), []).append(exp)
                if not temps:
                    stats["courses_sans_horaire"] += 1
                    continue
                tmin, tmax = min(temps), max(temps)
                if tmax < now - timedelta(minutes=1) or tmin > fen_fin:
                    continue
                L["courses"] += 1
                if annule:
                    L["supprimees"] += 1
                    continue
                fam = info_ligne(lid, L["nom_publie"])["famille"]
                if tmin <= now + timedelta(minutes=SEUIL_CIRCULATION.get(fam, 6)):
                    L["rames"] += 1
                L["actives"] = L.get("actives", 0) + 1
                if retard_prochain is not None:
                    L["retards"].append(max(retard_prochain, 0.0))
    resultat = {}
    for lid, L in lignes.items():
        intervalles = []
        for liste in L["passages"].values():
            liste.sort()
            if len(liste) >= 2:
                ecarts = [(b - a).total_seconds() / 60 for a, b in zip(liste, liste[1:]) if (b - a).total_seconds() > 20]
                if ecarts:
                    intervalles.append(statistics.median(ecarts))
        r = L["retards"]
        occ = L["occupation"]
        n_occ = sum(occ.values())
        charges = sum(v for k, v in occ.items() if k.lower() in NIVEAUX_CHARGES)
        resultat[lid] = {
            "nb_occupation_publiee": n_occ,
            "occupation_chargee_pct": round(100 * charges / n_occ, 1) if n_occ else None,
            "occupation_detail": occ,
            "courses": L["courses"], "rames": L["rames"], "supprimees": L["supprimees"],
            "retard_moyen_min": round(sum(r) / len(r), 2) if r else None,
            "retard_median_min": round(statistics.median(r), 2) if r else None,
            "pct_retard_5min": round(100 * sum(1 for x in r if x >= 5) / len(r), 1) if r else None,
            "nb_mesures_retard": len(r),
            "pct_heure_prevue": round(100 * len(r) / L["actives"], 1) if L.get("actives") else None,
            "intervalle_median_min": round(statistics.median(intervalles), 2) if intervalles else None,
            "nom_publie": L["nom_publie"],
        }
    stats["lignes_inconnues"] = sorted(stats["lignes_inconnues"])
    return resultat, stats


# ---------------------------------------------------------------- perturbations

def niveau_perturbation(d):
    sev = (d.get("severity") or "").upper()
    cause = (d.get("cause") or "").upper()
    if "BLOQ" in sev:
        return 3
    if "PERTURB" in sev or cause == "PERTURBATION":
        return 2
    if cause == "TRAVAUX":
        return 1
    return 0


LIBELLE_NIVEAU = {3: "interrompu", 2: "perturbé", 1: "travaux", 0: "info", -1: "normal"}


def analyser_perturbations(data, now):
    """Renvoie (par_ligne, liste_actives)."""
    perturb = {d.get("id"): d for d in data.get("disruptions", []) or []}
    actives = {}
    for pid, d in perturb.items():
        actif = False
        for p in d.get("applicationPeriods", []) or []:
            try:
                deb = datetime.strptime(p["begin"], "%Y%m%dT%H%M%S").astimezone()
                fin = datetime.strptime(p["end"], "%Y%m%dT%H%M%S").astimezone()
            except Exception:
                continue
            if deb <= now <= fin:
                actif = True
                d["_debut"], d["_fin"] = deb.strftime("%Y-%m-%d %H:%M"), fin.strftime("%Y-%m-%d %H:%M")
                break
        if actif:
            actives[pid] = d
    par_ligne = {}
    liens = {}
    for l in data.get("lines", []) or []:
        lid = id_ligne(l.get("id"))
        ids = set()
        for o in l.get("impactedObjects", []) or []:
            ids.update(o.get("disruptionIds", []) or [])
        for pid in ids:
            if pid in actives:
                par_ligne.setdefault(lid, []).append(pid)
                liens.setdefault(pid, set()).add(lid)
    liste = []
    for pid, d in actives.items():
        lids = sorted(liens.get(pid, []))
        liste.append({
            "id": pid, "lignes": lids,
            "familles": sorted({info_ligne(x)["famille"] for x in lids}),
            "niveau": niveau_perturbation(d), "gravite": LIBELLE_NIVEAU[niveau_perturbation(d)],
            "cause": d.get("cause"), "severite_brute": d.get("severity"),
            "titre": nettoyer_html(d.get("title")), "message": nettoyer_html(d.get("message"))[:1500],
            "debut": d.get("_debut"), "fin": d.get("_fin"), "maj": d.get("lastUpdate"),
        })
    liste.sort(key=lambda x: -x["niveau"])
    etat = {}
    for lid, pids in par_ligne.items():
        etat[lid] = {"niveau": max(niveau_perturbation(actives[p]) for p in pids), "ids": pids}
    return etat, liste


# ---------------------------------------------------------------- Vélib'

def lire_velib(forcer=False):
    if not forcer and ETAT["velib_cache"] and time.time() - ETAT["velib_cache_ts"] < 60:
        return ETAT["velib_cache"]
    info = http_get(VELIB + "/station_information.json", timeout=40)
    statut = http_get(VELIB + "/station_status.json", timeout=40)
    infos = {str(s["station_id"]): s for s in info.get("data", {}).get("stations", [])}
    stations = []
    for s in statut.get("data", {}).get("stations", []):
        sid = str(s.get("station_id"))
        i = infos.get(sid, {})
        meca = elec = 0
        for t in s.get("num_bikes_available_types", []) or []:
            meca += int(t.get("mechanical", 0) or 0)
            elec += int(t.get("ebike", 0) or 0)
        velos = int(s.get("num_bikes_available", meca + elec) or 0)
        places = int(s.get("num_docks_available", s.get("numDocksAvailable", 0)) or 0)
        cap = int(i.get("capacity", 0) or 0)
        en_service = bool(s.get("is_installed", 1)) and bool(s.get("is_renting", 1)) and bool(s.get("is_returning", 1))
        stations.append({
            "id": sid, "code": str(s.get("stationCode") or i.get("stationCode") or ""),
            "nom": i.get("name", sid), "lat": i.get("lat"), "lon": i.get("lon"), "capacite": cap,
            "velos": velos, "mecaniques": meca, "electriques": elec, "places": places,
            "en_service": en_service, "indispo": max(cap - velos - places, 0) if cap else None,
            "maj": s.get("last_reported"),
        })
    # contrôles de cohérence du flux
    ids_statut = {str(s.get("station_id")) for s in statut.get("data", {}).get("stations", [])}
    t_now = time.time()
    def age_h(x):
        try:
            return (t_now - float(x)) / 3600
        except (TypeError, ValueError):
            return None
    q = {
        "stations_statut": len(stations), "stations_info": len(infos),
        "sans_statut": sum(1 for k in infos if k not in ids_statut),
        "sans_position": sum(1 for x in stations if x["lat"] is None or x["lon"] is None),
        "figees_2h": sum(1 for x in stations if x["en_service"] and (age_h(x["maj"]) or 0) > 2),
        "depasse_capacite": sum(1 for x in stations if x["capacite"] and x["velos"] + x["places"] > x["capacite"]),
        "capacite_nulle": sum(1 for x in stations if not x["capacite"]),
        "types_incoherents": sum(1 for x in stations if x["mecaniques"] + x["electriques"] != x["velos"]),
        "age_flux_min": round((t_now - float(statut.get("lastUpdatedOther") or statut.get("last_updated") or t_now)) / 60, 1),
    }
    resultat = {"ts": iso_local(), "stations": stations, "qualite": q}
    ETAT["velib_cache"], ETAT["velib_cache_ts"] = resultat, time.time()
    return resultat


def agreger_velib(stations):
    actives = [s for s in stations if s["en_service"]]
    cap = sum(s["capacite"] for s in actives)
    velos = sum(s["velos"] for s in actives)
    return {
        "stations": len(stations), "stations_en_service": len(actives),
        "velos": velos, "mecaniques": sum(s["mecaniques"] for s in actives),
        "electriques": sum(s["electriques"] for s in actives),
        "places_libres": sum(s["places"] for s in actives), "capacite": cap,
        "stations_vides": sum(1 for s in actives if s["velos"] == 0),
        "stations_pleines": sum(1 for s in actives if s["places"] == 0),
        "taux_remplissage": round(100 * velos / cap, 1) if cap else None,
        "indispo_estimee": sum(s["indispo"] or 0 for s in actives),
        "pct_indispo": round(100 * sum(s["indispo"] or 0 for s in actives) / cap, 1) if cap else None,
    }


# ---------------------------------------------------------------- affluence estimée (validations IDFM)
#
# Il n'existe pas de taux d'occupation des rames en open data. On construit donc un modèle :
#   entrées estimées (station, heure) = validations journalières habituelles de la station (même type de jour)
#                                       × part de l'heure dans la journée (profil horaire publié par IDFM)
# puis on répartit les entrées de chaque station entre les lignes qui la desservent.

def _norm(s):
    import unicodedata
    s = unicodedata.normalize("NFD", str(s or "")).encode("ascii", "ignore").decode().upper()
    s = re.sub(r"\b(GARE|DE|DU|DES|LA|LE|LES|D|L|ST|SAINT|STE|SAINTE)\b", " ", s.replace("-", " ").replace("'", " "))
    return re.sub(r"[^A-Z0-9]+", "", s)


def _champ(champs, *motifs, exclure=()):
    for m in motifs:
        for c in champs:
            cl = c.lower()
            if m in cl and not any(e in cl for e in exclure):
                return c
    return None


def _nombre(v):
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", ".").replace(" ", "").replace(" ", "")
    if not s:
        return 0.0
    if "moins" in s.lower():          # « Moins de 5 » : petits volumes masqués par IDFM
        return 2.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def lambert93_vers_wgs84(x, y):
    """Conversion Lambert 93 (EPSG:2154) -> latitude/longitude WGS84 (précision métrique)."""
    n, c, xs, ys = 0.7256077650532670, 11754255.426096, 700000.0, 12655612.049876
    e = 0.08181919104281579
    lon0 = math.radians(3)
    dx, dy = x - xs, y - ys
    r = math.hypot(dx, dy)
    gamma = math.atan(dx / -dy)
    lon = lon0 + gamma / n
    lat_iso = -1 / n * math.log(abs(r / c))
    lat = 2 * math.atan(math.exp(lat_iso)) - math.pi / 2
    for _ in range(10):
        lat = 2 * math.atan(((1 + e * math.sin(lat)) / (1 - e * math.sin(lat))) ** (e / 2) * math.exp(lat_iso)) - math.pi / 2
    return math.degrees(lat), math.degrees(lon)


def dist_m(a_lat, a_lon, b_lat, b_lon):
    r = math.pi / 180
    x = math.sin((b_lat - a_lat) * r / 2) ** 2 + math.cos(a_lat * r) * math.cos(b_lat * r) * math.sin((b_lon - a_lon) * r / 2) ** 2
    return 2 * 6371000 * math.asin(math.sqrt(x))


def lire_csv_ods(url, params=None, timeout=300):
    brut = http_get(url, params, timeout=timeout, brut=True).decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(brut), delimiter=";"))


def jeux_ods(recherche):
    d = http_get(ODS_CATALOGUE, {"where": 'search("%s")' % recherche, "limit": "100"}, timeout=60)
    out = []
    for r in d.get("results", []):
        did = r.get("dataset_id")
        modif = (((r.get("metas") or {}).get("default") or {}).get("modified")) or ""
        out.append((did, modif))
    return out


def choisir_jeu_validations(jeux, motif):
    cands = [(d, m) for d, m in jeux if d and motif in d]
    if not cands:
        return None
    if motif.endswith("par-jour"):
        meilleur, date_max = None, ""
        for did, _ in cands:
            try:
                ex = http_get(ODS_CATALOGUE + "/%s/records" % did, {"limit": "1"}, timeout=40).get("results", [{}])[0]
                cj = _champ(ex.keys(), "jour", exclure=("cat",))
                m = http_get(ODS_CATALOGUE + "/%s/records" % did, {"select": "max(%s) as m" % cj}, timeout=60)["results"][0]["m"]
                if m and str(m) > date_max:
                    meilleur, date_max = did, str(m)
            except Exception:
                continue
        return meilleur or sorted(cands, key=lambda x: x[1])[-1][0]
    return sorted(cands, key=lambda x: x[1])[-1][0]


def charger_calendrier():
    """Jours fériés et vacances scolaires zone C (Paris, Créteil, Versailles)."""
    if ETAT["calendrier"] and time.time() - ETAT["calendrier"]["ts"] < 7 * 86400:
        return ETAT["calendrier"]
    cal = {"ts": time.time(), "feries": {}, "vacances": []}
    try:
        cal["feries"] = http_get(JOURS_FERIES, timeout=20)
    except Exception:
        pass
    try:
        for r in http_get(VACANCES, {"where": 'zones="Zone C" and end_date>="%s"' % (datetime.now().year - 1)}, timeout=40):
            if (r.get("population") or "-") in ("-", "Élèves", "Elèves", None) and r.get("start_date"):
                cal["vacances"].append((str(r["start_date"])[:10], str(r["end_date"])[:10], r.get("description")))
    except Exception:
        pass
    ETAT["calendrier"] = cal
    return cal


def categorie_jour(d):
    """Catégories IDFM : JOHV/JOVS (jour ouvré hors/en vacances), SAHV/SAVS, DIJFP (dimanche & férié)."""
    loc = d.astimezone()
    jour = loc.strftime("%Y-%m-%d")
    cal = ETAT["calendrier"] or {"feries": {}, "vacances": []}
    if loc.weekday() == 6 or jour in cal["feries"]:
        return "DIJFP"
    vac = any(a <= jour < b for a, b, _ in cal["vacances"])
    if loc.weekday() == 5:
        return "SAVS" if vac else "SAHV"
    return "JOVS" if vac else "JOHV"


TYPE_POUR_CAT = {"JOHV": "semaine", "JOVS": "semaine", "SAHV": "samedi", "SAVS": "samedi", "DIJFP": "dimanche"}


def construire_affluence():
    """Télécharge et agrège les validations. Lourd (~30-60 Mo) : fait au démarrage puis une fois par semaine."""
    ETAT["affluence_etat"] = "construction en cours"
    charger_referentiel()
    charger_calendrier()
    jeux = jeux_ods("validations")
    jeu_jour = choisir_jeu_validations(jeux, "validations-reseau-ferre-nombre-validations-par-jour")
    jeu_prof = choisir_jeu_validations(jeux, "validations-reseau-ferre-profils-horaires")
    if not jeu_jour:
        raise RuntimeError("jeu de validations du réseau ferré introuvable sur le portail IDFM")

    # 1) volumes journaliers par station
    lignes = lire_csv_ods(ODS_CATALOGUE + "/%s/exports/csv" % jeu_jour)
    ch = lignes[0].keys() if lignes else []
    c_jour, c_lda = _champ(ch, "jour", exclure=("cat",)), _champ(ch, "lda", "zdc", "zda")
    c_nom, c_nb = _champ(ch, "libelle", "nom"), _champ(ch, "nb_vald", "vald", "nb")
    par_station_jour, noms = {}, {}
    for r in lignes:
        k = str(r.get(c_lda) or "").strip()
        if not k or k in ("?", "ND"):
            k = "NOM:" + _norm(r.get(c_nom))
        noms.setdefault(k, r.get(c_nom))
        cle = (k, str(r.get(c_jour))[:10])
        par_station_jour[cle] = par_station_jour.get(cle, 0.0) + _nombre(r.get(c_nb))
    jours = sorted({j for _, j in par_station_jour})
    feries = (ETAT["calendrier"] or {}).get("feries", {})

    def type_de(j):
        try:
            d = datetime.strptime(j, "%Y-%m-%d")
        except ValueError:
            return None
        if d.weekday() == 6 or j in feries:
            return "dimanche"
        return "samedi" if d.weekday() == 5 else "semaine"
    series = {}
    for (k, j), v in par_station_jour.items():
        t = type_de(j)
        if t:
            series.setdefault(k, {}).setdefault(t, []).append(v)
    stations = {}
    for k, s in series.items():
        stations[k] = {"nom": noms.get(k), "jour": {t: round(statistics.median(v)) for t, v in s.items()}}

    # 2) profils horaires (part de chaque heure dans la journée)
    profils, profil_reseau = {}, {}
    if jeu_prof:
        try:
            pr = lire_csv_ods(ODS_CATALOGUE + "/%s/exports/csv" % jeu_prof)
            chp = pr[0].keys() if pr else []
            p_lda, p_cat = _champ(chp, "lda", "zdc", "zda"), _champ(chp, "cat_jour", "cat")
            p_tr, p_pct = _champ(chp, "trnc", "tranche", "horaire"), _champ(chp, "pourc", "pct", "part")
            p_nom = _champ(chp, "libelle", "nom")
            cumul = {}
            for r in pr:
                m = re.match(r"\s*(\d{1,2})", str(r.get(p_tr) or ""))
                if not m:
                    continue
                h = int(m.group(1)) % 24
                k = str(r.get(p_lda) or "").strip() or ("NOM:" + _norm(r.get(p_nom)))
                cat = str(r.get(p_cat) or "").upper()
                pct = _nombre(r.get(p_pct))
                profils.setdefault(k, {}).setdefault(cat, {})
                profils[k][cat][h] = profils[k][cat].get(h, 0) + pct
                cumul.setdefault(cat, {}).setdefault(h, []).append(pct)
            for cat, hs in cumul.items():
                tot = sum(statistics.mean(v) for v in hs.values()) or 1
                profil_reseau[cat] = {h: 100 * statistics.mean(v) / tot for h, v in hs.items()}
            for k in profils:                      # normalise chaque profil à 100 %
                for cat, hs in profils[k].items():
                    tot = sum(hs.values()) or 1
                    profils[k][cat] = {h: 100 * v / tot for h, v in hs.items()}
        except Exception as e:
            journal_erreur("Profils horaires : " + str(e))

    # 3) coordonnées des stations (référentiel des zones IDFM)
    coords = {}
    for jeu in ("zones-de-correspondance", "zones-d-arrets"):
        try:
            zs = lire_csv_ods(ODS_CATALOGUE + "/%s/exports/csv" % jeu)
        except Exception:
            continue
        if not zs:
            continue
        chz = zs[0].keys()
        z_id = _champ(chz, "zdcid", "zdaid", "id")
        z_nom = _champ(chz, "zdcname", "zdaname", "name", "nom")
        z_geo = _champ(chz, "geo_point", "geopoint", "point")
        z_x, z_y = _champ(chz, "xepsg2154", "x"), _champ(chz, "yepsg2154", "y")
        trouvees = {}
        for r in zs:
            lat = lon = None
            if z_geo and r.get(z_geo) and "," in r[z_geo]:
                lat, lon = [float(v) for v in r[z_geo].split(",")[:2]]
            elif z_x and z_y and r.get(z_x) and r.get(z_y):
                try:
                    lat, lon = lambert93_vers_wgs84(float(r[z_x]), float(r[z_y]))
                except Exception:
                    pass
            if lat is not None:
                trouvees[str(r.get(z_id)).strip()] = (lat, lon, r.get(z_nom))
                trouvees.setdefault("NOM:" + _norm(r.get(z_nom)), (lat, lon, r.get(z_nom)))
        if sum(1 for k in stations if k in trouvees) > len(coords):
            coords = trouvees
    for k, s in stations.items():
        c = coords.get(k) or coords.get("NOM:" + _norm(s["nom"]))
        if c:
            s["lat"], s["lon"] = round(c[0], 6), round(c[1], 6)

    # 4) lignes desservant chaque station (arrêts des lignes métro, RER, train, tram à moins de 300 m)
    ferre = {lid for lid, r in ETAT["ref"].items() if r["famille"] in ("metro", "rer", "train", "tram")}
    arrets = []
    try:
        al = lire_csv_ods(ODS_CATALOGUE + "/arrets-lignes/exports/csv")
        cha = al[0].keys() if al else []
        a_id = _champ(cha, "route_id") or _champ(cha, "id", exclure=("stop",))
        a_lat, a_lon = _champ(cha, "stop_lat"), _champ(cha, "stop_lon")
        a_nom = _champ(cha, "stop_name")
        for r in al:
            lid = id_ligne(r.get(a_id))
            if lid in ferre and r.get(a_lat) and r.get(a_lon):
                arrets.append((lid, float(r[a_lat]), float(r[a_lon]), _norm(r.get(a_nom))))
    except Exception as e:
        journal_erreur("Arrêts des lignes (affluence) : " + str(e))
    for k, s in stations.items():
        ls = set()
        if "lat" in s:
            for lid, la, lo, _ in arrets:
                if abs(la - s["lat"]) < 0.004 and abs(lo - s["lon"]) < 0.006 and dist_m(s["lat"], s["lon"], la, lo) <= 300:
                    ls.add(lid)
        if not ls:
            n = _norm(s["nom"])
            ls = {lid for lid, _, _, nm in arrets if nm and nm == n}
            if "lat" not in s:
                p = [(la, lo) for lid, la, lo, nm in arrets if nm == n]
                if p:
                    s["lat"], s["lon"] = p[0]
        s["lignes"] = sorted(ls)

    modele = {
        "construit_le": iso_local(), "jeu_jours": jeu_jour, "jeu_profils": jeu_prof,
        "periode": [jours[0], jours[-1]] if jours else None, "nb_jours": len(jours),
        "stations": stations, "profils": profils, "profil_reseau": profil_reseau,
        "stations_localisees": sum(1 for s in stations.values() if "lat" in s),
        "stations_rattachees": sum(1 for s in stations.values() if s.get("lignes")),
    }
    with open(FICHIER_AFFLUENCE, "w", encoding="utf-8") as f:
        json.dump(modele, f, ensure_ascii=False)
    return modele


def charger_affluence(forcer=False):
    try:
        modele = None
        if not forcer and os.path.exists(FICHIER_AFFLUENCE):
            with open(FICHIER_AFFLUENCE, encoding="utf-8") as f:
                modele = json.load(f)
            try:      # âge lu dans le fichier (la date du fichier n'est pas fiable après un git clone)
                age = (datetime.now() - datetime.strptime(modele["construit_le"], "%Y-%m-%d %H:%M:%S")).total_seconds()
            except Exception:
                age = 1e9
            if age > 7 * 86400:
                modele = None
        if modele is None:
            modele = construire_affluence()
        for st in modele.get("profils", {}).values():          # clés JSON -> entiers
            for cat in list(st):
                st[cat] = {int(h): v for h, v in st[cat].items()}
        modele["profil_reseau"] = {c: {int(h): v for h, v in hs.items()} for c, hs in modele.get("profil_reseau", {}).items()}
        ETAT["affluence"] = modele
        ETAT["affluence_etat"] = "prêt"
        print("[affluence] modèle prêt : %d stations (%d localisées, %d rattachées à des lignes), période %s" % (
            len(modele["stations"]), modele["stations_localisees"], modele["stations_rattachees"], modele.get("periode")))
    except Exception as e:
        ETAT["affluence_etat"] = "indisponible : " + str(e)
        journal_erreur("Affluence : " + str(e))


def part_heure(k, cat, h):
    m = ETAT["affluence"]
    p = (m["profils"].get(k) or {}).get(cat) or m["profil_reseau"].get(cat)
    if not p:                                  # catégorie absente : on se rabat sur le jour ouvré
        p = (m["profils"].get(k) or {}).get("JOHV") or m["profil_reseau"].get("JOHV") or {}
    return p.get(h, 0.0) / 100.0


def affluence_maintenant(now=None, heure=None):
    """Entrées estimées par station et par ligne pour l'heure courante + courbes de la journée type."""
    m = ETAT["affluence"]
    if not m:
        return None
    now = now or maintenant_utc()
    cat = categorie_jour(now)
    typ = TYPE_POUR_CAT[cat]
    h = now.astimezone().hour if heure is None else int(heure)
    stations, par_ligne, courbe = [], {}, [0.0] * 24
    for k, s in m["stations"].items():
        jour = s["jour"].get(typ) or s["jour"].get("semaine")
        if not jour:
            continue
        heures = [jour * part_heure(k, cat, x) for x in range(24)]
        for x in range(24):
            courbe[x] += heures[x]
        e = heures[h]
        pic = max(heures) or 1
        stations.append({"id": k, "nom": s["nom"], "lat": s.get("lat"), "lon": s.get("lon"), "lignes": s.get("lignes", []),
                         "entrees_h": round(e), "entrees_jour": round(jour), "pct_pic": round(100 * e / pic)})
        ls = s.get("lignes") or []
        for lid in ls:
            L = par_ligne.setdefault(lid, [0.0] * 24)
            for x in range(24):
                L[x] += heures[x] / len(ls)
    lignes = {}
    for lid, hs in par_ligne.items():
        pic = max(hs) or 1
        lignes[lid] = {"entrees_h": round(hs[h]), "pct_pic": round(100 * hs[h] / pic), "heure_pic": hs.index(max(hs)),
                       "entrees_jour": round(sum(hs)), "courbe": [round(v) for v in hs]}
    stations.sort(key=lambda s: -s["entrees_h"])
    return {"categorie_jour": cat, "type_jour": typ, "heure": h, "stations": stations, "lignes": lignes,
            "courbe_reseau": [round(v) for v in courbe], "total_h": round(courbe[h]), "total_jour": round(sum(courbe)),
            "modele": {k: m.get(k) for k in ("construit_le", "jeu_jours", "jeu_profils", "periode", "nb_jours",
                                            "stations_localisees", "stations_rattachees")} | {"nb_stations": len(m["stations"])}}


# ---------------------------------------------------------------- ponctualité officielle SNCF (mensuelle)

def charger_ponctualite(forcer=False):
    try:
        if not forcer and os.path.exists(FICHIER_PONCTUALITE) and time.time() - os.path.getmtime(FICHIER_PONCTUALITE) < 86400:
            with open(FICHIER_PONCTUALITE, encoding="utf-8") as f:
                ETAT["ponctualite"] = json.load(f)
            return ETAT["ponctualite"]
        brut = http_get(SNCF_PONCTUALITE, timeout=60)
        if not brut:
            return None
        ch = brut[0].keys()
        c_date = _champ(ch, "date", "mois")
        c_ligne = _champ(ch, "ligne", exclure=("nom",)) or _champ(ch, "ligne")
        c_nom = _champ(ch, "nom_de_la_ligne", "nom")
        c_taux = _champ(ch, "taux", "ponctualite", "regularite")
        c_serv = _champ(ch, "service")
        out = {}
        for r in brut:
            l = str(r.get(c_ligne) or "").strip().upper().replace("RER ", "").replace("LIGNE ", "")
            if not l:
                continue
            out.setdefault(l, []).append({"mois": str(r.get(c_date))[:7], "taux": _nombre(r.get(c_taux)) if r.get(c_taux) not in (None, "") else None,
                                          "nom": r.get(c_nom), "service": r.get(c_serv)})
        for l in out:
            out[l] = sorted(out[l], key=lambda x: x["mois"])[-24:]
        ETAT["ponctualite"] = out
        with open(FICHIER_PONCTUALITE, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False)
        return out
    except Exception as e:
        journal_erreur("Ponctualité officielle SNCF : " + str(e))
        return ETAT["ponctualite"]


def boucle_donnees_lentes():
    """Données de référence lourdes, rafraîchies en arrière-plan sans bloquer la collecte."""
    while True:
        charger_calendrier()
        charger_ponctualite()
        charger_affluence()
        time.sleep(6 * 3600)


# ---------------------------------------------------------------- base de données

SCHEMA = """
CREATE TABLE IF NOT EXISTS collectes(ts TEXT, ok_horaires INT, ok_perturbations INT, ok_velib INT, duree_s REAL, erreurs TEXT);
CREATE TABLE IF NOT EXISTS lignes(ts TEXT, type_jour TEXT, heure INT, id_ligne TEXT, ligne TEXT, famille TEXT, exploitant TEXT,
  rames INT, courses INT, supprimees INT, retard_moyen_min REAL, retard_median_min REAL, pct_retard_5min REAL,
  intervalle_median_min REAL, indice_dispo REAL, etat TEXT, nb_perturbations INT,
  entrees_estimees_h INT, affluence_pct_pic REAL, entrees_par_rame REAL, occupation_chargee_pct REAL, nb_occupation_publiee INT);
CREATE INDEX IF NOT EXISTS ix_lignes ON lignes(id_ligne, ts);
CREATE INDEX IF NOT EXISTS ix_lignes_ts ON lignes(ts);
CREATE TABLE IF NOT EXISTS bus_agrege(ts TEXT, lignes_suivies INT, bus_en_circulation INT, courses INT, supprimees INT,
  retard_moyen_min REAL, pct_retard_5min REAL, lignes_perturbees INT);
CREATE TABLE IF NOT EXISTS perturbations(ts TEXT, id_perturbation TEXT, id_ligne TEXT, ligne TEXT, famille TEXT,
  gravite TEXT, cause TEXT, titre TEXT, debut TEXT, fin TEXT);
CREATE INDEX IF NOT EXISTS ix_pert ON perturbations(id_perturbation);
CREATE TABLE IF NOT EXISTS velib_global(ts TEXT, stations INT, stations_en_service INT, velos INT, mecaniques INT, electriques INT,
  places_libres INT, capacite INT, stations_vides INT, stations_pleines INT, taux_remplissage REAL, indispo_estimee INT, pct_indispo REAL);
CREATE TABLE IF NOT EXISTS velib_stations(ts TEXT, station_id TEXT, code TEXT, nom TEXT, velos INT, mecaniques INT,
  electriques INT, places INT, capacite INT, en_service INT, indispo INT);
CREATE INDEX IF NOT EXISTS ix_vs ON velib_stations(station_id, ts);
CREATE TABLE IF NOT EXISTS qualite(ts TEXT, source TEXT, controle TEXT, valeur REAL, statut TEXT, detail TEXT);
CREATE INDEX IF NOT EXISTS ix_q ON qualite(ts);
"""

COLONNES = {
    "lignes": ["ts", "type_jour", "heure", "id_ligne", "ligne", "famille", "exploitant", "rames", "courses", "supprimees",
               "retard_moyen_min", "retard_median_min", "pct_retard_5min", "intervalle_median_min", "indice_dispo",
               "etat", "nb_perturbations", "entrees_estimees_h", "affluence_pct_pic", "entrees_par_rame",
               "occupation_chargee_pct", "nb_occupation_publiee"],
    "bus_agrege": ["ts", "lignes_suivies", "bus_en_circulation", "courses", "supprimees", "retard_moyen_min",
                   "pct_retard_5min", "lignes_perturbees"],
    "perturbations": ["ts", "id_perturbation", "id_ligne", "ligne", "famille", "gravite", "cause", "titre", "debut", "fin"],
    "velib_global": ["ts", "stations", "stations_en_service", "velos", "mecaniques", "electriques", "places_libres",
                     "capacite", "stations_vides", "stations_pleines", "taux_remplissage", "indispo_estimee", "pct_indispo"],
    "velib_stations": ["ts", "station_id", "code", "nom", "velos", "mecaniques", "electriques", "places", "capacite",
                       "en_service", "indispo"],
    "collectes": ["ts", "ok_horaires", "ok_perturbations", "ok_velib", "duree_s", "erreurs"],
    "qualite": ["ts", "source", "controle", "valeur", "statut", "detail"],
}


def db():
    con = sqlite3.connect(FICHIER_DB, timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    with VERROU_DB:
        con = db()
        con.executescript(SCHEMA)
        for table, cols in COLONNES.items():            # ajoute les colonnes apparues dans une nouvelle version
            existantes = {r[1] for r in con.execute("PRAGMA table_info(%s)" % table)}
            for c in cols:
                if c not in existantes:
                    con.execute("ALTER TABLE %s ADD COLUMN %s" % (table, c))
        con.commit()
        con.close()


def inserer(con, table, lignes):
    if not lignes:
        return
    cols = COLONNES[table]
    con.executemany("INSERT INTO %s(%s) VALUES(%s)" % (table, ",".join(cols), ",".join("?" * len(cols))),
                    [[r.get(c) for c in cols] for r in lignes])


def ecrire_csv(table, lignes, ts_utc, sous_dossier_jour=False):
    """Copie CSV (séparateur virgule, UTF-8) partitionnée par mois, ou par jour pour les stations Vélib'."""
    if not lignes:
        return
    base = os.path.join(DOSSIER, CONFIG.get("dossier_csv", "data"))
    loc = ts_utc.astimezone()
    if sous_dossier_jour:
        chemin = os.path.join(base, table, loc.strftime("%Y-%m-%d") + ".csv")
    else:
        chemin = os.path.join(base, loc.strftime("%Y-%m"), table + ".csv")
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    nouveau = not os.path.exists(chemin)
    cols = COLONNES[table]
    if not nouveau:
        with open(chemin, encoding="utf-8") as f:
            entete = next(csv.reader(f), [])
        if entete != cols:                     # nouvelle version : on réécrit le fichier avec les nouvelles colonnes
            with open(chemin, encoding="utf-8") as f:
                anciennes = list(csv.DictReader(f))
            with open(chemin, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(cols)
                for r in anciennes:
                    w.writerow([r.get(c, "") for c in cols])
    with open(chemin, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if nouveau:
            w.writerow(cols)
        for r in lignes:
            w.writerow(["" if r.get(c) is None else r.get(c) for c in cols])


def references_dispo(con):
    """Pour l'indice de disponibilité : 90e centile des rames observées par ligne / type de jour / heure, sur 28 jours."""
    limite = (maintenant_utc() - timedelta(days=28)).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    refs = {}
    try:
        for r in con.execute("SELECT id_ligne, type_jour, heure, rames FROM lignes WHERE ts >= ? AND rames IS NOT NULL", (limite,)):
            refs.setdefault((r[0], r[1], r[2]), []).append(r[3])
    except sqlite3.Error:
        return {}
    out = {}
    for k, v in refs.items():
        if len(v) >= 3:
            v.sort()
            out[k] = v[min(len(v) - 1, int(round(0.9 * (len(v) - 1))))]
    return out


# ---------------------------------------------------------------- contrôles qualité

def controles_qualite(now, ts, ok_h, horaires, stats_et, ok_p, liste_pert, velib, velib_g, con):
    """Une ligne par contrôle : statut ok / attention / alerte / info, avec la valeur mesurée et un détail lisible."""
    out = []
    loc = now.astimezone()
    nuit = (loc.hour == 1 and loc.minute >= 30) or 2 <= loc.hour < 5

    def add(source, controle, valeur, statut, detail=""):
        out.append({"ts": ts, "source": source, "controle": controle,
                    "valeur": None if valeur is None else round(float(valeur), 2), "statut": statut, "detail": detail})

    def palier(v, ok, att, haut_bon=True):
        if v is None:
            return "info"
        if haut_bon:
            return "ok" if v >= ok else ("attention" if v >= att else "alerte")
        return "ok" if v <= ok else ("attention" if v <= att else "alerte")

    # --- horaires temps réel
    add("Horaires", "Collecte réussie", 100 if ok_h else 0, "ok" if ok_h else "alerte",
        "" if ok_h else "le flux estimated-timetable n'a pas répondu (clé, quota ou panne PRIM)")
    if ok_h:
        actives = {lid for lid, h in horaires.items() if (h.get("courses") or 0) > 0}
        for fam, nom, seuils in (("metro", "métro", (95, 80)), ("rer", "RER", (95, 80)), ("tram", "tram", (90, 70)),
                                 ("train", "Transilien/train", (70, 40))):
            attendues = {lid: r for lid, r in ETAT["ref"].items() if r["famille"] == fam
                         and str(r.get("statut") or "active").lower() in ("active", "")}
            if not attendues:
                continue
            vues = [lid for lid in attendues if lid in actives]
            pct = 100 * len(vues) / len(attendues)
            manquantes = sorted(attendues[l]["court"] for l in attendues if l not in actives)
            add("Horaires", "Couverture " + nom, pct, "info" if nuit else palier(pct, *seuils),
                ("lignes sans donnée : " + ", ".join(manquantes[:20]) + (" …" if len(manquantes) > 20 else "")) if manquantes else "toutes les lignes ont des données")
        for fam, nom in (("rer", "RER"), ("train", "Transilien/train"), ("tram", "tram"), ("bus", "bus"), ("metro", "métro")):
            vals = [h["pct_heure_prevue"] for lid, h in horaires.items() if h.get("pct_heure_prevue") is not None
                    and info_ligne(lid)["famille"] == fam]
            if vals:
                v = statistics.mean(vals)
                add("Horaires", "Retard calculable (" + nom + ")", v,
                    "info" if fam == "metro" else palier(v, 70, 30),
                    "part des courses publiant l'heure prévue ET l'heure estimée ; en dessous, le retard moyen repose sur peu de courses")
        brutes = stats_et.get("courses_brutes") or 0
        if brutes:
            d = 100 * stats_et.get("doublons", 0) / brutes
            add("Horaires", "Courses en double", d, palier(d, 0.5, 5, haut_bon=False),
                "%d doublons écartés sur %d courses reçues" % (stats_et.get("doublons", 0), brutes))
            p_ = 100 * stats_et.get("courses_perimees", 0) / brutes
            add("Horaires", "Courses non mises à jour depuis 30 min", p_, palier(p_, 5, 20, haut_bon=False),
                "horodatage d'enregistrement ancien : le temps réel de ces courses est peut-être figé")
            add("Horaires", "Retards aberrants écartés", stats_et.get("retards_aberrants", 0),
                "ok" if stats_et.get("retards_aberrants", 0) <= max(5, brutes * 0.002) else "attention",
                "écarts prévu/estimé de plus de 3 h, exclus des moyennes")
        inc = stats_et.get("lignes_inconnues") or []
        add("Horaires", "Lignes absentes du référentiel", len(inc), "ok" if not inc else "attention",
            ", ".join(inc[:15]) if inc else "")
        if 7 <= loc.hour < 21:
            sans = []
            for lid, r in ETAT["ref"].items():
                if r["famille"] == "metro" and lid in horaires and not horaires[lid].get("rames"):
                    sans.append(r["court"])
            add("Horaires", "Lignes de métro sans rame en journée", len(sans), "ok" if not sans else "attention",
                ("à vérifier (interruption réelle ou trou de données) : " + ", ".join(sans)) if sans else "")

    # --- perturbations
    add("Perturbations", "Collecte réussie", 100 if ok_p else 0, "ok" if ok_p else "alerte")
    if ok_p:
        orphelines = sum(1 for x in liste_pert if not x["lignes"])
        add("Perturbations", "Perturbations sans ligne rattachée", orphelines, "ok" if orphelines <= 3 else "attention",
            "elles apparaissent dans la liste mais ne colorent aucune ligne")
        sans_titre = sum(1 for x in liste_pert if not x["titre"])
        add("Perturbations", "Perturbations sans titre", sans_titre, "ok" if not sans_titre else "attention")

    # --- Vélib'
    add("Vélib'", "Collecte réussie", 100 if velib_g else 0, "ok" if velib_g else "alerte")
    if velib and velib.get("qualite"):
        q = velib["qualite"]
        n = max(q["stations_statut"], 1)
        add("Vélib'", "Âge du flux (min)", q["age_flux_min"], palier(q["age_flux_min"], 5, 30, haut_bon=False))
        f = 100 * q["figees_2h"] / n
        add("Vélib'", "Stations figées depuis plus de 2 h", f, palier(f, 3, 10, haut_bon=False),
            "%d stations en service n'ont rien remonté depuis 2 h (borne déconnectée ?)" % q["figees_2h"])
        add("Vélib'", "Vélos + places > capacité", q["depasse_capacite"], "ok" if q["depasse_capacite"] <= n * 0.02 else "attention",
            "l'indicateur « indisponibles » vaut alors 0 pour ces stations")
        add("Vélib'", "Stations sans position", q["sans_position"], "ok" if not q["sans_position"] else "attention")
        add("Vélib'", "Stations sans état (info seule)", q["sans_statut"], "ok" if q["sans_statut"] <= 10 else "attention")
        add("Vélib'", "Total par type ≠ total vélos", q["types_incoherents"], "ok" if q["types_incoherents"] <= n * 0.01 else "attention")
        if con is not None and velib_g:
            try:
                hist = [r[0] for r in con.execute(
                    "SELECT velos FROM velib_global WHERE ts >= ? AND substr(ts,12,2) = ?",
                    (depuis(7), loc.strftime("%H")))]
                if len(hist) >= 5:
                    med = statistics.median(hist)
                    ecart = 100 * (velib_g["velos"] - med) / med if med else 0
                    add("Vélib'", "Écart du total de vélos vs habitude (%)", ecart, palier(abs(ecart), 25, 50, haut_bon=False),
                        "comparé à la médiane des 7 derniers jours à la même heure (%d vélos)" % med)
            except sqlite3.Error:
                pass

    # --- modèle d'affluence
    m = ETAT["affluence"]
    if m:
        ns = max(len(m["stations"]), 1)
        add("Affluence", "Stations localisées (%)", 100 * m["stations_localisees"] / ns, palier(100 * m["stations_localisees"] / ns, 90, 70))
        add("Affluence", "Stations rattachées à une ligne (%)", 100 * m["stations_rattachees"] / ns, palier(100 * m["stations_rattachees"] / ns, 85, 60),
            "les stations non rattachées ne comptent dans aucune ligne")
        add("Affluence", "Jours de validations dans le modèle", m.get("nb_jours"), palier(m.get("nb_jours"), 60, 20))
        try:
            age = (datetime.now() - datetime.strptime(m["periode"][1], "%Y-%m-%d")).days
            add("Affluence", "Ancienneté des validations (jours)", age, palier(age, 200, 320, haut_bon=False),
                "IDFM publie avec environ un semestre de décalage")
        except Exception:
            pass
    else:
        add("Affluence", "Modèle disponible", 0, "attention", ETAT["affluence_etat"])

    # --- ponctualité officielle
    if ETAT["ponctualite"]:
        dernier = max((x["mois"] for v in ETAT["ponctualite"].values() for x in v), default=None)
        if dernier:
            try:
                retard_mois = (loc.year - int(dernier[:4])) * 12 + loc.month - int(dernier[5:7])
                add("Ponctualité SNCF", "Dernier mois publié (mois de retard)", retard_mois, palier(retard_mois, 3, 6, haut_bon=False), dernier)
            except ValueError:
                pass

    # --- continuité de l'historique
    if con is not None:
        try:
            pas = max(int(CONFIG.get("intervalle_min", 10)), 2)
            tss = [datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S") for r in con.execute(
                "SELECT ts FROM collectes WHERE ts >= ? ORDER BY ts", (depuis(1),))]
            premier = min(tss) if tss else None
            if premier:
                fenetre = max(min(24 * 60, (datetime.now() - premier).total_seconds() / 60), pas)
                bornes = tss + [datetime.now()]
                trous = [(a, b) for a, b in zip(bornes, bornes[1:]) if (b - a).total_seconds() > 2.5 * pas * 60]
                manquant = sum((b - a).total_seconds() / 60 - pas for a, b in trous)
                pct = max(0.0, 100 * (1 - manquant / fenetre))
                plus_long = max(((b - a).total_seconds() / 60 for a, b in trous), default=0)
                add("Historique", "Complétude des 24 dernières heures (%)", pct, palier(pct, 95, 80),
                    "%d trou(s), le plus long : %d min" % (len(trous), plus_long) if trous else "aucun trou")
        except (sqlite3.Error, ValueError):
            pass
    return out


# ---------------------------------------------------------------- collecte

def journal_erreur(msg):
    ETAT["erreurs"] = ([{"ts": iso_local(), "message": msg}] + ETAT["erreurs"])[:30]
    print("[ERREUR]", msg, file=sys.stderr)


def collecter(mode_once=False):
    if ETAT["en_cours"]:
        return ETAT["live"]
    ETAT["en_cours"] = True
    debut = time.time()
    now = maintenant_utc()
    ts = iso_local(now)
    tj, heure = type_jour(now), now.astimezone().hour
    erreurs = []
    ok_h = ok_p = ok_v = 0
    try:
        charger_referentiel()
        horaires, etat_pert, liste_pert, stats_et = {}, {}, [], {}
        try:
            horaires, stats_et = analyser_et(http_get(URL_ET, cle=True, timeout=180), now)
            ok_h = 1
        except Exception as e:
            erreurs.append("Horaires temps réel : " + str(e))
        try:
            etat_pert, liste_pert = analyser_perturbations(http_get(URL_PERTURBATIONS, cle=True, timeout=90), now)
            ok_p = 1
        except Exception as e:
            erreurs.append("Perturbations : " + str(e))
        velib, velib_g = None, None
        try:
            velib = lire_velib(forcer=True)
            velib_g = agreger_velib(velib["stations"])
            velib_g["ts"] = ts
            ok_v = 1
        except Exception as e:
            erreurs.append("Vélib' : " + str(e))

        con = None
        refs = {}
        if not mode_once:
            with VERROU_DB:
                con = db()
                refs = references_dispo(con)
        # une ligne = union des lignes vues dans les horaires et les perturbations
        ids = set(horaires) | set(etat_pert)
        lignes_live = []
        aff = None
        try:
            aff = affluence_maintenant(now)
        except Exception as e:
            erreurs.append("Affluence : " + str(e))
        aff_l = (aff or {}).get("lignes", {})
        for lid in ids:
            h = horaires.get(lid, {})
            a = aff_l.get(lid, {})
            inf = info_ligne(lid, h.get("nom_publie"))
            p = etat_pert.get(lid, {})
            ref = refs.get((lid, tj, heure))
            indice = round(100 * h["rames"] / ref, 1) if ref and h.get("rames") is not None else None
            lignes_live.append({
                "ts": ts, "type_jour": tj, "heure": heure, "id_ligne": lid, "ligne": inf["court"],
                "nom": inf["nom"], "famille": inf["famille"], "exploitant": inf.get("exploitant"),
                "reseau": inf.get("reseau"), "couleur": inf.get("couleur"), "couleur_texte": inf.get("couleur_texte"),
                "rames": h.get("rames"), "courses": h.get("courses"), "supprimees": h.get("supprimees"),
                "retard_moyen_min": h.get("retard_moyen_min"), "retard_median_min": h.get("retard_median_min"),
                "pct_retard_5min": h.get("pct_retard_5min"), "intervalle_median_min": h.get("intervalle_median_min"),
                "indice_dispo": indice, "reference_rames": ref,
                "etat": LIBELLE_NIVEAU[p.get("niveau", -1)], "niveau": p.get("niveau", -1),
                "nb_perturbations": len(p.get("ids", [])), "perturbations": p.get("ids", []),
                "entrees_estimees_h": a.get("entrees_h"), "affluence_pct_pic": a.get("pct_pic"),
                "heure_pic": a.get("heure_pic"),
                "entrees_par_rame": round(a["entrees_h"] / h["rames"]) if a.get("entrees_h") is not None and h.get("rames") else None,
                "occupation_chargee_pct": h.get("occupation_chargee_pct"),
                "nb_occupation_publiee": h.get("nb_occupation_publiee"),
                "occupation_detail": h.get("occupation_detail"),
            })
        lignes_live.sort(key=lambda r: (r["famille"], r["ligne"] or ""))

        bus = [r for r in lignes_live if r["famille"] == "bus"]
        rb = [r for r in bus if r["retard_moyen_min"] is not None]
        bus_g = {
            "ts": ts, "lignes_suivies": len(bus),
            "bus_en_circulation": sum(r["rames"] or 0 for r in bus),
            "courses": sum(r["courses"] or 0 for r in bus), "supprimees": sum(r["supprimees"] or 0 for r in bus),
            "retard_moyen_min": round(sum(r["retard_moyen_min"] for r in rb) / len(rb), 2) if rb else None,
            "pct_retard_5min": round(sum(r["pct_retard_5min"] or 0 for r in rb) / len(rb), 1) if rb else None,
            "lignes_perturbees": sum(1 for r in bus if r["niveau"] >= 2),
        }

        mode_bus = CONFIG.get("historiser_bus", "agrege")
        a_historiser = [r for r in lignes_live if r["famille"] != "bus" or mode_bus == "lignes"]
        pert_rows = []
        for pz in liste_pert:
            for lid in pz["lignes"] or [""]:
                inf = info_ligne(lid) if lid else {"court": "", "famille": ""}
                pert_rows.append({"ts": ts, "id_perturbation": pz["id"], "id_ligne": lid, "ligne": inf["court"],
                                  "famille": inf["famille"], "gravite": pz["gravite"], "cause": pz["cause"],
                                  "titre": pz["titre"], "debut": pz["debut"], "fin": pz["fin"]})
        stations_rows = []
        pas = int(CONFIG.get("velib_stations_toutes_min", 10) or 10)
        minutes = now.astimezone().hour * 60 + now.astimezone().minute
        if velib and minutes % pas < int(CONFIG.get("intervalle_min", 10)):
            stations_rows = [{"ts": ts, "station_id": s["id"], "code": s["code"], "nom": s["nom"], "velos": s["velos"],
                              "mecaniques": s["mecaniques"], "electriques": s["electriques"], "places": s["places"],
                              "capacite": s["capacite"], "en_service": int(s["en_service"]), "indispo": s["indispo"]}
                             for s in velib["stations"]]
        run = {"ts": ts, "ok_horaires": ok_h, "ok_perturbations": ok_p, "ok_velib": ok_v,
               "duree_s": round(time.time() - debut, 1), "erreurs": " | ".join(erreurs)}

        if ok_h or ok_p:
            ecrire_csv("lignes", a_historiser, now)
            if mode_bus != "non" and ok_h:
                ecrire_csv("bus_agrege", [bus_g], now)
            ecrire_csv("perturbations", pert_rows, now)
        if velib_g:
            ecrire_csv("velib_global", [velib_g], now)
            ecrire_csv("velib_stations", stations_rows, now, sous_dossier_jour=True)
        ecrire_csv("collectes", [run], now)
        if con is not None:
            with VERROU_DB:
                qrows = controles_qualite(now, ts, ok_h, horaires, stats_et, ok_p, liste_pert, velib, velib_g, con)
        else:
            qrows = controles_qualite(now, ts, ok_h, horaires, stats_et, ok_p, liste_pert, velib, velib_g, None)
        ecrire_csv("qualite", qrows, now)

        if con is not None:
            with VERROU_DB:
                if ok_h or ok_p:
                    inserer(con, "lignes", a_historiser)
                    if mode_bus != "non" and ok_h:
                        inserer(con, "bus_agrege", [bus_g])
                    inserer(con, "perturbations", pert_rows)
                if velib_g:
                    inserer(con, "velib_global", [velib_g])
                    inserer(con, "velib_stations", stations_rows)
                inserer(con, "collectes", [run])
                inserer(con, "qualite", qrows)
                con.commit()
                con.close()

        for e in erreurs:
            journal_erreur(e)
        live = {"ts": ts, "lignes": lignes_live, "bus_agrege": bus_g, "perturbations": liste_pert,
                "affluence": {k: aff[k] for k in ("categorie_jour", "type_jour", "heure", "total_h", "total_jour")} if aff else None,
                "velib": velib_g, "erreurs": erreurs, "qualite": qrows, "ok": {"horaires": ok_h, "perturbations": ok_p, "velib": ok_v}}
        ETAT["live"] = live
        ETAT["derniere_collecte"] = ts
        print("[%s] collecte : %d lignes, %d perturbations, Vélib' %s, %.1f s%s" % (
            ts, len(lignes_live), len(liste_pert), "ok" if ok_v else "KO", time.time() - debut,
            (" — " + " | ".join(erreurs)) if erreurs else ""))
        return live
    except Exception as e:
        journal_erreur("Collecte : %s\n%s" % (e, traceback.format_exc()))
        return ETAT["live"]
    finally:
        ETAT["en_cours"] = False


def boucle_collecte():
    while True:
        collecter()
        pas = max(int(CONFIG.get("intervalle_min", 10)), 2) * 60
        attente = pas - (time.time() % pas) + 5   # aligné sur :00, :10, :20…
        ETAT["prochaine_collecte"] = iso_local(maintenant_utc() + timedelta(seconds=attente))
        time.sleep(attente)


# ---------------------------------------------------------------- requêtes historiques pour l'interface

def requete(sql, params=()):
    with VERROU_DB:
        con = db()
        try:
            return [dict(r) for r in con.execute(sql, params)]
        finally:
            con.close()


def depuis(jours):
    return (maintenant_utc() - timedelta(days=float(jours))).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def historique(params):
    serie = params.get("serie", "familles")
    jours = params.get("jours", "7")
    d = depuis(jours)
    if serie == "ligne":
        return requete("SELECT ts, rames, courses, supprimees, retard_moyen_min, pct_retard_5min, intervalle_median_min, "
                       "indice_dispo, etat, nb_perturbations, entrees_estimees_h, affluence_pct_pic, occupation_chargee_pct "
                       "FROM lignes WHERE id_ligne=? AND ts>=? ORDER BY ts",
                       (params.get("id", ""), d))
    if serie == "familles":
        return requete("SELECT ts, famille, SUM(rames) rames, SUM(supprimees) supprimees, AVG(retard_moyen_min) retard_moyen_min, "
                       "AVG(pct_retard_5min) pct_retard_5min, SUM(CASE WHEN etat IN ('perturbé','interrompu') THEN 1 ELSE 0 END) lignes_perturbees "
                       "FROM lignes WHERE ts>=? GROUP BY ts, famille ORDER BY ts", (d,))
    if serie == "bus":
        return requete("SELECT * FROM bus_agrege WHERE ts>=? ORDER BY ts", (d,))
    if serie == "velib":
        return requete("SELECT * FROM velib_global WHERE ts>=? ORDER BY ts", (d,))
    if serie == "station":
        return requete("SELECT ts, velos, mecaniques, electriques, places, capacite FROM velib_stations "
                       "WHERE station_id=? AND ts>=? ORDER BY ts", (params.get("id", ""), d))
    if serie == "perturbations":
        return requete("SELECT id_perturbation, MIN(ts) premiere_vue, MAX(ts) derniere_vue, COUNT(DISTINCT ts) nb_releves, "
                       "GROUP_CONCAT(DISTINCT ligne) lignes, GROUP_CONCAT(DISTINCT famille) familles, MAX(gravite) gravite, "
                       "MAX(cause) cause, MAX(titre) titre FROM perturbations WHERE ts>=? "
                       "GROUP BY id_perturbation ORDER BY premiere_vue DESC LIMIT 500", (d,))
    if serie == "classement":
        return requete("SELECT ligne, famille, id_ligne, COUNT(DISTINCT id_perturbation) incidents, "
                       "COUNT(DISTINCT ts) releves_perturbes FROM perturbations WHERE ts>=? AND id_ligne<>'' "
                       "GROUP BY id_ligne ORDER BY releves_perturbes DESC LIMIT 40", (d,))
    if serie == "qualite":
        return requete("SELECT ts, controle, valeur, statut FROM qualite WHERE ts>=? AND (controle LIKE 'Couverture%' "
                       "OR controle LIKE 'Complétude%' OR controle LIKE 'Stations figées%') ORDER BY ts", (d,))
    if serie == "qualite_bilan":
        return requete("SELECT source, controle, SUM(statut='ok') ok, SUM(statut='attention') attention, "
                       "SUM(statut='alerte') alerte, COUNT(*) n, AVG(valeur) moyenne FROM qualite WHERE ts>=? "
                       "GROUP BY source, controle ORDER BY source, controle", (d,))
    if serie == "collectes":
        return requete("SELECT * FROM collectes ORDER BY ts DESC LIMIT 200")
    return []


def resume_tables():
    out = []
    for t in ("lignes", "bus_agrege", "perturbations", "velib_global", "velib_stations", "collectes", "qualite"):
        r = requete("SELECT COUNT(*) n, MIN(ts) debut, MAX(ts) fin FROM %s" % t)[0]
        r["table"] = t
        out.append(r)
    return out


# ---------------------------------------------------------------- serveur HTTP

class Gestionnaire(BaseHTTPRequestHandler):
    server_version = "IDFTransportsLive/" + VERSION

    def log_message(self, *a):
        pass

    def envoyer(self, code, corps, type_mime="application/json; charset=utf-8", entetes=None):
        if isinstance(corps, (dict, list)):
            corps = json.dumps(corps, ensure_ascii=False).encode("utf-8")
        elif isinstance(corps, str):
            corps = corps.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", type_mime)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        for k, v in (entetes or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(corps)))
        self.end_headers()
        self.wfile.write(corps)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        try:
            n = int(self.headers.get("Content-Length", 0) or 0)
            corps = json.loads(self.rfile.read(n).decode("utf-8") or "{}") if n else {}
            chemin = urllib.parse.urlparse(self.path).path
            if chemin == "/api/config":
                for k in ("prim_api_key", "intervalle_min", "historiser_bus", "velib_stations_toutes_min"):
                    if k in corps and corps[k] not in (None, ""):
                        CONFIG[k] = corps[k].strip() if isinstance(corps[k], str) else corps[k]
                sauver_config(CONFIG)
                return self.envoyer(200, {"ok": True})
            if chemin == "/api/collecter":
                threading.Thread(target=collecter, daemon=True).start()
                return self.envoyer(200, {"ok": True, "message": "Collecte lancée"})
            self.envoyer(404, {"erreur": "inconnu"})
        except Exception as e:
            self.envoyer(500, {"erreur": str(e)})

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        p = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
        chemin = u.path
        try:
            if chemin in ("/", "/index.html"):
                with open(FICHIER_HTML, "rb") as f:
                    return self.envoyer(200, f.read(), "text/html; charset=utf-8")
            if chemin == "/api/statut":
                return self.envoyer(200, {
                    "version": VERSION, "cle_presente": bool(CONFIG.get("prim_api_key")),
                    "derniere_collecte": ETAT["derniere_collecte"], "prochaine_collecte": ETAT["prochaine_collecte"],
                    "en_cours": ETAT["en_cours"], "intervalle_min": CONFIG.get("intervalle_min"),
                    "historiser_bus": CONFIG.get("historiser_bus"),
                    "velib_stations_toutes_min": CONFIG.get("velib_stations_toutes_min"),
                    "erreurs": ETAT["erreurs"][:10], "tables": resume_tables(),
                    "taille_db_mo": round(os.path.getsize(FICHIER_DB) / 1e6, 2) if os.path.exists(FICHIER_DB) else 0,
                    "lignes_referentiel": len(ETAT["ref"]), "affluence_etat": ETAT["affluence_etat"],
                })
            if chemin == "/api/live":
                return self.envoyer(200, ETAT["live"] or {"ts": None, "lignes": [], "perturbations": [], "en_attente": True})
            if chemin == "/api/referentiel":
                return self.envoyer(200, list(charger_referentiel().values()))
            if chemin == "/api/velib":
                return self.envoyer(200, lire_velib())
            if chemin == "/api/affluence":
                a = affluence_maintenant(heure=p.get("heure"))
                if not a:
                    return self.envoyer(200, {"pret": False, "etat": ETAT["affluence_etat"]})
                a["pret"] = True
                if p.get("leger"):
                    for l in a["lignes"].values():
                        l.pop("courbe", None)
                return self.envoyer(200, a)
            if chemin == "/api/ponctualite":
                return self.envoyer(200, ETAT["ponctualite"] or charger_ponctualite() or {})
            if chemin == "/api/historique":
                return self.envoyer(200, historique(p))
            if chemin == "/api/arrets":
                return self.envoyer(200, arrets_ligne(p.get("ligne", "")))
            if chemin == "/api/passages":
                return self.envoyer(200, http_get(URL_STOP, {"MonitoringRef": p.get("ref", "")}, cle=True, timeout=30))
            if chemin == "/api/itineraire":
                q = {"from": p.get("from"), "to": p.get("to"), "count": "3"}
                if p.get("datetime"):
                    q["datetime"] = p["datetime"]
                return self.envoyer(200, http_get(URL_ITINERAIRE, q, cle=True, timeout=40))
            if chemin == "/api/geocode":
                return self.envoyer(200, geocoder(p.get("q", "")))
            if chemin.startswith("/export/") and chemin.endswith(".csv"):
                return self.exporter(chemin[8:-4], p)
            self.envoyer(404, {"erreur": "chemin inconnu : " + chemin})
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", "replace")[:300]
            except Exception:
                pass
            self.envoyer(502, {"erreur": "HTTP %s sur la source distante" % e.code, "detail": detail})
        except Exception as e:
            self.envoyer(500, {"erreur": str(e)})

    def exporter(self, table, p):
        if table not in COLONNES:
            return self.envoyer(404, {"erreur": "table inconnue", "tables": list(COLONNES)})
        sep = ";" if p.get("format") == "fr" else ","
        cond, params = [], []
        if p.get("jours"):
            cond.append("ts >= ?")
            params.append(depuis(p["jours"]))
        if p.get("debut"):
            cond.append("ts >= ?")
            params.append(p["debut"])
        if p.get("fin"):
            cond.append("ts <= ?")
            params.append(p["fin"] + " 23:59:59" if len(p["fin"]) == 10 else p["fin"])
        if p.get("id") and table in ("lignes", "perturbations"):
            cond.append("id_ligne = ?")
            params.append(p["id"])
        if p.get("famille") and table in ("lignes", "perturbations"):
            cond.append("famille = ?")
            params.append(p["famille"])
        sql = "SELECT %s FROM %s%s ORDER BY ts" % (",".join(COLONNES[table]), table,
                                                   (" WHERE " + " AND ".join(cond)) if cond else "")
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=sep)
        w.writerow(COLONNES[table])
        for r in requete(sql, params):
            ligne = []
            for c in COLONNES[table]:
                v = r.get(c)
                if v is None:
                    v = ""
                elif isinstance(v, float) and sep == ";":
                    v = str(v).replace(".", ",")
                ligne.append(v)
            w.writerow(ligne)
        nom = "%s_%s.csv" % (table, datetime.now().strftime("%Y%m%d_%H%M"))
        self.envoyer(200, "\ufeff" + buf.getvalue(), "text/csv; charset=utf-8",
                     {"Content-Disposition": 'attachment; filename="%s"' % nom} if p.get("telecharger") else None)


def arrets_ligne(lid):
    lid = id_ligne(lid)
    for champ in ("id", "route_id"):
        try:
            data = http_get(URL_ARRETS_LIGNES, {"where": '%s="IDFM:%s"' % (champ, lid), "limit": "100"}, timeout=30)
            res = []
            for r in data.get("results", []):
                lat = r.get("stop_lat")
                lon = r.get("stop_lon")
                if (lat is None or lon is None) and isinstance(r.get("pointgeo"), dict):
                    lat, lon = r["pointgeo"].get("lat"), r["pointgeo"].get("lon")
                res.append({"id": r.get("stop_id"), "nom": r.get("stop_name"), "lat": lat, "lon": lon,
                            "commune": r.get("nom_commune")})
            if res:
                vus, uniques = set(), []
                for a in res:
                    if a["nom"] not in vus:
                        vus.add(a["nom"])
                        uniques.append(a)
                return uniques
        except Exception:
            continue
    return []


def geocoder(q):
    for url in GEOCODEURS:
        try:
            d = http_get(url, {"q": q, "limit": "5", "lat": "48.8566", "lon": "2.3522"}, timeout=15)
            out = [{"label": f["properties"].get("label"), "lon": f["geometry"]["coordinates"][0],
                    "lat": f["geometry"]["coordinates"][1]} for f in d.get("features", [])]
            if out:
                return out
        except Exception:
            continue
    return []


# ---------------------------------------------------------------- lancement

def main():
    ap = argparse.ArgumentParser(description="IDF Transports Live")
    ap.add_argument("--once", action="store_true", help="une seule collecte (CSV dans data/), puis arrêt")
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--velib-stations-toutes", type=int, default=None, help="minutes entre deux relevés station par station")
    a = ap.parse_args()
    if a.velib_stations_toutes:
        CONFIG["velib_stations_toutes_min"] = a.velib_stations_toutes
    if a.once:
        charger_calendrier()
        charger_affluence()
        collecter(mode_once=True)
        return
    init_db()
    port = a.port or int(CONFIG.get("port", 8765))
    threading.Thread(target=boucle_donnees_lentes, daemon=True).start()
    threading.Thread(target=boucle_collecte, daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", port), Gestionnaire)
    print("=" * 64)
    print(" IDF Transports Live %s" % VERSION)
    print(" Ouvre ton navigateur sur : http://localhost:%d" % port)
    print(" Liens CSV : http://localhost:%d/export/lignes.csv (et velib_global, perturbations…)" % port)
    print(" Pour arrêter : Ctrl + C")
    print("=" * 64)
    if not CONFIG.get("prim_api_key"):
        print(" ⚠  Pas encore de clé PRIM : Vélib' fonctionne, le reste attend ta clé (onglet Réglages).")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nArrêt.")


if __name__ == "__main__":
    main()
