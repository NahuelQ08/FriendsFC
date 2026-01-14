"""
Scraping de competiciones deportivas
===================================

Este módulo se encarga de obtener y procesar datos de competiciones deportivas
desde ScoresWay, organizando la información en DataFrames y creando estructuras
de directorios para almacenar datos.

Autor: Tu nombre
Fecha: Julio 2025
"""

import os
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple

# Importar funciones comunes
from utils_common import sanitize_dir_name


class CompetitionScraper:
    """
    Clase para hacer scraping de competiciones deportivas desde ScoresWay.
    """
    
    def __init__(self, base_url: str = "https://www.scoresway.com"):
        """
        Inicializa el scraper de competiciones.
        
        Args:
            base_url (str): URL base del sitio web
        """
        self.base_url = base_url
        self.competitions_url = f"{base_url}/en_GB/soccer/competitions"
        self.headers = {
            "Host": "www.scoresway.com",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Cookie": "_ga=GA1.1.666883386.1751465023; OptanonAlertBoxClosed=2025-09-19T18:35:29.983Z; eupubconsent-v2=CQX_8fgQX_8fgAcABBESB8FsAP_gAEPgACiQLmtR_G__bWlr-b73aftkeYxP9_hr7sQxBgbJk24FzLvW_JwXx2E5NAzatqIKmRIAu3TBIQNlHJDURVCgKIgVryDMaEyUoTNKJ6BkiFMRI2NYCFxvm4pjeQCY5vr99lc1mB-N7dr82dzyy6hHn3a5_2S1WJCdIYetDfv8ZBKT-9IEd_x8v4v4_F7pE2-eS1n_pGvp6D9-Yns_dBmx9_baffzPn__rl_e7X_vf_n37v943H77v____f_-7_-C5gAJhoVEEZZECIRKBhBAgAUFYQAUCAIAAEgaICAEwYFOQMAF1hMgBACgAGCAEAAIMAAQAACQAIRABQAQCAACAQKAAMACAICABgYAAwAWIgEAAIDoGKYEEAgWACRmVQaYEoACQQEtlQgkAwIK4QhFngEECImCgAABAAKAgAAeCwEJJASsSCALiCaAAAgAACiBAgRSNmAIKAzRaC8GT6MjTAMHzBMkpkGQBMEZGSbEJvwmHjkKIUEOQGxSzAAAA.f_wACHwAAAAA; OptanonConsent=isGpcEnabled=0&datestamp=Sat+Oct+04+2025+07%3A54%3A48+GMT-0300+(Argentina+Standard+Time)&version=202501.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=3aea9760-108d-4551-a221-5897ac42ce3e&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0002%3A1%2CC0001%3A1%2CV2STACK42%3A1&intType=1&geolocation=AR%3BS&AwaitingReconsent=false; _ga_7T677PWWJ1=GS2.1.s1759575288$o13$g0$t1759575663$j60$l0$h0; _ga_SQ24F7Q7YW=GS2.1.s1759575288$o12$g0$t1759575663$j60$l0$h0; _ga_K2ECMCJBFQ=GS2.1.s1759575289$o12$g0$t1759575663$j60$l0$h0; ak_bmsc=2ECEC4E130BAC5B20F08F35A6089AD16~000000000000000000000000000000~YAAQ1fcSAj+I74CZAQAAs+Lhrh2YPxK77E7qLFZQQuUebu8S0QfESbLbXxUf+GBQYg1VDTAbYpqkpZDI8yGwzACCEALkrWskMm5uYnzHod+pAGWM63Wd809k9bgU465O9y1iGHBRyU6BYhpIDR58GC1IHZckBUKMfrOpX5vpxRhdZyxDbiNrqy8BQ7ZrpGZ2QRKsLZLEpg2ubVfna7Iq3j67+IQjyAD8niyPPvMGf1PEGrCLDyt0NW2D1Zpxxd2epGIhDmvYpYQScZmHTwWq8cQs5rzrfYV9GnYpmkDWxEJQBpO9qNcNd7fOy25RM/4ViEc6VfmL7jE4n4+5ojvrqfoOFTxAFNYimypmRem1BjLFWBy5Xeh8y1h9epVd+fUNt7knN3ZDMmZpouwku1fNUCjIgzbAilqHxc4ZfymZUpTu3Ls40cYHID9ZI797B0SuLaLlTyUErgXKBux+DRpL1ZD18Fw=",
            "Pragma": "no-cache",
            "Priority": "u=0, i",
            "Referer": "https://www.scoresway.com/en_GB/soccer/competitions",
            "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": "\"Android\"",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Mobile Safari/537.36",
        }

        self.data_dir = 'data'
    
    def fetch_competitions_data(self) -> Dict:
        """
        Obtiene los datos de competiciones desde la página web.
        
        Returns:
            Dict: Datos JSON de competiciones
            
        Raises:
            Exception: Si no se pueden obtener los datos
        """
        try:
            print("Obteniendo datos de competiciones...")
            
            # Realizar request a la página
            response = requests.get(self.competitions_url, headers=self.headers)
            response.raise_for_status()
            
            # Parsear HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Buscar el script con el JSON de competiciones
            script = soup.find('script', {'id': 'compData', 'type': 'application/json'})
            if not script:
                raise Exception("No se encontró el script con ID compData")
            
            # Parsear JSON
            data = json.loads(script.string)
            print(f"✅ Datos obtenidos exitosamente")
            return data
            
        except requests.RequestException as e:
            raise Exception(f"Error al hacer request: {e}")
        except json.JSONDecodeError as e:
            raise Exception(f"Error al parsear JSON: {e}")
        except Exception as e:
            raise Exception(f"Error inesperado: {e}")
    
    def fetch_single_competition_from_url(self, competition_url: str) -> Dict:
        """
        Obtiene datos de UNA competición específica desde su URL.
        
        Args:
            competition_url (str): URL completa de la competición
            
        Returns:
            Dict: Información de la competición
        """
        try:
            print(f"Obteniendo datos de: {competition_url}")
            
            # Realizar request a la página de la competición
            response = requests.get(competition_url, headers=self.headers)
            response.raise_for_status()
            
            # Parsear HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extraer información básica de la página
            competition_info = {}
            
            # Intentar obtener el nombre de la competición
            title_tag = soup.find('h1') or soup.find('title')
            if title_tag:
                competition_info['competicion'] = title_tag.get_text(strip=True)
            
            # Extraer ID de la URL (generalmente está al final)
            url_parts = competition_url.rstrip('/').split('/')
            if url_parts:
                competition_info['id_competicion'] = url_parts[-1]
            
            competition_info['url'] = competition_url
            competition_info['continente'] = 'N/A'
            competition_info['pais'] = 'N/A'
            competition_info['crest'] = None
            competition_info['top'] = False
            competition_info['orden'] = 0
            
            print(f"✅ Datos obtenidos: {competition_info['competicion']}")
            return competition_info
            
        except requests.RequestException as e:
            raise Exception(f"Error al hacer request: {e}")
        except Exception as e:
            raise Exception(f"Error inesperado: {e}")
    
    def parse_competition_data(self, data: Dict) -> pd.DataFrame:
        """
        Parsea los datos de competiciones y los convierte en DataFrame.
        
        Args:
            data (Dict): Datos JSON de competiciones
            
        Returns:
            pd.DataFrame: DataFrame con información de competiciones
        """
        competitions = []
        
        try:
            for continent in data.get('continents', []):
                continent_name = continent.get('name')
                
                for country in continent.get('countries', []):
                    country_name = country.get('name')
                    
                    for comp in country.get('comps', []):
                        # Extraer slug e id_hash de la URL
                        url = comp.get('url', '')
                        slug = None
                        id_hash = None
                        
                        if url:
                            # URL formato: /en_GB/soccer/SLUG/ID_HASH/results
                            parts = url.strip('/').split('/')
                            if len(parts) >= 4:
                                slug = parts[2]      # afc-asian-cup-2023-qatar
                                id_hash = parts[3]   # dxgoo5g7fx8rp5vu8kkzhcxnu
                        
                        competition_info = {
                            'continente': continent_name,
                            'pais': country_name,
                            'competicion': comp.get('name'),
                            'id_competicion': comp.get('id'),
                            'slug': slug,
                            'id_hash': id_hash,
                            'url': f"{self.base_url}{comp.get('url')}" if comp.get('url') else None,
                            'crest': f"{self.base_url}{comp.get('crest')}" if comp.get('crest') else None,
                            'top': comp.get('top'),
                            'orden': comp.get('ord')
                        }
                        competitions.append(competition_info)
            
            df = pd.DataFrame(competitions)
            print(f"✅ Parseados {len(competitions)} competiciones")
            return df
            
        except Exception as e:
            raise Exception(f"Error al parsear datos de competiciones: {e}")
    
    def save_competitions_csv(self, df: pd.DataFrame, filename: str = 'competiciones.csv') -> str:
        """
        Guarda el DataFrame de competiciones en CSV.
        
        Args:
            df (pd.DataFrame): DataFrame con competiciones
            filename (str): Nombre del archivo CSV
            
        Returns:
            str: Ruta del archivo guardado
        """
        try:
            # Crear directorio si no existe
            os.makedirs(self.data_dir, exist_ok=True)
            
            filepath = os.path.join(self.data_dir, filename)
            df.to_csv(filepath, index=False)
            
            print(f"✅ Competiciones guardadas en: {filepath}")
            return filepath
            
        except Exception as e:
            raise Exception(f"Error al guardar CSV: {e}")
    
    def load_competitions_csv(self, filename: str = 'competiciones.csv') -> pd.DataFrame:
        """
        Carga el DataFrame de competiciones desde CSV.
        
        Args:
            filename (str): Nombre del archivo CSV
            
        Returns:
            pd.DataFrame: DataFrame con competiciones
        """
        try:
            filepath = os.path.join(self.data_dir, filename)
            df = pd.read_csv(filepath)
            print(f"✅ Competiciones cargadas desde: {filepath}")
            return df
            
        except FileNotFoundError:
            raise Exception(f"Archivo no encontrado: {filepath}")
        except Exception as e:
            raise Exception(f"Error al cargar CSV: {e}")
    
    def create_directory_structure(self, df: pd.DataFrame) -> None:
        """
        Crea estructura de directorios basada en las competiciones.
        
        Args:
            df (pd.DataFrame): DataFrame con competiciones
        """
        try:
            # Asegurar que el directorio base existe
            os.makedirs(self.data_dir, exist_ok=True)
            
            created_dirs = 0
            
            for _, row in df.iterrows():
                # Verificar que los datos necesarios están presentes
                required_fields = ['continente', 'pais', 'competicion', 'id_competicion']
                if not all(pd.notna(row[field]) for field in required_fields):
                    continue
                
                # Crear nombres de directorio seguros
                continente_dir = sanitize_dir_name(row['continente'])
                pais_dir = sanitize_dir_name(row['pais'])
                competicion_dir = f"{sanitize_dir_name(row['competicion'])}_{row['id_competicion']}"
                
                # Crear la ruta completa
                ruta_competicion = os.path.join(
                    self.data_dir, 
                    continente_dir, 
                    pais_dir, 
                    competicion_dir
                )
                
                # Crear directorios anidados si no existen
                if not os.path.exists(ruta_competicion):
                    os.makedirs(ruta_competicion, exist_ok=True)
                    created_dirs += 1
            
            print(f"✅ Estructura de directorios creada exitosamente")
            print(f"   - Directorios creados: {created_dirs}")
            print(f"   - Total competiciones procesadas: {len(df)}")
            
        except Exception as e:
            raise Exception(f"Error al crear estructura de directorios: {e}")
    
    def get_competition_summary(self, df: pd.DataFrame) -> Dict:
        """
        Genera un resumen estadístico de las competiciones.
        
        Args:
            df (pd.DataFrame): DataFrame con competiciones
            
        Returns:
            Dict: Resumen estadístico
        """
        try:
            summary = {
                'total_competiciones': len(df),
                'total_continentes': df['continente'].nunique(),
                'total_paises': df['pais'].nunique(),
                'competiciones_por_continente': df['continente'].value_counts().to_dict(),
                'competiciones_top': df[df['top'] == True]['competicion'].tolist() if 'top' in df.columns else []
            }
            
            return summary
            
        except Exception as e:
            raise Exception(f"Error al generar resumen: {e}")


# ========================================
# NUEVA FUNCIÓN: Scraping desde UN link
# ========================================
def scrape_from_link(competition_url: str, filename: str = None) -> pd.DataFrame:
    """
    🎯 FUNCIÓN SIMPLE: Crea CSV desde un link de competición.
    
    Esta es la función que vas a usar en tu notebook!
    
    Args:
        competition_url (str): El link de la competición (ejemplo: "https://www.scoresway.com/...")
        filename (str, optional): Nombre del archivo CSV (si no lo pones, usa el ID de la competición)
        
    Returns:
        pd.DataFrame: DataFrame con la información de la competición
        
    Ejemplo de uso en tu notebook:
        from scraping_competitions import scrape_from_link
        
        link = "https://www.scoresway.com/en_GB/soccer/competitions/premier-league/12345"
        df = scrape_from_link(link)
        display(df)
    """
    scraper = CompetitionScraper()
    
    try:
        # Obtener datos de esa competición específica
        competition_data = scraper.fetch_single_competition_from_url(competition_url)
        
        # Convertir a DataFrame
        df = pd.DataFrame([competition_data])
        
        # Generar nombre de archivo si no se proporciona
        if filename is None:
            comp_id = competition_data.get('id_competicion', 'competicion')
            filename = f"competicion_{comp_id}.csv"
        
        # Guardar CSV
        scraper.save_competitions_csv(df, filename)
        
        print(f"\n🎉 ¡Listo! CSV creado exitosamente")
        print(f"📁 Archivo: data/{filename}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise


# Funciones originales (mantienen la funcionalidad anterior)
def scrape_and_save_competitions(save_csv: bool = True, create_dirs: bool = True) -> Tuple[pd.DataFrame, Dict]:
    """
    Función principal para hacer scraping completo de competiciones.
    
    Args:
        save_csv (bool): Si guardar en CSV
        create_dirs (bool): Si crear estructura de directorios
        
    Returns:
        Tuple[pd.DataFrame, Dict]: DataFrame de competiciones y resumen
    """
    scraper = CompetitionScraper()
    
    try:
        # Obtener datos
        data = scraper.fetch_competitions_data()
        
        # Parsear datos
        df = scraper.parse_competition_data(data)
        
        # Guardar CSV si se solicita
        if save_csv:
            scraper.save_competitions_csv(df)
        
        # Crear directorios si se solicita
        if create_dirs:
            scraper.create_directory_structure(df)
        
        # Generar resumen
        summary = scraper.get_competition_summary(df)
        
        return df, summary
        
    except Exception as e:
        print(f"❌ Error en scraping: {e}")
        raise


def load_existing_competitions() -> pd.DataFrame:
    """
    Carga competiciones desde CSV existente.
    
    Returns:
        pd.DataFrame: DataFrame con competiciones
    """
    scraper = CompetitionScraper()
    return scraper.load_competitions_csv()


# Función de testing
def test_scraper():
    """
    Función de prueba para el scraper de competiciones.
    """
    print("=== Testing Competition Scraper ===")
    
    try:
        df, summary = scrape_and_save_competitions()
        
        print(f"\n📊 Resumen:")
        print(f"   - Total competiciones: {summary['total_competiciones']}")
        print(f"   - Total continentes: {summary['total_continentes']}")
        print(f"   - Total países: {summary['total_paises']}")
        
        print(f"\n🔍 Primeras 5 competiciones:")
        print(df.head())
        
        return True
        
    except Exception as e:
        print(f"❌ Error en testing: {e}")
        return False


if __name__ == "__main__":
    test_scraper()