"""
Scraping de temporadas deportivas
================================

Este módulo se encarga de obtener y procesar las temporadas disponibles
para cada competición deportiva, creando estructuras de datos y directorios
organizados por temporada.

Autor: Tu nombre
Fecha: Julio 2025
"""

import os
import time
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, unquote

# Importar funciones comunes
from utils_common import sanitize_dir_name, get_season_name_from_url, random_sleep_time


class SeasonScraper:
    """
    Clase para hacer scraping de temporadas deportivas desde ScoresWay.
    """
    
    def __init__(self, base_url: str = "https://www.scoresway.com"):
        """
        Inicializa el scraper de temporadas.
        
        Args:
            base_url (str): URL base del sitio web
        """
        self.base_url = base_url
        self.headers = {
            'Host': 'www.scoresway.com',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Cookie': '_ga=GA1.1.666883386.1751465023; OptanonAlertBoxClosed=2025-09-19T18:35:29.983Z; eupubconsent-v2=CQX_8fgQX_8fgAcABBESB8FsAP_gAEPgACiQLmtR_G__bWlr-b73aftkeYxP9_hr7sQxBgbJk24FzLvW_JwXx2E5NAzatqIKmRIAu3TBIQNlHJDURVCgKIgVryDMaEyUoTNKJ6BkiFMRI2NYCFxvm4pjeQCY5vr99lc1mB-N7dr82dzyy6hHn3a5_2S1WJCdIYetDfv8ZBKT-9IEd_x8v4v4_F7pE2-eS1n_pGvp6D9-Yns_dBmx9_baffzPn__rl_e7X_vf_n37v943H77v____f_-7_-C5gAJhoVEEZZECIRKBhBAgAUFYQAUCAIAAEgaICAEwYFOQMAF1hMgBACgAGCAEAAIMAAQAACQAIRABQAQCAACAQKAAMACAICABgYAAwAWIgEAAIDoGKYEEAgWACRmVQaYEoACQQEtlQgkAwIK4QhFngEECImCgAABAAKAgAAeCwEJJASsSCALiCaAAAgAACiBAgRSNmAIKAzRaC8GT6MjTAMHzBMkpkGQBMEZGSbEJvwmHjkKIUEOQGxSzAAAA.f_wACHwAAAAA; OptanonConsent=isGpcEnabled=0&datestamp=Sat+Oct+04+2025+07%3A54%3A48+GMT-0300+(Argentina+Standard+Time)&version=202501.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=3aea9760-108d-4551-a221-5897ac42ce3e&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0002%3A1%2CC0001%3A1%2CV2STACK42%3A1&intType=1&geolocation=AR%3BS&AwaitingReconsent=false; _ga_7T677PWWJ1=GS2.1.s1759575288$o13$g0$t1759575663$j60$l0$h0; _ga_SQ24F7Q7YW=GS2.1.s1759575288$o12$g0$t1759575663$j60$l0$h0; _ga_K2ECMCJBFQ=GS2.1.s1759575289$o12$g0$t1759575663$j60$l0$h0; ak_bmsc=2ECEC4E130BAC5B20F08F35A6089AD16~000000000000000000000000000000~YAAQ1fcSAj+I74CZAQAAs+Lhrh2YPxK77E7qLFZQQuUebu8S0QfESbLbXxUf+GBQYg1VDTAbYpqkpZDI8yGwzACCEALkrWskMm5uYnzHod+pAGWM63Wd809k9bgU465O9y1iGHBRyU6BYhpIDR58GC1IHZckBUKMfrOpX5vpxRhdZyxDbiNrqy8BQ7ZrpGZ2QRKsLZLEpg2ubVfna7Iq3j67+IQjyAD8niyPPvMGf1PEGrCLDyt0NW2D1Zpxxd2epGIhDmvYpYQScZmHTwWq8cQs5rzrfYV9GnYpmkDWxEJQBpO9qNcNd7fOy25RM/4ViEc6VfmL7jE4n4+5ojvrqfoOFTxAFNYimypmRem1BjLFWBy5Xeh8y1h9epVd+fUNt7knN3ZDMmZpouwku1fNUCjIgzbAilqHxc4ZfymZUpTu3Ls40cYHID9ZI797B0SuLaLlTyUErgXKBux+DRpL1ZD18Fw=',
            'Pragma': 'no-cache',
            'Priority': 'u=0, i',
            'referer': f'{base_url}/en_GB/soccer/',
            'sec-ch-ua': '\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '\'Android\'',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Mobile Safari/537.36',
        }
        # self.headers = {
        #     'Accept': 'application/json',
        #     'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36',
        #     'referer': f'{base_url}/en_GB/soccer/'
        # }
        self.data_dir = 'data'
        self.min_delay = 1.0
        self.max_delay = 3.0
    
    def set_delay_range(self, min_delay: float, max_delay: float) -> None:
        """
        Configura el rango de delays entre requests.
        
        Args:
            min_delay (float): Delay mínimo en segundos
            max_delay (float): Delay máximo en segundos
        """
        self.min_delay = min_delay
        self.max_delay = max_delay
    
    def get_seasons_from_competition(self, competition_row: pd.Series) -> List[Dict]:
        """
        Obtiene todas las temporadas disponibles para una competición.
        
        Args:
            competition_row (pd.Series): Fila del DataFrame de competiciones
            
        Returns:
            List[Dict]: Lista de diccionarios con información de temporadas
        """
        url = competition_row['url']
        seasons = []
        
        try:
            # Delay aleatorio para no sobrecargar el servidor
            delay = random.uniform(self.min_delay, self.max_delay)
            print(f"⏳ Esperando {delay:.2f} segundos antes de descargar...")
            time.sleep(delay)
            
            # Realizar request
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            # Parsear HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            season_select = soup.find('select', {'id': 'season-select'})
            
            if not season_select:
                print(f"⚠️ No se encontró selector de temporada en {url}")
                return seasons
            
            # Extraer temporadas
            for option in season_select.find_all('option'):
                season_text = option.text.strip()
                season_value = option.get('value', '')
                
                if season_value:
                    season_url = f"{self.base_url}{season_value}"
                    
                    season_info = {
                        'continente': competition_row.get('continente'),
                        'pais': competition_row.get('pais'),
                        'competicion': competition_row.get('competicion'),
                        'id_competicion': competition_row.get('id_competicion'),
                        'url_competicion': url,
                        'temporada': season_text,
                        'url_temporada': season_url
                    }
                    seasons.append(season_info)
            
            print(f"✅ Encontradas {len(seasons)} temporadas para {competition_row.get('competicion')}")
            
        except requests.RequestException as e:
            print(f"❌ Error de conexión con {url}: {e}")
        except Exception as e:
            print(f"❌ Error inesperado con {url}: {e}")
        
        return seasons
    
    def scrape_all_seasons(self, df_competitions: pd.DataFrame, 
                          start_index: int = 0, 
                          limit: Optional[int] = None) -> pd.DataFrame:
        """
        Extrae todas las temporadas de todas las competiciones.
        
        Args:
            df_competitions (pd.DataFrame): DataFrame con competiciones
            start_index (int): Índice de inicio para procesar
            limit (Optional[int]): Límite de competiciones a procesar
            
        Returns:
            pd.DataFrame: DataFrame con todas las temporadas
        """
        all_seasons = []
        
        # Determinar rango de procesamiento
        end_index = len(df_competitions)
        if limit:
            end_index = min(start_index + limit, end_index)
        
        total_competitions = end_index - start_index
        
        print(f"🚀 Iniciando scraping de temporadas...")
        print(f"   - Competiciones a procesar: {total_competitions}")
        print(f"   - Rango: {start_index} a {end_index-1}")
        
        try:
            for idx in range(start_index, end_index):
                row = df_competitions.iloc[idx]
                competition_name = row.get('competicion', 'N/A')
                
                print(f"\n📋 Procesando {idx + 1}/{len(df_competitions)}: {competition_name}")
                
                # Obtener temporadas de esta competición
                seasons = self.get_seasons_from_competition(row)
                all_seasons.extend(seasons)
                
                # Delay adicional entre competiciones
                if idx < end_index - 1:  # No delay después de la última
                    delay = random.uniform(1, 2)
                    time.sleep(delay)
            
            # Crear DataFrame
            df_seasons = pd.DataFrame(all_seasons)
            
            # Agregar URL de resultados
            if not df_seasons.empty:
                df_seasons['url_resultados'] = df_seasons['url_temporada'].str.replace('fixtures', 'results')
            
            print(f"\n✅ Scraping completado!")
            print(f"   - Total temporadas encontradas: {len(all_seasons)}")
            
            return df_seasons
            
        except KeyboardInterrupt:
            print(f"\n⚠️ Scraping interrumpido por el usuario")
            print(f"   - Temporadas procesadas hasta el momento: {len(all_seasons)}")
            
            if all_seasons:
                df_partial = pd.DataFrame(all_seasons)
                if not df_partial.empty:
                    df_partial['url_resultados'] = df_partial['url_temporada'].str.replace('fixtures', 'results')
                return df_partial
            
            return pd.DataFrame()
        
        except Exception as e:
            print(f"❌ Error durante el scraping: {e}")
            raise
    
    def save_seasons_csv(self, df_seasons: pd.DataFrame, filename: str = 'todas_las_temporadas.csv') -> str:
        """
        Guarda el DataFrame de temporadas en CSV.
        
        Args:
            df_seasons (pd.DataFrame): DataFrame con temporadas
            filename (str): Nombre del archivo CSV
            
        Returns:
            str: Ruta del archivo guardado
        """
        try:
            # Crear directorio si no existe
            os.makedirs(self.data_dir, exist_ok=True)
            
            filepath = os.path.join(self.data_dir, filename)
            df_seasons.to_csv(filepath, index=False)
            
            print(f"✅ Temporadas guardadas en: {filepath}")
            return filepath
            
        except Exception as e:
            raise Exception(f"Error al guardar CSV: {e}")
    
    def load_seasons_csv(self, filename: str = 'todas_las_temporadas.csv') -> pd.DataFrame:
        """
        Carga el DataFrame de temporadas desde CSV.
        
        Args:
            filename (str): Nombre del archivo CSV
            
        Returns:
            pd.DataFrame: DataFrame con temporadas
        """
        try:
            filepath = os.path.join(self.data_dir, filename)
            df = pd.read_csv(filepath)
            print(f"✅ Temporadas cargadas desde: {filepath}")
            return df
            
        except FileNotFoundError:
            raise Exception(f"Archivo no encontrado: {filepath}")
        except Exception as e:
            raise Exception(f"Error al cargar CSV: {e}")
    
    def create_seasons_directory_structure(self, df_seasons: pd.DataFrame) -> Dict:
        """
        Crea estructura de directorios basada en las temporadas.
        
        Args:
            df_seasons (pd.DataFrame): DataFrame con temporadas
            
        Returns:
            Dict: Estadísticas de creación de directorios
        """
        try:
            # Asegurar que el directorio base existe
            os.makedirs(self.data_dir, exist_ok=True)
            
            created_dirs = 0
            skipped_rows = 0
            errors = 0
            
            print("🗂️ Creando estructura de directorios para temporadas...")
            
            for idx, row in df_seasons.iterrows():
                try:
                    # Verificar que los datos necesarios están presentes
                    required_fields = ['continente', 'pais', 'competicion', 'id_competicion', 'url_resultados']
                    if not all(pd.notna(row[field]) for field in required_fields):
                        skipped_rows += 1
                        continue
                    
                    # Crear nombres de directorio seguros
                    continente_dir = sanitize_dir_name(row['continente'])
                    pais_dir = sanitize_dir_name(row['pais'])
                    competicion_dir = f"{sanitize_dir_name(row['competicion'])}_{row['id_competicion']}"
                    
                    # Obtener el nombre de la temporada desde la URL de resultados
                    season_name = get_season_name_from_url(row['url_resultados'])
                    if not season_name:
                        print(f"⚠️ No se pudo extraer temporada de: {row['url_resultados']}")
                        skipped_rows += 1
                        continue
                    
                    season_name = sanitize_dir_name(season_name)
                    
                    # Crear la ruta completa
                    ruta_temporada = os.path.join(
                        self.data_dir,
                        continente_dir,
                        pais_dir,
                        competicion_dir,
                        season_name
                    )
                    
                    # Crear directorios anidados si no existen
                    if not os.path.exists(ruta_temporada):
                        os.makedirs(ruta_temporada, exist_ok=True)
                        created_dirs += 1
                
                except Exception as e:
                    print(f"⚠️ Error procesando fila {idx}: {e}")
                    errors += 1
            
            stats = {
                'total_rows': len(df_seasons),
                'created_dirs': created_dirs,
                'skipped_rows': skipped_rows,
                'errors': errors
            }
            
            print(f"\n✅ Estructura de directorios para temporadas creada!")
            print(f"   - Filas procesadas: {stats['total_rows']}")
            print(f"   - Directorios creados: {stats['created_dirs']}")
            print(f"   - Filas omitidas: {stats['skipped_rows']}")
            print(f"   - Errores: {stats['errors']}")
            
            return stats
            
        except Exception as e:
            raise Exception(f"Error al crear estructura de directorios: {e}")
    
    def get_seasons_summary(self, df_seasons: pd.DataFrame) -> Dict:
        """
        Genera un resumen estadístico de las temporadas.
        
        Args:
            df_seasons (pd.DataFrame): DataFrame con temporadas
            
        Returns:
            Dict: Resumen estadístico
        """
        try:
            if df_seasons.empty:
                return {'total_temporadas': 0}
            
            summary = {
                'total_temporadas': len(df_seasons),
                'competiciones_unicas': df_seasons['competicion'].nunique(),
                'paises_unicos': df_seasons['pais'].nunique(),
                'continentes_unicos': df_seasons['continente'].nunique(),
                'temporadas_por_competicion': df_seasons.groupby('competicion')['temporada'].count().to_dict(),
                'temporadas_por_pais': df_seasons['pais'].value_counts().head(10).to_dict()
            }
            
            return summary
            
        except Exception as e:
            raise Exception(f"Error al generar resumen: {e}")


# Funciones de conveniencia para usar directamente
def scrape_and_save_seasons(df_competitions: pd.DataFrame, 
                            save_csv: bool = True, 
                            create_dirs: bool = True,
                            start_index: int = 0,
                            limit: Optional[int] = None,
                            append_to_existing: bool = True) -> Tuple[pd.DataFrame, Dict]:

    """
    Función principal para hacer scraping completo de temporadas.
    
    Args:
        df_competitions (pd.DataFrame): DataFrame con competiciones
        save_csv (bool): Si guardar en CSV
        create_dirs (bool): Si crear estructura de directorios
        start_index (int): Índice de inicio
        limit (Optional[int]): Límite de competiciones a procesar
        append_to_existing (bool): Si agregar a CSV existente en lugar de sobrescribir
        
    Returns:
        Tuple[pd.DataFrame, Dict]: DataFrame de temporadas y resumen
    """
    scraper = SeasonScraper()
    
    try:
        # Intentar cargar datos existentes si append_to_existing es True
        existing_df = pd.DataFrame()
        if append_to_existing and save_csv:
            try:
                existing_df = scraper.load_seasons_csv()
                print(f"📂 Cargadas {len(existing_df)} temporadas existentes")
            except:
                print("📂 No se encontraron temporadas existentes, comenzando desde cero")
        
        # Scraping de temporadas
        df_new_seasons = scraper.scrape_all_seasons(df_competitions, start_index, limit)
        
        # Combinar con datos existentes si corresponde
        if append_to_existing and not existing_df.empty and not df_new_seasons.empty:
            # Evitar duplicados basándose en columnas clave
            key_columns = ['competicion', 'id_competicion', 'temporada']
            
            # Crear una clave única para identificar duplicados
            existing_df['_temp_key'] = existing_df[key_columns].apply(
                lambda x: '_'.join(x.astype(str)), axis=1
            )
            df_new_seasons['_temp_key'] = df_new_seasons[key_columns].apply(
                lambda x: '_'.join(x.astype(str)), axis=1
            )
            
            # Filtrar solo las temporadas nuevas que no existen
            new_keys = set(df_new_seasons['_temp_key']) - set(existing_df['_temp_key'])
            df_truly_new = df_new_seasons[df_new_seasons['_temp_key'].isin(new_keys)]
            
            # Combinar DataFrames
            df_combined = pd.concat([existing_df, df_truly_new], ignore_index=True)
            
            # Limpiar columnas temporales
            df_combined = df_combined.drop(columns=['_temp_key'])
            df_seasons = df_combined
            
            print(f"📊 Combinado: {len(existing_df)} existentes + {len(df_truly_new)} nuevas = {len(df_seasons)} total")
        else:
            df_seasons = df_new_seasons
        
        # Guardar CSV si se solicita
        if save_csv and not df_seasons.empty:
            scraper.save_seasons_csv(df_seasons)
        
        # Crear directorios si se solicita
        if create_dirs and not df_seasons.empty:
            scraper.create_seasons_directory_structure(df_seasons)
        
        # Generar resumen
        summary = scraper.get_seasons_summary(df_seasons)
        
        return df_seasons, summary
        
    except Exception as e:
        print(f"❌ Error en scraping de temporadas: {e}")
        raise


def find_resume_index(df_competitions: pd.DataFrame) -> int:
    """
    Encuentra el índice desde donde continuar basándose en temporadas ya procesadas.
    
    Args:
        df_competitions (pd.DataFrame): DataFrame con competiciones
        
    Returns:
        int: Índice desde donde continuar
    """
    try:
        scraper = SeasonScraper()
        
        # Intentar cargar temporadas existentes
        try:
            existing_seasons = scraper.load_seasons_csv()
        except:
            print("📂 No se encontraron temporadas existentes")
            return 0
        
        if existing_seasons.empty:
            return 0
        
        # Obtener competiciones ya procesadas
        processed_competitions = set()
        for _, row in existing_seasons.iterrows():
            key = f"{row['competicion']}_{row['id_competicion']}"
            processed_competitions.add(key)
        
        # Encontrar la primera competición no procesada
        for idx, row in df_competitions.iterrows():
            key = f"{row['competicion']}_{row['id_competicion']}"
            if key not in processed_competitions:
                print(f"🔍 Encontrado punto de reanudación en índice {idx}: {row['competicion']}")
                return idx
        
        print("✅ Todas las competiciones ya fueron procesadas")
        return len(df_competitions)
        
    except Exception as e:
        print(f"⚠️ Error al buscar punto de reanudación: {e}")
        return 0


def resume_seasons_scraping(df_competitions: pd.DataFrame,
                            save_csv: bool = True,
                            create_dirs: bool = True,
                            limit: Optional[int] = None) -> Tuple[pd.DataFrame, Dict]:
    """
    Continúa el scraping de temporadas desde donde se quedó automáticamente.
    
    Args:
        df_competitions (pd.DataFrame): DataFrame con competiciones
        save_csv (bool): Si guardar en CSV
        create_dirs (bool): Si crear estructura de directorios
        limit (Optional[int]): Límite de competiciones a procesar
        
    Returns:
        Tuple[pd.DataFrame, Dict]: DataFrame de temporadas y resumen
    """
    # Encontrar desde dónde continuar
    start_index = find_resume_index(df_competitions)
    
    if start_index >= len(df_competitions):
        print("✅ No hay nada más que procesar")
        scraper = SeasonScraper()
        existing_df = scraper.load_seasons_csv()
        summary = scraper.get_seasons_summary(existing_df)
        return existing_df, summary
    
    print(f"🚀 Continuando desde competición {start_index + 1}/{len(df_competitions)}")
    
    return scrape_and_save_seasons(
        df_competitions,
        save_csv=save_csv,
        create_dirs=create_dirs,
        start_index=start_index,
        limit=limit,
        append_to_existing=True
    )


def load_existing_seasons() -> pd.DataFrame:
    """
    Carga temporadas desde CSV existente.
    
    Returns:
        pd.DataFrame: DataFrame con temporadas
    """
    scraper = SeasonScraper()
    return scraper.load_seasons_csv()


def smart_scrape_seasons(restart_from_zero: bool = False, batch_size: int = 100) -> Tuple[pd.DataFrame, Dict]:
    """
    Función inteligente que detecta automáticamente si continuar o empezar de cero.
    
    Args:
        restart_from_zero (bool): Si True, borra datos existentes y empieza de cero
        batch_size (int): Número de competiciones a procesar en este lote
        
    Returns:
        Tuple[pd.DataFrame, Dict]: DataFrame de temporadas y resumen
        
    Example:
        # Para empezar de cero:
        df, summary = smart_scrape_seasons(restart_from_zero=True, batch_size=100)
        
        # Para continuar automáticamente:
        df, summary = smart_scrape_seasons(restart_from_zero=False, batch_size=100)
    """
    import os
    
    csv_path = os.path.join('data', 'todas_las_temporadas.csv')
    
    # Si quiere empezar de cero, borrar archivo existente
    if restart_from_zero and os.path.exists(csv_path):
        os.remove(csv_path)
        print("🔥 Reiniciando desde cero - archivo anterior eliminado")
    
    # Cargar competiciones
    from scraping_competitions import load_existing_competitions
    df_competitions = load_existing_competitions()
    
    # Detectar punto de continuación
    start_idx = 0
    existing_seasons = pd.DataFrame()
    
    try:
        if not restart_from_zero:
            existing_seasons = load_existing_seasons()
            
            # Encontrar competiciones ya procesadas
            processed_comps = set()
            for _, row in existing_seasons.iterrows():
                key = f"{row['competicion']}_{row['id_competicion']}"
                processed_comps.add(key)
            
            # Encontrar desde dónde continuar
            for idx, row in df_competitions.iterrows():
                key = f"{row['competicion']}_{row['id_competicion']}"
                if key not in processed_comps:
                    start_idx = idx
                    break
            else:
                # Si llegamos aquí, todas las competiciones ya fueron procesadas
                print("✅ Todas las competiciones ya fueron procesadas")
                summary = {
                    'total_temporadas': len(existing_seasons),
                    'competiciones_unicas': existing_seasons['competicion'].nunique() if not existing_seasons.empty else 0,
                    'paises_unicos': existing_seasons['pais'].nunique() if not existing_seasons.empty else 0,
                    'continentes_unicos': existing_seasons['continente'].nunique() if not existing_seasons.empty else 0,
                }
                return existing_seasons, summary
                
        print(f"🚀 Empezando desde competición {start_idx + 1}/{len(df_competitions)}")
        print(f"📂 Temporadas existentes: {len(existing_seasons)}")
        print(f"📋 Competiciones restantes: {len(df_competitions) - start_idx}")
        
    except Exception as e:
        print(f"📂 No hay datos previos o error al cargarlos: {e}")
        existing_seasons = pd.DataFrame()
        start_idx = 0
    
    # Procesar nuevas temporadas
    print(f"⚡ Procesando {min(batch_size, len(df_competitions) - start_idx)} competiciones...")
    
    df_new_seasons, summary = scrape_and_save_seasons(
        df_competitions,
        start_index=start_idx,
        limit=batch_size,
        save_csv=False,    # No guardar todavía
        create_dirs=True,
        append_to_existing=False  # No usamos esta función
    )
    
    # Combinar con temporadas existentes
    if not existing_seasons.empty and not df_new_seasons.empty and not restart_from_zero:
        # Evitar duplicados
        existing_keys = set()
        for _, row in existing_seasons.iterrows():
            key = f"{row['competicion']}_{row['id_competicion']}_{row['temporada']}"
            existing_keys.add(key)
        
        # Filtrar solo temporadas realmente nuevas
        new_rows = []
        for _, row in df_new_seasons.iterrows():
            key = f"{row['competicion']}_{row['id_competicion']}_{row['temporada']}"
            if key not in existing_keys:
                new_rows.append(row)
        
        if new_rows:
            df_truly_new = pd.DataFrame(new_rows)
            df_final = pd.concat([existing_seasons, df_truly_new], ignore_index=True)
            print(f"📊 Combinando: {len(existing_seasons)} existentes + {len(df_truly_new)} nuevas = {len(df_final)} total")
        else:
            df_final = existing_seasons
            print("📊 No se encontraron temporadas realmente nuevas")
    else:
        df_final = df_new_seasons
        print(f"📊 Total temporadas: {len(df_final)}")
    
    # Guardar resultado final
    if not df_final.empty:
        df_final.to_csv(csv_path, index=False)
        print(f"✅ Temporadas guardadas: {csv_path}")
        
        # Actualizar resumen con números finales
        scraper = SeasonScraper()
        summary = scraper.get_seasons_summary(df_final)
        
        # Agregar información de progreso
        summary['progress'] = {
            'processed_competitions': start_idx + min(batch_size, len(df_competitions) - start_idx),
            'total_competitions': len(df_competitions),
            'percentage': ((start_idx + min(batch_size, len(df_competitions) - start_idx)) / len(df_competitions)) * 100
        }
        
        print(f"📈 Progreso: {summary['progress']['processed_competitions']}/{summary['progress']['total_competitions']} ({summary['progress']['percentage']:.1f}%)")
    
    return df_final, summary


# Función de testing
def test_season_scraper():
    """
    Función de prueba para el scraper de temporadas.
    """
    print("=== Testing Season Scraper ===")
    
    try:
        # Probar la función inteligente con pocas competiciones
        print("🧪 Probando smart_scrape_seasons...")
        
        df_test, summary = smart_scrape_seasons(
            restart_from_zero=True,  # Para testing, empezar limpio
            batch_size=2            # Solo 2 competiciones para prueba
        )
        
        print(f"\n📊 Resumen del test:")
        print(f"   - Total temporadas: {summary['total_temporadas']}")
        print(f"   - Competiciones únicas: {summary['competiciones_unicas']}")
        
        if 'progress' in summary:
            print(f"   - Progreso: {summary['progress']['percentage']:.1f}%")
        
        if not df_test.empty:
            print(f"\n🔍 Primeras 3 temporadas:")
            print(df_test.head(3)[['competicion', 'temporada', 'pais']])
        
        return True
        
    except Exception as e:
        print(f"❌ Error en testing: {e}")
        return False


# ========================================
# 🎯 NUEVA FUNCIÓN AGREGADA: Versión simple
# ========================================
def get_seasons_simple(df_competitions: pd.DataFrame, 
                       save_csv: bool = True,
                       filename: str = 'temporadas.csv') -> pd.DataFrame:
    """
    🎯 FUNCIÓN SIMPLE AGREGADA: Obtener temporadas de competiciones.
    
    Esta función se agregó para facilitar el uso desde notebooks.
    Solo requiere pasar el DataFrame de competiciones.
    
    Args:
        df_competitions (pd.DataFrame): DataFrame con las competiciones
        save_csv (bool): Si quieres guardar el CSV (por defecto True)
        filename (str): Nombre del archivo CSV
        
    Returns:
        pd.DataFrame: DataFrame con todas las temporadas
        
    Ejemplo de uso en tu notebook:
        from scraping_seasons import get_seasons_simple
        
        # Opción 1: Simple
        df_seasons = get_seasons_simple(df_competitions)
        display(df_seasons.head())
        
        # Opción 2: Sin guardar CSV
        df_seasons = get_seasons_simple(df_competitions, save_csv=False)
        
        # Opción 3: Con nombre personalizado
        df_seasons = get_seasons_simple(df_competitions, filename='mis_temporadas.csv')
    """
    scraper = SeasonScraper()
    
    try:
        print(f"🚀 Obteniendo temporadas de {len(df_competitions)} competiciones...")
        print("⏱️ Esto puede tomar varios minutos dependiendo de cuántas competiciones tengas")
        print("💡 Tip: Puedes interrumpir con Ctrl+C y los datos hasta ese momento se guardarán\n")
        
        # Hacer el scraping
        df_seasons = scraper.scrape_all_seasons(df_competitions)
        
        # Guardar si se solicita
        if save_csv and not df_seasons.empty:
            scraper.save_seasons_csv(df_seasons, filename)
        
        # Mostrar resumen
        if not df_seasons.empty:
            print(f"\n🎉 ¡Listo! Se encontraron {len(df_seasons)} temporadas")
            print(f"📊 De {df_seasons['competicion'].nunique()} competiciones diferentes")
            if save_csv:
                print(f"📁 Guardado en: data/{filename}")
        else:
            print("⚠️ No se encontraron temporadas")
        
        return df_seasons
        
    except KeyboardInterrupt:
        print("\n⚠️ Proceso interrumpido por el usuario")
        # Retornar lo que se haya procesado hasta el momento
        return pd.DataFrame()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise


if __name__ == "__main__":
    test_season_scraper()