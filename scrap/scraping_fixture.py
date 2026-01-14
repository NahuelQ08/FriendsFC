"""
Scraping de fixtures deportivos
==============================

Este módulo se encarga de descargar y guardar los datos de fixtures (calendario de partidos)
desde la API de ScoresWay, organizando los archivos JSON en la estructura de directorios
creada previamente.

Autor: Tu nombre
Fecha: Julio 2025
"""

import os
import re
import json
import time
import random
import requests
import pandas as pd
from urllib.parse import quote, urlparse, parse_qs
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from typing import Dict, List, Optional, Tuple, Union

# Importar funciones comunes
from utils_common import get_season_name_from_url, get_torneo_id, random_sleep_time


class FixtureScraper:
    """
    Clase para hacer scraping de fixtures deportivos desde la API de ScoresWay.
    """
    
    def __init__(self, 
                 sdapi_outlet_key: str = 'ft1tiv1inq7v1sk3y9tv12yh5',
                 callback_id: str = 'W3e14cbc3e4b2577e854bf210e5a3c7028c7409678',
                 base_url: str = "https://www.scoresway.com"):
        """
        Inicializa el scraper de fixtures.
        
        Args:
            sdapi_outlet_key (str): Clave del outlet para la API
            callback_id (str): ID del callback para JSONP
            base_url (str): URL base del sitio web
        """
        self.sdapi_outlet_key = sdapi_outlet_key
        self.callback_id = callback_id
        self.base_url = base_url
        self.api_base_url = "https://api.performfeeds.com/soccerdata/match"
        self.data_dir = 'data'
        
        # Configurar headers
        self.headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36'
        }
        
        # Configurar sesión con reintentos
        self.session = self._create_session_with_retries()
        
        # Configuración de delays
        self.min_delay = 1.0
        self.max_delay = 2.0
    
    def _create_session_with_retries(self) -> requests.Session:
        """
        Crea una sesión de requests con estrategia de reintentos.
        
        Returns:
            requests.Session: Sesión configurada
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504, 429]  # Incluir 429 (Too Many Requests)
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def set_api_credentials(self, sdapi_outlet_key: str, callback_id: str) -> None:
        """
        Actualiza las credenciales de la API.
        
        Args:
            sdapi_outlet_key (str): Nueva clave del outlet
            callback_id (str): Nuevo ID del callback
        """
        self.sdapi_outlet_key = sdapi_outlet_key
        self.callback_id = callback_id
        print(f"✅ Credenciales actualizadas")
    
    def set_delay_range(self, min_delay: float, max_delay: float) -> None:
        """
        Configura el rango de delays entre requests.
        
        Args:
            min_delay (float): Delay mínimo en segundos
            max_delay (float): Delay máximo en segundos
        """
        self.min_delay = min_delay
        self.max_delay = max_delay
    
    def obtener_fixture_json(self, torneo_id: str, competicion_name: str, referer: str = None) -> Dict:
        """
        Obtiene los datos de fixture desde la API.
        
        Args:
            torneo_id (str): ID del torneo
            competicion_name (str): Nombre de la competición
            referer (str): URL de referencia
            
        Returns:
            Dict: Datos del fixture en formato JSON
            
        Raises:
            Exception: Si hay error en la petición o parsing
        """
        try:
            # Configurar referer
            if not referer:
                referer_base = f'{self.base_url}/en_GB/soccer/'
                safe_competition_name = quote(competicion_name)
                referer = f"{referer_base}{safe_competition_name}/{torneo_id}/fixtures"
            
            # Construir URL de la API
            fixture_url = (
                f"{self.api_base_url}/{self.sdapi_outlet_key}/"
                f"?_rt=c&tmcl={torneo_id}&live=yes&_pgSz=400&_lcl=en&_fmt=jsonp"
                f"&sps=widgets&_clbk={self.callback_id}"
            )
            
            # Actualizar headers con referer
            headers = self.headers.copy()
            headers['Referer'] = referer
            
            print(f"🌐 API URL: {fixture_url}")
            
            # Realizar petición
            response = self.session.get(fixture_url, headers=headers)
            response.raise_for_status()
            
            # Limpiar JSONP y extraer JSON puro
            content = response.text
            json_start = content.find('(') + 1
            json_end = content.rfind(')')
            
            if json_start <= 0 or json_end <= json_start:
                raise Exception("No se pudo extraer JSON del response JSONP")
            
            fixture_data = json.loads(content[json_start:json_end])
            
            return fixture_data
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error al realizar petición: {e}")
        except json.JSONDecodeError as e:
            raise Exception(f"Error al parsear JSON: {e}")
        except Exception as e:
            raise Exception(f"Error inesperado: {e}")
    
    def save_fixture_json(self, season_row: pd.Series, skip_existing: bool = True) -> bool:
        """
        Descarga y guarda el fixture JSON para una temporada.
        
        Args:
            season_row (pd.Series): Fila del DataFrame de temporadas
            skip_existing (bool): Si saltar archivos que ya existen
            
        Returns:
            bool: True si se guardó exitosamente, False en caso contrario
        """
        try:
            # Obtener ID del torneo
            torneo_id = get_torneo_id(season_row['url_temporada'])
            if not torneo_id:
                print(f"⚠️  No se pudo extraer torneo_id de: {season_row['url_temporada']}")
                return False
            
            # Obtener nombre de temporada
            season_name = get_season_name_from_url(season_row['url_resultados'])
            if not season_name:
                print(f"⚠️  No se pudo extraer season_name de: {season_row['url_resultados']}")
                return False
            
            # Crear nombres de directorio seguros
            continente_dir = str(season_row['continente']).replace('/', '_')
            pais_dir = str(season_row['pais']).replace('/', '_')
            competicion = str(season_row['competicion']).replace('/', '_')
            competicion_dir = f"{competicion}_{season_row['id_competicion']}"
            
            # Construir ruta del directorio
            dir_path = os.path.join(
                self.data_dir,
                continente_dir,
                pais_dir,
                competicion_dir,
                season_name
            )
            
            # Verificar si el directorio existe
            if not os.path.exists(dir_path):
                print(f"⚠️  Directorio no existe: {dir_path}")
                return False
            
            # Ruta del archivo JSON
            json_path = os.path.join(dir_path, 'fixture.json')
            
            # Si el archivo ya existe y skip_existing es True, saltarlo
            if skip_existing and os.path.exists(json_path):
                print(f"⏭️  Archivo ya existe (saltando): {json_path}")
                return True
            
            # Obtener datos del fixture
            fixture_data = self.obtener_fixture_json(
                torneo_id=torneo_id,
                competicion_name=competicion,
                referer=season_row['url_temporada']
            )
            
            # Guardar el JSON
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(fixture_data, f, ensure_ascii=False, indent=2)
            
            print(f"✅ Fixture guardado: {json_path}")
            
            # Delay entre peticiones
            delay = random.uniform(self.min_delay, self.max_delay)
            time.sleep(delay)
            
            return True
            
        except Exception as e:
            print(f"❌ Error al procesar {season_row.get('temporada', 'N/A')}: {str(e)}")
            return False
    
    def process_seasons(self, 
                       df_seasons: pd.DataFrame,
                       filters: Optional[Dict] = None,
                       skip_existing: bool = True,
                       start_index: int = 0,
                       limit: Optional[int] = None) -> Dict:
        """
        Procesa múltiples temporadas para descargar fixtures.
        
        Args:
            df_seasons (pd.DataFrame): DataFrame con temporadas
            filters (Dict, optional): Filtros para aplicar (continente, pais, competicion)
            skip_existing (bool): Si saltar archivos existentes
            start_index (int): Índice de inicio
            limit (Optional[int]): Límite de temporadas a procesar
            
        Returns:
            Dict: Estadísticas del procesamiento
        """
        try:
            # Aplicar filtros si se proporcionan
            df_filtered = self._apply_filters(df_seasons, filters)
            
            # Determinar rango de procesamiento
            end_index = len(df_filtered)
            if limit:
                end_index = min(start_index + limit, end_index)
            
            df_to_process = df_filtered.iloc[start_index:end_index]
            
            print(f"🚀 Iniciando descarga de fixtures...")
            print(f"   - Temporadas originales: {len(df_seasons)}")
            if filters:
                print(f"   - Temporadas después de filtros: {len(df_filtered)}")
            print(f"   - Temporadas a procesar: {len(df_to_process)}")
            print(f"   - Rango: {start_index} a {end_index-1}")
            
            # Estadísticas
            stats = {
                'total_seasons': len(df_to_process),
                'processed': 0,
                'success': 0,
                'skipped': 0,
                'errors': 0,
                'start_time': time.time()
            }
            
            # Procesar cada temporada
            for idx, (_, row) in enumerate(df_to_process.iterrows()):
                try:
                    competition_info = f"{row.get('competicion', 'N/A')} - {row.get('temporada', 'N/A')}"
                    print(f"\n📋 Procesando {idx + 1}/{len(df_to_process)}: {competition_info}")
                    
                    # Intentar guardar fixture
                    result = self.save_fixture_json(row, skip_existing)
                    
                    stats['processed'] += 1
                    
                    if result:
                        if skip_existing and os.path.exists(self._get_json_path(row)):
                            stats['skipped'] += 1
                        else:
                            stats['success'] += 1
                    else:
                        stats['errors'] += 1
                    
                    # Mostrar progreso cada 10 elementos
                    if (idx + 1) % 10 == 0:
                        self._print_progress(stats, idx + 1, len(df_to_process))
                
                except KeyboardInterrupt:
                    print(f"\n⚠️  Procesamiento interrumpido por el usuario")
                    break
                except Exception as e:
                    print(f"❌ Error inesperado en temporada {idx}: {e}")
                    stats['errors'] += 1
            
            # Calcular tiempo total
            stats['duration'] = time.time() - stats['start_time']
            
            # Imprimir resumen final
            self._print_final_summary(stats)
            
            return stats
            
        except Exception as e:
            print(f"❌ Error en procesamiento: {e}")
            raise
    
    def _apply_filters(self, df: pd.DataFrame, filters: Optional[Dict]) -> pd.DataFrame:
        """
        Aplica filtros al DataFrame de temporadas.
        
        Args:
            df (pd.DataFrame): DataFrame original
            filters (Dict, optional): Filtros a aplicar
            
        Returns:
            pd.DataFrame: DataFrame filtrado
        """
        if not filters:
            return df
        
        df_filtered = df.copy()
        
        for column, value in filters.items():
            if column in df_filtered.columns:
                if isinstance(value, list):
                    df_filtered = df_filtered[df_filtered[column].isin(value)]
                else:
                    df_filtered = df_filtered[df_filtered[column] == value]
                print(f"🔍 Filtro aplicado - {column}: {value} → {len(df_filtered)} temporadas")
        
        return df_filtered
    
    def _get_json_path(self, row: pd.Series) -> str:
        """
        Construye la ruta del archivo JSON para una temporada.
        
        Args:
            row (pd.Series): Fila de temporada
            
        Returns:
            str: Ruta del archivo JSON
        """
        season_name = get_season_name_from_url(row['url_resultados'])
        continente_dir = str(row['continente']).replace('/', '_')
        pais_dir = str(row['pais']).replace('/', '_')
        competicion = str(row['competicion']).replace('/', '_')
        competicion_dir = f"{competicion}_{row['id_competicion']}"
        
        return os.path.join(
            self.data_dir,
            continente_dir,
            pais_dir,
            competicion_dir,
            season_name,
            'fixture.json'
        )
    
    def _print_progress(self, stats: Dict, current: int, total: int) -> None:
        """
        Imprime el progreso del procesamiento.
        """
        elapsed = time.time() - stats['start_time']
        rate = current / elapsed if elapsed > 0 else 0
        
        print(f"\n📊 Progreso: {current}/{total} ({current/total*100:.1f}%)")
        print(f"   ⚡ Velocidad: {rate:.2f} temporadas/min")
        print(f"   ✅ Exitosas: {stats['success']}")
        print(f"   ⏭️  Saltadas: {stats['skipped']}")
        print(f"   ❌ Errores: {stats['errors']}")
    
    def _print_final_summary(self, stats: Dict) -> None:
        """
        Imprime el resumen final del procesamiento.
        """
        print(f"\n" + "="*50)
        print(f"📊 RESUMEN FINAL")
        print(f"="*50)
        print(f"⏰ Tiempo total: {stats['duration']:.1f} segundos")
        print(f"📋 Temporadas procesadas: {stats['processed']}/{stats['total_seasons']}")
        print(f"✅ Exitosas: {stats['success']}")
        print(f"⏭️  Saltadas (ya existían): {stats['skipped']}")
        print(f"❌ Con errores: {stats['errors']}")
        
        if stats['duration'] > 0:
            rate = stats['processed'] / (stats['duration'] / 60)
            print(f"⚡ Velocidad promedio: {rate:.2f} temporadas/minuto")


# Funciones de conveniencia para usar directamente
def download_fixtures_by_filters(df_seasons: pd.DataFrame,
                                continente: Optional[str] = None,
                                pais: Optional[str] = None,
                                competicion: Optional[str] = None,
                                skip_existing: bool = True,
                                start_index: int = 0,
                                limit: Optional[int] = None,
                                **scraper_kwargs) -> Dict:
    """
    Descarga fixtures aplicando filtros comunes.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        continente (str, optional): Filtrar por continente
        pais (str, optional): Filtrar por país
        competicion (str, optional): Filtrar por competición
        skip_existing (bool): Si saltar archivos existentes
        start_index (int): Índice de inicio para procesar
        limit (Optional[int]): Límite de temporadas a procesar
        **scraper_kwargs: Argumentos adicionales para FixtureScraper (sdapi_outlet_key, callback_id, etc.)
        
    Returns:
        Dict: Estadísticas del procesamiento
    """
    # Crear filtros
    filters = {}
    if continente:
        filters['continente'] = continente
    if pais:
        filters['pais'] = pais
    if competicion:
        filters['competicion'] = competicion
    
    # Crear scraper con argumentos apropiados
    scraper = FixtureScraper(**scraper_kwargs)
    
    # Procesar con todos los parámetros
    return scraper.process_seasons(
        df_seasons, 
        filters=filters, 
        skip_existing=skip_existing,
        start_index=start_index,
        limit=limit
    )


def download_all_fixtures(df_seasons: pd.DataFrame, 
                         skip_existing: bool = True,
                         start_index: int = 0,
                         limit: Optional[int] = None,
                         **scraper_kwargs) -> Dict:
    """
    Descarga fixtures para todas las temporadas.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        skip_existing (bool): Si saltar archivos existentes
        start_index (int): Índice de inicio para procesar
        limit (Optional[int]): Límite de temporadas a procesar
        **scraper_kwargs: Argumentos adicionales para FixtureScraper (sdapi_outlet_key, callback_id, etc.)
        
    Returns:
        Dict: Estadísticas del procesamiento
    """
    scraper = FixtureScraper(**scraper_kwargs)
    return scraper.process_seasons(
        df_seasons, 
        skip_existing=skip_existing,
        start_index=start_index,
        limit=limit
    )


def find_fixture_resume_index(df_seasons: pd.DataFrame, filters: Optional[Dict] = None) -> int:
    """
    Encuentra el índice desde donde continuar basándose en fixtures ya descargados.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        filters (Dict, optional): Filtros aplicados para determinar el subconjunto
        
    Returns:
        int: Índice desde donde continuar
    """
    try:
        # Aplicar filtros si se proporcionan
        scraper = FixtureScraper()
        df_filtered = scraper._apply_filters(df_seasons, filters) if filters else df_seasons
        
        # Revisar qué fixtures ya existen
        for idx, row in df_filtered.iterrows():
            try:
                json_path = scraper._get_json_path(row)
                if not os.path.exists(json_path):
                    print(f"🔍 Primer fixture faltante encontrado en índice {idx}")
                    print(f"   Competición: {row.get('competicion', 'N/A')}")
                    print(f"   Temporada: {row.get('temporada', 'N/A')}")
                    print(f"   País: {row.get('pais', 'N/A')}")
                    return idx
            except:
                # Si hay error al construir la ruta, significa que falta
                return idx
        
        print("✅ Todos los fixtures ya fueron descargados")
        return len(df_filtered)
        
    except Exception as e:
        print(f"⚠️  Error al buscar punto de reanudación: {e}")
        return 0


def smart_download_fixtures(df_seasons: pd.DataFrame,
                           continente: Optional[str] = None,
                           pais: Optional[str] = None,
                           competicion: Optional[str] = None,
                           restart_from_zero: bool = False,
                           batch_size: int = 100,
                           **scraper_kwargs) -> Dict:
    """
    Función inteligente que detecta automáticamente desde dónde continuar la descarga de fixtures.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        continente (str, optional): Filtrar por continente
        pais (str, optional): Filtrar por país
        competicion (str, optional): Filtrar por competición
        restart_from_zero (bool): Si True, borra fixtures existentes y empieza de cero
        batch_size (int): Número de temporadas a procesar en este lote
        **scraper_kwargs: Argumentos adicionales para FixtureScraper
        
    Returns:
        Dict: Estadísticas del procesamiento
        
    Example:
        # Para empezar de cero:
        stats = smart_download_fixtures(df_seasons, pais='Argentina', restart_from_zero=True)
        
        # Para continuar automáticamente:
        stats = smart_download_fixtures(df_seasons, pais='Argentina', restart_from_zero=False)
    """
    import shutil
    
    # Crear filtros
    filters = {}
    if continente:
        filters['continente'] = continente
    if pais:
        filters['pais'] = pais
    if competicion:
        filters['competicion'] = competicion
    
    # Aplicar filtros para obtener subconjunto
    scraper = FixtureScraper(**scraper_kwargs)
    df_filtered = scraper._apply_filters(df_seasons, filters) if filters else df_seasons
    
    print(f"🎯 Filtros aplicados: {filters if filters else 'Ninguno'}")
    print(f"📋 Temporadas a procesar: {len(df_filtered)}")
    
    # Si restart_from_zero, borrar archivos existentes
    if restart_from_zero:
        deleted_count = 0
        print("🔥 Modo reinicio: Borrando fixtures existentes...")
        
        for _, row in df_filtered.iterrows():
            try:
                json_path = scraper._get_json_path(row)
                if os.path.exists(json_path):
                    os.remove(json_path)
                    deleted_count += 1
            except:
                pass
        
        print(f"🗑️  Eliminados {deleted_count} fixtures existentes")
        start_index = 0
    else:
        # Encontrar desde dónde continuar
        start_index = find_fixture_resume_index(df_seasons, filters)
        
        if start_index >= len(df_filtered):
            print("✅ Todos los fixtures ya están descargados")
            # Calcular estadísticas de lo existente
            existing_count = 0
            for _, row in df_filtered.iterrows():
                try:
                    json_path = scraper._get_json_path(row)
                    if os.path.exists(json_path):
                        existing_count += 1
                except:
                    pass
            
            return {
                'total_seasons': len(df_filtered),
                'processed': len(df_filtered),
                'success': existing_count,
                'skipped': existing_count,
                'errors': len(df_filtered) - existing_count,
                'duration': 0,
                'progress': {
                    'processed_seasons': len(df_filtered),
                    'total_seasons': len(df_filtered),
                    'percentage': 100.0
                }
            }
    
    print(f"🚀 Empezando descarga desde temporada {start_index + 1}/{len(df_filtered)}")
    
    # Calcular índices relativos al DataFrame filtrado
    start_idx_filtered = 0
    for idx, (orig_idx, row) in enumerate(df_filtered.iterrows()):
        if orig_idx >= df_seasons.index[start_index]:
            start_idx_filtered = idx
            break
    
    # Procesar fixtures
    stats = download_fixtures_by_filters(
        df_seasons,
        continente=continente,
        pais=pais,
        competicion=competicion,
        skip_existing=not restart_from_zero,  # Si restart, no skip
        start_index=start_index,
        limit=batch_size,
        **scraper_kwargs
    )
    
    # Agregar información de progreso
    if 'progress' not in stats:
        stats['progress'] = {}
    
    stats['progress'].update({
        'processed_seasons': min(start_index + batch_size, len(df_filtered)),
        'total_seasons': len(df_filtered),
        'percentage': (min(start_index + batch_size, len(df_filtered)) / len(df_filtered)) * 100
    })
    
    print(f"\n📈 Progreso total: {stats['progress']['processed_seasons']}/{stats['progress']['total_seasons']} ({stats['progress']['percentage']:.1f}%)")
    
    return stats


def resume_fixtures_download(df_seasons: pd.DataFrame,
                            continente: Optional[str] = None,
                            pais: Optional[str] = None,
                            competicion: Optional[str] = None,
                            batch_size: int = 100,
                            **scraper_kwargs) -> Dict:
    """
    Continúa la descarga de fixtures desde donde se quedó automáticamente.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        continente (str, optional): Filtrar por continente
        pais (str, optional): Filtrar por país
        competicion (str, optional): Filtrar por competición
        batch_size (int): Número de temporadas a procesar
        **scraper_kwargs: Argumentos adicionales para FixtureScraper
        
    Returns:
        Dict: Estadísticas del procesamiento
    """
    return smart_download_fixtures(
        df_seasons,
        continente=continente,
        pais=pais,
        competicion=competicion,
        restart_from_zero=False,
        batch_size=batch_size,
        **scraper_kwargs
    )


# Función de testing
def test_fixture_scraper():
    """
    Función de prueba para el scraper de fixtures.
    """
    print("=== Testing Fixture Scraper ===")
    
    try:
        # Cargar temporadas existentes
        from scraping_seasons import load_existing_seasons
        
        df_seasons = load_existing_seasons()
        print(f"📋 Temporadas cargadas: {len(df_seasons)}")
        
        # Probar la función inteligente con Argentina (pocas temporadas)
        print("🧪 Probando smart_download_fixtures...")
        
        stats = smart_download_fixtures(
            df_seasons,
            pais='Argentina',
            restart_from_zero=False,  # No borrar para testing
            batch_size=3            # Solo 3 temporadas para prueba
        )
        
        print(f"\n📊 Resultado del test:")
        print(f"   - Procesadas: {stats['processed']}")
        print(f"   - Exitosas: {stats['success']}")
        print(f"   - Saltadas: {stats['skipped']}")
        print(f"   - Errores: {stats['errors']}")
        
        if 'progress' in stats:
            print(f"   - Progreso: {stats['progress']['percentage']:.1f}%")
        
        return stats['errors'] == 0
        
    except Exception as e:
        print(f"❌ Error en testing: {e}")
        return False


if __name__ == "__main__":
    test_fixture_scraper()