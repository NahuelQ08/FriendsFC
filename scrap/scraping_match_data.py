"""
Scraping de datos de partidos deportivos
=======================================

Este módulo se encarga de descargar datos detallados de partidos deportivos
desde la API de ScoresWay, organizando los archivos JSON por directorios
de competición y temporada. Este endpoint proporciona información más 
detallada o actualizada que el endpoint de fixtures.

Autor: Tu nombre
Fecha: Julio 2025
"""

import os
import json
import time
import random
import requests
import pandas as pd
from urllib.parse import quote
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from typing import Dict, List, Optional, Tuple, Union

# Importar funciones comunes
from utils_common import get_season_name_from_url, get_torneo_id, sanitize_dir_name


class MatchDataScraper:
    """
    Clase para hacer scraping de datos de partidos desde la API de ScoresWay.
    """
    
    def __init__(self, 
                 sdapi_outlet_key: str = 'ft1tiv1inq7v1sk3y9tv12yh5',
                 callback_id: str = 'W3e14cbc3e4b2577e854bf210e5a3c7028c7409678',
                 base_url: str = "https://www.scoresway.com",
                 data_dir: str = 'data'):
        """
        Inicializa el scraper de datos de partidos.
        
        Args:
            sdapi_outlet_key (str): Clave del outlet para la API
            callback_id (str): ID del callback para JSONP
            base_url (str): URL base del sitio web
            data_dir (str): Directorio base de datos
        """
        self.sdapi_outlet_key = sdapi_outlet_key
        self.callback_id = callback_id
        self.base_url = base_url
        self.api_base_url = "https://api.performfeeds.com/soccerdata/match"
        self.data_dir = data_dir
        
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
        
        # Contadores para estadísticas
        self.reset_stats()
    
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
            status_forcelist=[500, 502, 503, 504, 429],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def reset_stats(self) -> None:
        """Reinicia las estadísticas de descarga."""
        self.exitos = 0
        self.fallos = 0
        self.saltados = 0
        self.procesados = 0
    
    def set_api_credentials(self, sdapi_outlet_key: str, callback_id: str) -> None:
        """
        Actualiza las credenciales de la API.
        
        Args:
            sdapi_outlet_key (str): Nueva clave del outlet
            callback_id (str): Nuevo ID del callback
        """
        self.sdapi_outlet_key = sdapi_outlet_key
        self.callback_id = callback_id
        print(f"✅ Credenciales de API actualizadas")
    
    def set_delay_range(self, min_delay: float, max_delay: float) -> None:
        """
        Configura el rango de delays entre requests.
        
        Args:
            min_delay (float): Delay mínimo en segundos
            max_delay (float): Delay máximo en segundos
        """
        self.min_delay = min_delay
        self.max_delay = max_delay
        print(f"⏱️  Delays configurados: {min_delay}-{max_delay} segundos")
    
    def obtener_match_json(self, torneo_id: str, competicion_name: str, referer: str = None) -> Dict:
        """
        Obtiene los datos de partidos desde la API.
        
        Args:
            torneo_id (str): ID del torneo
            competicion_name (str): Nombre de la competición
            referer (str): URL de referencia
            
        Returns:
            Dict: Datos de partidos en formato JSON
            
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
            match_url = (
                f"{self.api_base_url}/{self.sdapi_outlet_key}/"
                f"?_rt=c&tmcl={torneo_id}&live=yes&_lcl=en&_fmt=jsonp"
                f"&sps=widgets&_clbk={self.callback_id}"
            )
            
            # Actualizar headers con referer
            headers = self.headers.copy()
            headers['Referer'] = referer
            
            print(f"⚽ API URL: {match_url}")
            
            # Realizar petición
            response = self.session.get(match_url, headers=headers)
            response.raise_for_status()
            
            # Limpiar JSONP y extraer JSON puro
            content = response.text
            json_start = content.find('(') + 1
            json_end = content.rfind(')')
            
            if json_start <= 0 or json_end <= json_start:
                raise Exception("No se pudo extraer JSON del response JSONP")
            
            match_data = json.loads(content[json_start:json_end])
            
            return match_data
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error al realizar petición: {e}")
        except json.JSONDecodeError as e:
            raise Exception(f"Error al parsear JSON: {e}")
        except Exception as e:
            raise Exception(f"Error inesperado: {e}")
    
    def save_match_json(self, season_row: pd.Series, skip_existing: bool = True) -> bool:
        """
        Descarga y guarda el match JSON para una temporada.
        
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
            continente_dir = sanitize_dir_name(season_row['continente'])
            pais_dir = sanitize_dir_name(season_row['pais'])
            competicion = sanitize_dir_name(season_row['competicion'])
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
            json_path = os.path.join(dir_path, 'match.json')
            
            # Si el archivo ya existe y skip_existing es True, saltarlo
            if skip_existing and os.path.exists(json_path):
                print(f"⏭️  Archivo ya existe (saltando): {json_path}")
                self.saltados += 1
                return True
            
            # Obtener datos de partidos
            match_data = self.obtener_match_json(
                torneo_id=torneo_id,
                competicion_name=competicion,
                referer=season_row['url_temporada']
            )
            
            # Guardar el JSON
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(match_data, f, ensure_ascii=False, indent=2)
            
            print(f"✅ Match guardado: {json_path}")
            self.exitos += 1
            
            # Delay entre peticiones
            delay = random.uniform(self.min_delay, self.max_delay)
            time.sleep(delay)
            
            return True
            
        except Exception as e:
            print(f"❌ Error al procesar {season_row.get('temporada', 'N/A')}: {str(e)}")
            self.fallos += 1
            return False
    
    def process_seasons(self, 
                       df_seasons: pd.DataFrame,
                       filters: Optional[Dict] = None,
                       skip_existing: bool = True,
                       start_index: int = 0,
                       limit: Optional[int] = None) -> Dict:
        """
        Procesa múltiples temporadas para descargar datos de partidos.
        
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
            # Reiniciar estadísticas
            self.reset_stats()
            start_time = time.time()
            
            # Aplicar filtros si se proporcionan
            df_filtered = self._apply_filters(df_seasons, filters)
            
            # Determinar rango de procesamiento
            end_index = len(df_filtered)
            if limit:
                end_index = min(start_index + limit, end_index)
            
            df_to_process = df_filtered.iloc[start_index:end_index]
            
            print(f"🚀 Iniciando descarga de datos de partidos...")
            print(f"   - Temporadas originales: {len(df_seasons)}")
            if filters:
                print(f"   - Temporadas después de filtros: {len(df_filtered)}")
            print(f"   - Temporadas a procesar: {len(df_to_process)}")
            print(f"   - Rango: {start_index} a {end_index-1}")
            print(f"   - Delay: {self.min_delay}-{self.max_delay} segundos")
            
            # Procesar cada temporada
            for idx, (_, row) in enumerate(df_to_process.iterrows()):
                try:
                    competition_info = f"{row.get('competicion', 'N/A')} - {row.get('temporada', 'N/A')}"
                    print(f"\n📋 Procesando {idx + 1}/{len(df_to_process)}: {competition_info}")
                    
                    # Intentar guardar datos de partidos
                    self.save_match_json(row, skip_existing)
                    self.procesados += 1
                    
                    # Mostrar progreso cada 10 elementos
                    if (idx + 1) % 10 == 0:
                        self._print_progress(idx + 1, len(df_to_process), start_time)
                
                except KeyboardInterrupt:
                    print(f"\n⚠️  Procesamiento interrumpido por el usuario")
                    break
                except Exception as e:
                    print(f"❌ Error inesperado en temporada {idx}: {e}")
                    self.fallos += 1
                    self.procesados += 1
            
            # Calcular tiempo total
            duration = time.time() - start_time
            
            # Crear estadísticas finales
            stats = {
                'total_seasons': len(df_to_process),
                'procesados': self.procesados,
                'exitosos': self.exitos,
                'saltados': self.saltados,
                'fallos': self.fallos,
                'duration': duration,
                'temporadas_por_minuto': (self.procesados / (duration / 60)) if duration > 0 else 0
            }
            
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
    
    def _print_progress(self, current: int, total: int, start_time: float) -> None:
        """
        Imprime el progreso del procesamiento.
        """
        elapsed = time.time() - start_time
        rate = self.procesados / (elapsed / 60) if elapsed > 0 else 0
        
        print(f"\n📊 Progreso: {current}/{total} ({current/total*100:.1f}%)")
        print(f"   ⚡ Velocidad: {rate:.2f} temporadas/minuto")
        print(f"   ✅ Exitosas: {self.exitos}")
        print(f"   ⏭️  Saltadas: {self.saltados}")
        print(f"   ❌ Errores: {self.fallos}")
        
        if elapsed > 0:
            remaining = (total - current) * (elapsed / current)
            print(f"   ⏰ Tiempo estimado restante: {remaining/60:.1f} minutos")
    
    def _print_final_summary(self, stats: Dict) -> None:
        """
        Imprime el resumen final del procesamiento.
        """
        print(f"\n" + "="*60)
        print(f"⚽ RESUMEN FINAL - DESCARGA DE DATOS DE PARTIDOS")
        print(f"="*60)
        print(f"⏰ Tiempo total: {stats['duration']:.1f} segundos ({stats['duration']/60:.1f} minutos)")
        print(f"📋 Temporadas procesadas: {stats['procesados']}/{stats['total_seasons']}")
        print(f"✅ Descargas exitosas: {stats['exitosos']}")
        print(f"⏭️  Archivos saltados (ya existían): {stats['saltados']}")
        print(f"❌ Fallos: {stats['fallos']}")
        print(f"⚡ Velocidad promedio: {stats['temporadas_por_minuto']:.2f} temporadas/minuto")
        
        if stats['exitosos'] > 0:
            success_rate = (stats['exitosos'] / stats['procesados']) * 100
            print(f"📈 Tasa de éxito: {success_rate:.1f}%")


def find_match_resume_index(df_seasons: pd.DataFrame, filters: Optional[Dict] = None) -> int:
    """
    Encuentra el índice desde donde continuar basándose en datos de partidos ya descargados.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        filters (Dict, optional): Filtros aplicados
        
    Returns:
        int: Índice desde donde continuar
    """
    try:
        scraper = MatchDataScraper()
        
        # Aplicar filtros si se proporcionan
        df_filtered = scraper._apply_filters(df_seasons, filters) if filters else df_seasons
        
        # Revisar qué datos de partidos ya existen
        for idx, row in df_filtered.iterrows():
            try:
                # Obtener nombre de temporada
                season_name = get_season_name_from_url(row['url_resultados'])
                if not season_name:
                    continue
                
                # Crear nombres de directorio seguros
                continente_dir = sanitize_dir_name(row['continente'])
                pais_dir = sanitize_dir_name(row['pais'])
                competicion = sanitize_dir_name(row['competicion'])
                competicion_dir = f"{competicion}_{row['id_competicion']}"
                
                # Construir ruta del archivo
                dir_path = os.path.join(
                    'data',
                    continente_dir,
                    pais_dir,
                    competicion_dir,
                    season_name
                )
                json_path = os.path.join(dir_path, 'match.json')
                
                if not os.path.exists(json_path):
                    print(f"🔍 Primer match data faltante encontrado en índice {idx}")
                    print(f"   Competición: {row.get('competicion', 'N/A')}")
                    print(f"   Temporada: {row.get('temporada', 'N/A')}")
                    print(f"   País: {row.get('pais', 'N/A')}")
                    return idx
                    
            except Exception as e:
                # Si hay error al construir la ruta, significa que falta
                print(f"⚠️  Error procesando temporada {idx}: {e}")
                return idx
        
        print("✅ Todos los datos de partidos ya fueron descargados")
        return len(df_filtered)
        
    except Exception as e:
        print(f"⚠️  Error al buscar punto de reanudación: {e}")
        return 0


# Funciones de conveniencia para usar directamente
def download_match_data_by_filters(df_seasons: pd.DataFrame,
                                  continente: Optional[str] = None,
                                  pais: Optional[str] = None,
                                  competicion: Optional[str] = None,
                                  skip_existing: bool = True,
                                  start_index: int = 0,
                                  limit: Optional[int] = None,
                                  **scraper_kwargs) -> Dict:
    """
    Descarga datos de partidos aplicando filtros comunes.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        continente (str, optional): Filtrar por continente
        pais (str, optional): Filtrar por país
        competicion (str, optional): Filtrar por competición
        skip_existing (bool): Si saltar archivos existentes
        start_index (int): Índice de inicio para procesar
        limit (Optional[int]): Límite de temporadas a procesar
        **scraper_kwargs: Argumentos adicionales para MatchDataScraper
        
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
    scraper = MatchDataScraper(**scraper_kwargs)
    
    # Procesar con todos los parámetros
    return scraper.process_seasons(
        df_seasons, 
        filters=filters, 
        skip_existing=skip_existing,
        start_index=start_index,
        limit=limit
    )


def smart_download_match_data(df_seasons: pd.DataFrame,
                             continente: Optional[str] = None,
                             pais: Optional[str] = None,
                             competicion: Optional[str] = None,
                             restart_from_zero: bool = False,
                             batch_size: int = 100,
                             **scraper_kwargs) -> Dict:
    """
    Función inteligente que detecta automáticamente desde dónde continuar la descarga de datos de partidos.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        continente (str, optional): Filtrar por continente
        pais (str, optional): Filtrar por país
        competicion (str, optional): Filtrar por competición
        restart_from_zero (bool): Si True, borra datos existentes y empieza de cero
        batch_size (int): Número de temporadas a procesar en este lote
        **scraper_kwargs: Argumentos adicionales para MatchDataScraper
        
    Returns:
        Dict: Estadísticas del procesamiento
        
    Example:
        # Para empezar de cero:
        stats = smart_download_match_data(df_seasons, pais='Argentina', restart_from_zero=True)
        
        # Para continuar automáticamente:
        stats = smart_download_match_data(df_seasons, pais='Argentina', restart_from_zero=False)
    """
    # Crear filtros
    filters = {}
    if continente:
        filters['continente'] = continente
    if pais:
        filters['pais'] = pais
    if competicion:
        filters['competicion'] = competicion
    
    # Aplicar filtros para obtener subconjunto
    scraper = MatchDataScraper(**scraper_kwargs)
    df_filtered = scraper._apply_filters(df_seasons, filters) if filters else df_seasons
    
    print(f"🎯 Filtros aplicados: {filters if filters else 'Ninguno'}")
    print(f"📋 Temporadas a procesar: {len(df_filtered)}")
    
    # Si restart_from_zero, borrar archivos existentes
    if restart_from_zero:
        deleted_count = 0
        print("🔥 Modo reinicio: Borrando datos de partidos existentes...")
        
        for _, row in df_filtered.iterrows():
            try:
                # Obtener nombre de temporada
                season_name = get_season_name_from_url(row['url_resultados'])
                if not season_name:
                    continue
                
                # Crear nombres de directorio seguros
                continente_dir = sanitize_dir_name(row['continente'])
                pais_dir = sanitize_dir_name(row['pais'])
                competicion_clean = sanitize_dir_name(row['competicion'])
                competicion_dir = f"{competicion_clean}_{row['id_competicion']}"
                
                # Construir ruta del archivo
                dir_path = os.path.join(
                    'data',
                    continente_dir,
                    pais_dir,
                    competicion_dir,
                    season_name
                )
                json_path = os.path.join(dir_path, 'match.json')
                
                if os.path.exists(json_path):
                    os.remove(json_path)
                    deleted_count += 1
                    
            except Exception as e:
                print(f"⚠️  Error borrando archivo: {e}")
        
        print(f"🗑️  Eliminados {deleted_count} archivos de datos de partidos existentes")
        start_index = 0
    else:
        # Encontrar desde dónde continuar
        start_index = find_match_resume_index(df_seasons, filters)
        
        if start_index >= len(df_filtered):
            print("✅ Todos los datos de partidos ya están descargados")
            return {
                'total_seasons': len(df_filtered),
                'procesados': len(df_filtered),
                'exitosos': len(df_filtered),
                'saltados': len(df_filtered),
                'fallos': 0,
                'duration': 0,
                'temporadas_por_minuto': 0
            }
    
    print(f"🚀 Empezando descarga desde temporada {start_index + 1}/{len(df_filtered)}")
    
    # Procesar datos de partidos
    return scraper.process_seasons(
        df_seasons,
        filters=filters,
        skip_existing=not restart_from_zero,
        start_index=start_index,
        limit=batch_size
    )


# Función de testing
def test_match_data_scraper():
    """
    Función de prueba para el scraper de datos de partidos.
    """
    print("=== Testing Match Data Scraper ===")
    
    try:
        # Cargar temporadas existentes
        from scraping_seasons import load_existing_seasons
        
        df_seasons = load_existing_seasons()
        print(f"📋 Temporadas cargadas: {len(df_seasons)}")
        
        # Probar con Argentina (solo primeras 2 temporadas)
        stats = download_match_data_by_filters(
            df_seasons,
            pais='Argentina',
            skip_existing=True,
            limit=2
        )
        
        print(f"\n📊 Resultado del test:")
        print(f"   - Procesadas: {stats['procesados']}")
        print(f"   - Exitosas: {stats['exitosos']}")
        print(f"   - Errores: {stats['fallos']}")
        
        return stats['fallos'] == 0
        
    except Exception as e:
        print(f"❌ Error en testing: {e}")
        return False


if __name__ == "__main__":
    test_match_data_scraper()