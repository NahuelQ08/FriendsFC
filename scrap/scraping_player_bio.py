"""
Scraping de biografías de jugadores deportivos
============================================

Este módulo se encarga de descargar biografías detalladas de jugadores individuales
desde la API de ScoresWay. Primero lee los archivos squads.json para obtener la lista
de jugadores, luego descarga biografías individuales para cada jugador.

⚠️ ADVERTENCIA: Este es el módulo más intensivo en peticiones ya que descarga
datos individuales de cada jugador (pueden ser cientos por temporada).

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


class PlayerBioScraper:
    """
    Clase para hacer scraping de biografías de jugadores desde la API de ScoresWay.
    """
    
    def __init__(self, 
                 sdapi_outlet_key: str = 'ft1tiv1inq7v1sk3y9tv12yh5',
                 callback_id: str = 'W3e14cbc3e4b2577e854bf210e5a3c7028c7409678',
                 base_url: str = "https://www.scoresway.com",
                 data_dir: str = 'data'):
        """
        Inicializa el scraper de biografías de jugadores.
        
        Args:
            sdapi_outlet_key (str): Clave del outlet para la API
            callback_id (str): ID del callback para JSONP
            base_url (str): URL base del sitio web
            data_dir (str): Directorio base de datos
        """
        self.sdapi_outlet_key = sdapi_outlet_key
        self.callback_id = callback_id
        self.base_url = base_url
        self.api_base_url = "https://api.performfeeds.com/soccerdata/nlgdynamicplayerbio"
        self.data_dir = data_dir
        
        # Configurar headers
        self.headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36'
        }
        
        # Configurar sesión con reintentos
        self.session = self._create_session_with_retries()
        
        # Configuración de delays (más conservadores por el volumen de peticiones)
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
        self.jugadores_procesados = 0
        self.jugadores_exitosos = 0
        self.jugadores_saltados = 0
        self.jugadores_fallidos = 0
    
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
    
    def obtener_playerbio_json(self, torneo_id: str, player_id: str, competicion_name: str, referer: str = None) -> Dict:
        """
        Obtiene la biografía de un jugador desde la API.
        
        Args:
            torneo_id (str): ID del torneo
            player_id (str): ID del jugador
            competicion_name (str): Nombre de la competición
            referer (str): URL de referencia
            
        Returns:
            Dict: Datos de biografía del jugador en formato JSON
            
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
            playerbio_url = (
                f"{self.api_base_url}/{self.sdapi_outlet_key}/"
                f"?prsn={player_id}"
                f"&_rt=c&_fmt=jsonp&_lcl=en-gb&_clbk={self.callback_id}"
            )
            
            # Actualizar headers con referer
            headers = self.headers.copy()
            headers['Referer'] = referer
            
            print(f"👤 API URL: {playerbio_url}")
            
            # Realizar petición
            response = self.session.get(playerbio_url, headers=headers)
            response.raise_for_status()
            
            # Limpiar JSONP y extraer JSON puro
            content = response.text
            json_start = content.find('(') + 1
            json_end = content.rfind(')')
            
            if json_start <= 0 or json_end <= json_start:
                raise Exception("No se pudo extraer JSON del response JSONP")
            
            playerbio_data = json.loads(content[json_start:json_end])
            
            return playerbio_data
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error al realizar petición: {e}")
        except json.JSONDecodeError as e:
            raise Exception(f"Error al parsear JSON: {e}")
        except Exception as e:
            raise Exception(f"Error inesperado: {e}")
    
    def extract_players_from_squads(self, squads_json_path: str) -> pd.DataFrame:
        """
        Extrae información de jugadores del archivo squads.json.
        
        Args:
            squads_json_path (str): Ruta al archivo squads.json
            
        Returns:
            pd.DataFrame: DataFrame con información de jugadores
            
        Raises:
            Exception: Si no se puede leer o procesar el archivo
        """
        try:
            # Leer archivo squads.json
            with open(squads_json_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            
            # Extraer jugadores de todos los equipos
            if 'squad' not in raw:
                raise Exception("No se encontró la clave 'squad' en el archivo")
            
            # Aplanar la lista de jugadores y traer info del club
            players = pd.json_normalize(
                raw["squad"],                      # lista de clubes
                record_path=["person"],            # lista de jugadores dentro de cada club
                meta=["contestantId",              # columnas que «suben» desde el club
                      "contestantClubName"],       # nombre del club
                errors='ignore'                    # ignorar si falta alguna clave
            )

            players = players.rename(columns={"contestantClubName": "Team", "contestantId": "TeamId"})
            
            print(f"✅ Extraídos {len(players)} jugadores del archivo squads.json")
            
            # Verificar que tenemos las columnas necesarias
            required_columns = ['id', 'firstName', 'lastName']
            missing_columns = [col for col in required_columns if col not in players.columns]
            
            if missing_columns:
                print(f"⚠️  Advertencia: Faltan columnas {missing_columns} en algunos jugadores")
                # Rellenar con valores por defecto
                for col in missing_columns:
                    players[col] = players.get(col, 'Unknown')
            
            return players
            
        except FileNotFoundError:
            raise Exception(f"Archivo squads.json no encontrado: {squads_json_path}")
        except json.JSONDecodeError as e:
            raise Exception(f"Error al parsear squads.json: {e}")
        except Exception as e:
            raise Exception(f"Error procesando squads.json: {e}")
    
    def save_playerbio_json(self, season_row: pd.Series, skip_existing: bool = True, limit_players: Optional[int] = None) -> bool:
        """
        Descarga y guarda las biografías de todos los jugadores de una temporada.
        
        Args:
            season_row (pd.Series): Fila del DataFrame de temporadas
            skip_existing (bool): Si saltar archivos que ya existen
            limit_players (Optional[int]): Límite de jugadores a procesar (para testing)
            
        Returns:
            bool: True si se procesó exitosamente, False en caso contrario
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
            
            # Construir ruta del directorio base
            base_dir_path = os.path.join(
                self.data_dir,
                continente_dir,
                pais_dir,
                competicion_dir,
                season_name
            )
            
            # Verificar si el directorio base existe
            if not os.path.exists(base_dir_path):
                print(f"⚠️  Directorio base no existe: {base_dir_path}")
                return False
            
            # Ruta del archivo squads.json
            squads_json_path = os.path.join(base_dir_path, 'squads.json')
            
            # Verificar si existe el archivo squads.json
            if not os.path.exists(squads_json_path):
                print(f"⚠️  Archivo squads.json no encontrado: {squads_json_path}")
                print(f"   💡 Primero debes descargar los planteles para esta temporada")
                return False
            
            # Extraer jugadores del archivo squads.json
            players = self.extract_players_from_squads(squads_json_path)
            
            if players.empty:
                print(f"⚠️  No se encontraron jugadores en {squads_json_path}")
                return False
            
            # Aplicar límite si se especifica (útil para testing)
            if limit_players and limit_players < len(players):
                players = players.head(limit_players)
                print(f"🔒 Limitando a {limit_players} jugadores para testing")
            
            # Crear directorio para biografías de jugadores
            playersbio_dir = os.path.join(base_dir_path, 'playersbio')
            os.makedirs(playersbio_dir, exist_ok=True)
            
            # Procesar cada jugador
            players_exitosos = 0
            players_saltados = 0
            players_fallidos = 0
            
            print(f"👤 Procesando biografías para {len(players)} jugadores...")
            
            for idx, player_row in players.iterrows():
                try:
                    # Obtener información del jugador
                    player_id = str(player_row.get("id", "unknown"))
                    first_name = sanitize_dir_name(str(player_row.get("firstName", "Unknown")))
                    last_name = sanitize_dir_name(str(player_row.get("lastName", "Unknown")))
                    team_name = player_row.get("Team", "Unknown")
                    
                    # Construir nombre del archivo
                    filename = f"{last_name}_{first_name}_{player_id}.json"
                    player_json_path = os.path.join(playersbio_dir, filename)
                    
                    # Si el archivo ya existe y skip_existing es True, saltarlo
                    if skip_existing and os.path.exists(player_json_path):
                        print(f"  ⏭️  Jugador ya existe (saltando): {first_name} {last_name}")
                        players_saltados += 1
                        self.jugadores_saltados += 1
                        continue
                    
                    # Obtener biografía del jugador
                    print(f"  📖 Descargando bio ({idx+1}/{len(players)}): {first_name} {last_name} ({team_name})")
                    playerbio_data = self.obtener_playerbio_json(
                        torneo_id=torneo_id,
                        player_id=player_id,
                        competicion_name=competicion,
                        referer=season_row['url_temporada']
                    )
                    
                    # Guardar el JSON
                    with open(player_json_path, 'w', encoding='utf-8') as f:
                        json.dump(playerbio_data, f, ensure_ascii=False, indent=2)
                    
                    print(f"  ✅ Guardado: {filename}")
                    players_exitosos += 1
                    self.jugadores_exitosos += 1
                    
                    # Delay entre peticiones de jugadores
                    delay = random.uniform(self.min_delay, self.max_delay)
                    time.sleep(delay)
                    
                except Exception as e:
                    print(f"  ❌ Error con jugador {player_row.get('firstName', 'N/A')} {player_row.get('lastName', 'N/A')}: {e}")
                    players_fallidos += 1
                    self.jugadores_fallidos += 1
                
                self.jugadores_procesados += 1
            
            # Resumen de la temporada
            print(f"👤 Temporada completada: {players_exitosos} exitosos, {players_saltados} saltados, {players_fallidos} fallidos")
            
            if players_exitosos > 0:
                self.exitos += 1
                return True
            else:
                self.fallos += 1
                return False
            
        except Exception as e:
            print(f"❌ Error al procesar temporada {season_row.get('temporada', 'N/A')}: {str(e)}")
            self.fallos += 1
            return False
    
    def process_seasons(self, 
                       df_seasons: pd.DataFrame,
                       filters: Optional[Dict] = None,
                       skip_existing: bool = True,
                       start_index: int = 0,
                       limit: Optional[int] = None,
                       limit_players_per_season: Optional[int] = None) -> Dict:
        """
        Procesa múltiples temporadas para descargar biografías de jugadores.
        
        Args:
            df_seasons (pd.DataFrame): DataFrame con temporadas
            filters (Dict, optional): Filtros para aplicar (continente, pais, competicion)
            skip_existing (bool): Si saltar archivos existentes
            start_index (int): Índice de inicio
            limit (Optional[int]): Límite de temporadas a procesar
            limit_players_per_season (Optional[int]): Límite de jugadores por temporada (para testing)
            
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
            
            print(f"🚀 Iniciando descarga de biografías de jugadores...")
            print(f"   - Temporadas originales: {len(df_seasons)}")
            if filters:
                print(f"   - Temporadas después de filtros: {len(df_filtered)}")
            print(f"   - Temporadas a procesar: {len(df_to_process)}")
            print(f"   - Rango: {start_index} a {end_index-1}")
            print(f"   - Delay entre jugadores: {self.min_delay}-{self.max_delay} segundos")
            if limit_players_per_season:
                print(f"   - Límite jugadores por temporada: {limit_players_per_season}")
            print(f"   ⚠️  Requiere archivos squads.json previos")
            print(f"   ⚠️  ADVERTENCIA: Muchas peticiones (puede ser lento)")
            
            # Procesar cada temporada
            for idx, (_, row) in enumerate(df_to_process.iterrows()):
                try:
                    competition_info = f"{row.get('competicion', 'N/A')} - {row.get('temporada', 'N/A')}"
                    print(f"\n📋 Procesando {idx + 1}/{len(df_to_process)}: {competition_info}")
                    
                    # Intentar guardar biografías de jugadores
                    self.save_playerbio_json(row, skip_existing, limit_players_per_season)
                    self.procesados += 1
                    
                    # Mostrar progreso cada 2 elementos (menos frecuente porque cada temporada tiene MUCHOS jugadores)
                    if (idx + 1) % 2 == 0:
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
                'jugadores_procesados': self.jugadores_procesados,
                'jugadores_exitosos': self.jugadores_exitosos,
                'jugadores_saltados': self.jugadores_saltados,
                'jugadores_fallidos': self.jugadores_fallidos,
                'duration': duration,
                'temporadas_por_minuto': (self.procesados / (duration / 60)) if duration > 0 else 0,
                'jugadores_por_minuto': (self.jugadores_procesados / (duration / 60)) if duration > 0 else 0
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
        season_rate = self.procesados / (elapsed / 60) if elapsed > 0 else 0
        player_rate = self.jugadores_procesados / (elapsed / 60) if elapsed > 0 else 0
        
        print(f"\n📊 Progreso: {current}/{total} temporadas ({current/total*100:.1f}%)")
        print(f"   ⚡ Velocidad temporadas: {season_rate:.2f}/minuto")
        print(f"   ⚡ Velocidad jugadores: {player_rate:.2f}/minuto")
        print(f"   ✅ Temporadas exitosas: {self.exitos}")
        print(f"   👤 Jugadores procesados: {self.jugadores_procesados}")
        print(f"   📈 Jugadores exitosos: {self.jugadores_exitosos}")
        print(f"   ❌ Errores: {self.fallos}")
        
        if elapsed > 0:
            remaining = (total - current) * (elapsed / current)
            print(f"   ⏰ Tiempo estimado restante: {remaining/60:.1f} minutos")
    
    def _print_final_summary(self, stats: Dict) -> None:
        """
        Imprime el resumen final del procesamiento.
        """
        print(f"\n" + "="*80)
        print(f"👤 RESUMEN FINAL - DESCARGA DE BIOGRAFÍAS DE JUGADORES")
        print(f"="*80)
        print(f"⏰ Tiempo total: {stats['duration']:.1f} segundos ({stats['duration']/60:.1f} minutos)")
        print(f"📋 Temporadas procesadas: {stats['procesados']}/{stats['total_seasons']}")
        print(f"✅ Temporadas exitosas: {stats['exitosos']}")
        print(f"❌ Temporadas fallidas: {stats['fallos']}")
        print(f"👤 Total jugadores procesados: {stats['jugadores_procesados']}")
        print(f"📈 Jugadores exitosos: {stats['jugadores_exitosos']}")
        print(f"⏭️  Jugadores saltados: {stats['jugadores_saltados']}")
        print(f"💥 Jugadores fallidos: {stats['jugadores_fallidos']}")
        print(f"⚡ Velocidad temporadas: {stats['temporadas_por_minuto']:.2f}/minuto")
        print(f"⚡ Velocidad jugadores: {stats['jugadores_por_minuto']:.2f}/minuto")
        
        if stats['jugadores_procesados'] > 0:
            success_rate = (stats['jugadores_exitosos'] / stats['jugadores_procesados']) * 100
            print(f"📈 Tasa de éxito jugadores: {success_rate:.1f}%")
        
        if stats['procesados'] > 0:
            season_success_rate = (stats['exitosos'] / stats['procesados']) * 100
            print(f"📈 Tasa de éxito temporadas: {season_success_rate:.1f}%")
        
        # Estimación de tiempo para procesar todo
        if stats['jugadores_por_minuto'] > 0:
            est_total_time = (stats['jugadores_procesados'] / stats['jugadores_por_minuto'])
            print(f"⏱️  Tiempo estimado para 1000 jugadores: {1000/stats['jugadores_por_minuto']:.1f} minutos")


def find_playerbio_resume_index(df_seasons: pd.DataFrame, filters: Optional[Dict] = None) -> int:
    """
    Encuentra el índice desde donde continuar basándose en biografías ya descargadas.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        filters (Dict, optional): Filtros aplicados
        
    Returns:
        int: Índice desde donde continuar
    """
    try:
        scraper = PlayerBioScraper()
        
        # Aplicar filtros si se proporcionan
        df_filtered = scraper._apply_filters(df_seasons, filters) if filters else df_seasons
        
        # Revisar qué biografías ya existen
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
                
                # Construir ruta del directorio playersbio
                dir_path = os.path.join(
                    'data',
                    continente_dir,
                    pais_dir,
                    competicion_dir,
                    season_name,
                    'playersbio'
                )
                
                # Si no existe el directorio o está vacío, esta temporada no está procesada
                if not os.path.exists(dir_path) or not os.listdir(dir_path):
                    print(f"🔍 Primera temporada sin biografías encontrada en índice {idx}")
                    print(f"   Competición: {row.get('competicion', 'N/A')}")
                    print(f"   Temporada: {row.get('temporada', 'N/A')}")
                    print(f"   País: {row.get('pais', 'N/A')}")
                    return idx
                    
            except Exception as e:
                # Si hay error al construir la ruta, significa que falta
                print(f"⚠️  Error procesando temporada {idx}: {e}")
                return idx
        
        print("✅ Todas las biografías de jugadores ya fueron descargadas")
        return len(df_filtered)
        
    except Exception as e:
        print(f"⚠️  Error al buscar punto de reanudación: {e}")
        return 0


# Funciones de conveniencia para usar directamente
def download_player_bio_by_filters(df_seasons: pd.DataFrame,
                                  continente: Optional[str] = None,
                                  pais: Optional[str] = None,
                                  competicion: Optional[str] = None,
                                  skip_existing: bool = True,
                                  start_index: int = 0,
                                  limit: Optional[int] = None,
                                  limit_players_per_season: Optional[int] = None,
                                  **scraper_kwargs) -> Dict:
    """
    Descarga biografías de jugadores aplicando filtros comunes.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        continente (str, optional): Filtrar por continente
        pais (str, optional): Filtrar por país
        competicion (str, optional): Filtrar por competición
        skip_existing (bool): Si saltar archivos existentes
        start_index (int): Índice de inicio para procesar
        limit (Optional[int]): Límite de temporadas a procesar
        limit_players_per_season (Optional[int]): Límite de jugadores por temporada
        **scraper_kwargs: Argumentos adicionales para PlayerBioScraper
        
    Returns:
        Dict: Estadísticas del procesamiento
        
    Note:
        ⚠️ ADVERTENCIA: Este proceso puede tomar MUCHO tiempo debido al volumen de peticiones.
        Requiere que los archivos squads.json hayan sido descargados previamente.
        
    Example:
        # Solo procesar 10 jugadores por temporada para testing
        stats = download_player_bio_by_filters(
            df_seasons, 
            pais='Argentina', 
            limit=1,  # Solo 1 temporada
            limit_players_per_season=10  # Solo 10 jugadores
        )
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
    scraper = PlayerBioScraper(**scraper_kwargs)
    
    # Procesar con todos los parámetros
    return scraper.process_seasons(
        df_seasons, 
        filters=filters, 
        skip_existing=skip_existing,
        start_index=start_index,
        limit=limit,
        limit_players_per_season=limit_players_per_season
    )


def smart_download_player_bio(df_seasons: pd.DataFrame,
                             continente: Optional[str] = None,
                             pais: Optional[str] = None,
                             competicion: Optional[str] = None,
                             restart_from_zero: bool = False,
                             batch_size: int = 10,
                             limit_players_per_season: Optional[int] = None,
                             **scraper_kwargs) -> Dict:
    """
    Función inteligente que detecta automáticamente desde dónde continuar la descarga de biografías.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        continente (str, optional): Filtrar por continente
        pais (str, optional): Filtrar por país
        competicion (str, optional): Filtrar por competición
        restart_from_zero (bool): Si True, borra biografías existentes y empieza de cero
        batch_size (int): Número de temporadas a procesar (menor por defecto debido al volumen)
        limit_players_per_season (Optional[int]): Límite de jugadores por temporada
        **scraper_kwargs: Argumentos adicionales para PlayerBioScraper
        
    Returns:
        Dict: Estadísticas del procesamiento
        
    Example:
        # Para empezar de cero con límites de testing:
        stats = smart_download_player_bio(
            df_seasons, 
            pais='Argentina', 
            restart_from_zero=True,
            batch_size=2,  # Solo 2 temporadas
            limit_players_per_season=5  # Solo 5 jugadores por temporada
        )
        
    Warning:
        ⚠️ CUIDADO: Sin límites, este proceso puede tomar HORAS o DÍAS.
        Requiere que los archivos squads.json hayan sido descargados previamente.
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
    scraper = PlayerBioScraper(**scraper_kwargs)
    df_filtered = scraper._apply_filters(df_seasons, filters) if filters else df_seasons
    
    print(f"🎯 Filtros aplicados: {filters if filters else 'Ninguno'}")
    print(f"📋 Temporadas a procesar: {len(df_filtered)}")
    print(f"⚠️  Nota: Requiere archivos squads.json previos")
    if limit_players_per_season:
        print(f"🔒 Límite jugadores por temporada: {limit_players_per_season}")
    else:
        print(f"⚠️  SIN LÍMITE de jugadores - puede tomar MUCHO tiempo")
    
    # Si restart_from_zero, borrar directorios existentes
    if restart_from_zero:
        deleted_count = 0
        print("🔥 Modo reinicio: Borrando biografías de jugadores existentes...")
        
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
                
                # Construir ruta del directorio playersbio
                playersbio_dir = os.path.join(
                    'data',
                    continente_dir,
                    pais_dir,
                    competicion_dir,
                    season_name,
                    'playersbio'
                )
                
                if os.path.exists(playersbio_dir):
                    shutil.rmtree(playersbio_dir)
                    deleted_count += 1
                    
            except Exception as e:
                print(f"⚠️  Error borrando directorio: {e}")
        
        print(f"🗑️  Eliminados {deleted_count} directorios de biografías de jugadores")
        start_index = 0
    else:
        # Encontrar desde dónde continuar
        start_index = find_playerbio_resume_index(df_seasons, filters)
        
        if start_index >= len(df_filtered):
            print("✅ Todas las biografías de jugadores ya están descargadas")
            return {
                'total_seasons': len(df_filtered),
                'procesados': len(df_filtered),
                'exitosos': len(df_filtered),
                'saltados': len(df_filtered),
                'fallos': 0,
                'jugadores_procesados': 0,
                'jugadores_exitosos': 0,
                'jugadores_saltados': 0,
                'jugadores_fallidos': 0,
                'duration': 0,
                'temporadas_por_minuto': 0,
                'jugadores_por_minuto': 0
            }
    
    print(f"🚀 Empezando descarga desde temporada {start_index + 1}/{len(df_filtered)}")
    
    # Procesar biografías de jugadores
    return scraper.process_seasons(
        df_seasons,
        filters=filters,
        skip_existing=not restart_from_zero,
        start_index=start_index,
        limit=batch_size,
        limit_players_per_season=limit_players_per_season
    )


# Función de testing
def test_player_bio_scraper():
    """
    Función de prueba para el scraper de biografías de jugadores.
    """
    print("=== Testing Player Bio Scraper ===")
    
    try:
        # Cargar temporadas existentes
        from scraping_seasons import load_existing_seasons
        
        df_seasons = load_existing_seasons()
        print(f"📋 Temporadas cargadas: {len(df_seasons)}")
        
        # Probar con Argentina con límites muy restrictivos
        print("⚠️  Para probar este módulo, necesitas archivos squads.json existentes")
        print("🧪 Testing con límites muy restrictivos...")
        
        stats = download_player_bio_by_filters(
            df_seasons,
            pais='Argentina',
            skip_existing=True,
            limit=1,  # Solo 1 temporada
            limit_players_per_season=3  # Solo 3 jugadores para prueba
        )
        
        print(f"\n📊 Resultado del test:")
        print(f"   - Temporadas procesadas: {stats['procesados']}")
        print(f"   - Temporadas exitosas: {stats['exitosos']}")
        print(f"   - Jugadores procesados: {stats['jugadores_procesados']}")
        print(f"   - Jugadores exitosos: {stats['jugadores_exitosos']}")
        
        return stats['fallos'] == 0
        
    except Exception as e:
        print(f"❌ Error en testing: {e}")
        return False


if __name__ == "__main__":
    test_player_bio_scraper()