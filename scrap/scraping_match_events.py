"""
Scraping de eventos de partidos deportivos
=========================================

Este módulo se encarga de descargar eventos detallados de partidos individuales
desde la API de ScoresWay, organizando los archivos JSON por directorios
de competición y temporada.

Autor: Tu nombre
Fecha: Julio 2025
"""

import os
import json
import time
import requests
import pandas as pd
from urllib.parse import quote
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from typing import Dict, List, Optional, Tuple, Union

# Importar funciones comunes
from utils_common import sanitize_dir_name


class MatchEventScraper:
    """
    Clase para hacer scraping de eventos de partidos desde la API de ScoresWay.
    """
    
    def __init__(self, 
                 sdapi_outlet_key: str = 'ft1tiv1inq7v1sk3y9tv12yh5',
                 callback_id: str = 'W308c28470cb75c54dfa5b4fdc707239ca49953812',
                 base_url: str = "https://www.scoresway.com",
                 data_dir: str = 'data'):
        """
        Inicializa el scraper de eventos de partidos.
        
        Args:
            sdapi_outlet_key (str): Clave del outlet para la API
            callback_id (str): ID del callback para JSONP
            base_url (str): URL base del sitio web
            data_dir (str): Directorio base de datos
        """
        self.sdapi_outlet_key = sdapi_outlet_key
        self.callback_id = callback_id
        self.base_url = base_url
        self.api_base_url = "https://api.performfeeds.com/soccerdata/matchevent"
        self.data_dir = data_dir
        
        # Configurar headers
        # self.headers = {
        #     'Accept': 'application/json',
        #     'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36'
        # }
        # self.headers = {
        #     "Host": "www.scoresway.com",
        #     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        #     "Accept-Encoding": "gzip, deflate, br, zstd",
        #     "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        #     "Cache-Control": "no-cache",
        #     "Cookie": "_ga=GA1.1.666883386.1751465023; OptanonAlertBoxClosed=2025-09-19T18:35:29.983Z; eupubconsent-v2=CQX_8fgQX_8fgAcABBESB8FsAP_gAEPgACiQLmtR_G__bWlr-b73aftkeYxP9_hr7sQxBgbJk24FzLvW_JwXx2E5NAzatqIKmRIAu3TBIQNlHJDURVCgKIgVryDMaEyUoTNKJ6BkiFMRI2NYCFxvm4pjeQCY5vr99lc1mB-N7dr82dzyy6hHn3a5_2S1WJCdIYetDfv8ZBKT-9IEd_x8v4v4_F7pE2-eS1n_pGvp6D9-Yns_dBmx9_baffzPn__rl_e7X_vf_n37v943H77v____f_-7_-C5gAJhoVEEZZECIRKBhBAgAUFYQAUCAIAAEgaICAEwYFOQMAF1hMgBACgAGCAEAAIMAAQAACQAIRABQAQCAACAQKAAMACAICABgYAAwAWIgEAAIDoGKYEEAgWACRmVQaYEoACQQEtlQgkAwIK4QhFngEECImCgAABAAKAgAAeCwEJJASsSCALiCaAAAgAACiBAgRSNmAIKAzRaC8GT6MjTAMHzBMkpkGQBMEZGSbEJvwmHjkKIUEOQGxSzAAAA.f_wACHwAAAAA; OptanonConsent=isGpcEnabled=0&datestamp=Sat+Oct+04+2025+07%3A54%3A48+GMT-0300+(Argentina+Standard+Time)&version=202501.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=3aea9760-108d-4551-a221-5897ac42ce3e&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0002%3A1%2CC0001%3A1%2CV2STACK42%3A1&intType=1&geolocation=AR%3BS&AwaitingReconsent=false; _ga_7T677PWWJ1=GS2.1.s1759575288$o13$g0$t1759575663$j60$l0$h0; _ga_SQ24F7Q7YW=GS2.1.s1759575288$o12$g0$t1759575663$j60$l0$h0; _ga_K2ECMCJBFQ=GS2.1.s1759575289$o12$g0$t1759575663$j60$l0$h0; ak_bmsc=2ECEC4E130BAC5B20F08F35A6089AD16~000000000000000000000000000000~YAAQ1fcSAj+I74CZAQAAs+Lhrh2YPxK77E7qLFZQQuUebu8S0QfESbLbXxUf+GBQYg1VDTAbYpqkpZDI8yGwzACCEALkrWskMm5uYnzHod+pAGWM63Wd809k9bgU465O9y1iGHBRyU6BYhpIDR58GC1IHZckBUKMfrOpX5vpxRhdZyxDbiNrqy8BQ7ZrpGZ2QRKsLZLEpg2ubVfna7Iq3j67+IQjyAD8niyPPvMGf1PEGrCLDyt0NW2D1Zpxxd2epGIhDmvYpYQScZmHTwWq8cQs5rzrfYV9GnYpmkDWxEJQBpO9qNcNd7fOy25RM/4ViEc6VfmL7jE4n4+5ojvrqfoOFTxAFNYimypmRem1BjLFWBy5Xeh8y1h9epVd+fUNt7knN3ZDMmZpouwku1fNUCjIgzbAilqHxc4ZfymZUpTu3Ls40cYHID9ZI797B0SuLaLlTyUErgXKBux+DRpL1ZD18Fw=",
        #     "Pragma": "no-cache",
        #     "Priority": "u=0, i",
        #     "Referer": "https://www.scoresway.com/en_GB/soccer/competitions",
        #     "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
        #     "sec-ch-ua-mobile": "?1",
        #     "sec-ch-ua-platform": "\"Android\"",
        #     "Sec-Fetch-Dest": "document",
        #     "Sec-Fetch-Mode": "navigate",
        #     "Sec-Fetch-Site": "same-origin",
        #     "Upgrade-Insecure-Requests": "1",
        #     "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Mobile Safari/537.36",
        # }

        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9,es;q=0.8",
            "cache-control": "no-cache",
            "cookie": "_ga=GA1.1.1149564099.1746215801; _ga_K2ECMCJBFQ=GS2.1.s1759222075$o52$g0$t1759222075$j60$l0$h0; _ga_SQ24F7Q7YW=GS2.1.s1759222075$o53$g0$t1759222075$j60$l0$h0; __gads=ID=bd4692aa005c8e16:T=1761912434:RT=1761912434:S=ALNI_MY3Us_UClmoEMW0Kmu1uShJ0Nl61Q; __gpi=UID=000012a53e43afe1:T=1761912434:RT=1761912434:S=ALNI_MYOO3QSS8ltlzlyx-zIxwaoCExFLQ; __eoi=ID=108b66c8089ae6e2:T=1761912434:RT=1761912434:S=AA-AfjatiXL2OslGm0cNOl3_q5aK; ak_bmsc=4A2F6ECFCA6E3EE2D2A6BB529A9CD532~000000000000000000000000000000~YAAQ1fcSAoTGivyaAQAA78+JBR5gheUz0/E3pVneKDovOrw8T8cBVvYlcqaXKX0jgsnKeMLyv1ZC7ITGLcFd6HTBuIU5kIiNjn/qKrk2sMQ+3u+U2KVwOXHlVoicAP6RdiHldI38YYw3aUGL6ImYeqmjsAFIWuHa/HZrSkjmYqcXB2saJ/MTHwvvq63HgR8fkTXkIztonVA3Qv28EwGTXW6uyrTLqmmm1R76NuQDPrOzGyCYyVUfdfmNAnX3qu0bFBbWNJ5cgEDZ8DrwqMS5EsuuJcUIpBlVqBgaiyKCMVY/EkUtfzzb0V/Ct+ULgr2nP4zH11UQ6IgcmVPsfOM5GEYf3lKZbvUEUDR6e5N8OMC/wRy26MuoSbi2WVF35Ni87tLr07QYfwK53XyB9ROozWInZ0/GMsYX/haEWefSpgLR0vhlSBsxcSwDA3gpEM3ibuIJ7PdvycEfBzFvdfMJsYdlqE72RMAIwT3qAYa55aetghONrZSZ; _ga_7T677PWWJ1=GS2.1.s1765322843$o59$g1$t1765324481$j48$l0$h0",
            "pragma": "no-cache",
            "priority": "u=0, i",
            "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": '"Android"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36"
        }
        
        # Configurar sesión con reintentos
        self.session = self._create_session_with_retries()
        
        # Configuración de delays
        self.sleep_time = 1.0
        self.max_retries = 3
        
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
    
    def set_delay(self, sleep_time: float) -> None:
        """
        Configura el tiempo de espera entre peticiones.
        
        Args:
            sleep_time (float): Tiempo en segundos
        """
        self.sleep_time = sleep_time
        print(f"⏱️  Delay configurado a {sleep_time} segundos")
    
    def descargar_evento_partido(self, match_row: pd.Series, skip_existing: bool = True) -> bool:
        """
        Descarga los eventos de un partido individual.
        
        Args:
            match_row (pd.Series): Fila del DataFrame con información del partido
            skip_existing (bool): Si saltar archivos que ya existen
            
        Returns:
            bool: True si se descargó exitosamente, False en caso contrario
        """
        try:
            # Extraer datos del partido
            partido_id = match_row.get('Partido_ID') or match_row.get('Partido ID')
            continente = match_row.get('Continente') or match_row.get('continente')
            pais = match_row.get('Pais') or match_row.get('pais')
            competicion = match_row.get('Competicion') or match_row.get('competicion')
            id_competicion = match_row.get('ID_Competicion') or match_row.get('id_competicion')
            torneo_id = match_row.get('Torneo_ID') or match_row.get('torneo_id')
            temporada = match_row.get('Temporada') or match_row.get('temporada')
            equipo_local = match_row.get('Equipo_Local') or match_row.get('Equipo Local')
            equipo_visitante = match_row.get('Equipo_Visitante') or match_row.get('Equipo Visitante')
            fecha = match_row.get('Fecha')
            
            # Validar datos esenciales
            if not all([partido_id, continente, pais, competicion, id_competicion, torneo_id]):
                print(f"⚠️  Datos insuficientes para partido {partido_id}")
                return False
            
            # Crear estructura de directorios
            dir_path = self._create_match_directory(
                continente, pais, competicion, id_competicion, torneo_id
            )
            
            # Construir nombre del archivo
            filename = self._build_filename(
                partido_id, fecha, equipo_local, equipo_visitante
            )
            json_path = os.path.join(dir_path, filename)
            
            # Si el archivo ya existe y skip_existing es True, saltarlo
            if skip_existing and os.path.exists(json_path):
                print(f"⏭️  Archivo ya existe (saltando): {filename}")
                self.saltados += 1
                return True
            
            # Construir URL de la API
            url_eventos = self._build_api_url(partido_id)
            
            # Construir URL de referencia
            referer = self._build_referer_url(competicion, temporada, torneo_id)
            
            # Actualizar headers con referer
            headers = self.headers.copy()
            headers['Referer'] = referer
            
            # Realizar petición
            print(f"🌐 Descargando eventos: {equipo_local} vs {equipo_visitante}")
            response = self.session.get(url_eventos, headers=headers)
            response.raise_for_status()
            
            # Extraer JSON de la respuesta JSONP
            json_data = self._extract_json_from_jsonp(response.text)
            
            # Guardar archivo
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            print(f"✅ Guardado: {filename}")
            self.exitos += 1
            
            # Delay entre peticiones
            time.sleep(self.sleep_time)
            
            return True
            
        except Exception as e:
            print(f"❌ Error al procesar partido {partido_id}: {str(e)}")
            self.fallos += 1
            return False
    
    def _create_match_directory(self, continente: str, pais: str, competicion: str, 
                               id_competicion: str, torneo_id: str) -> str:
        """
        Crea la estructura de directorios para los eventos de partidos.
        
        Returns:
            str: Ruta del directorio creado
        """
        # Limpiar nombres para directorios
        continente_clean = sanitize_dir_name(continente)
        pais_clean = sanitize_dir_name(pais)
        competicion_clean = sanitize_dir_name(competicion)
        
        dir_path = os.path.join(
            self.data_dir,
            continente_clean,
            pais_clean,
            f"{competicion_clean}_{id_competicion}",
            torneo_id,
            'matches'
        )
        
        os.makedirs(dir_path, exist_ok=True)
        return dir_path
    
    def _build_filename(self, partido_id: str, fecha: str, equipo_local: str, 
                       equipo_visitante: str) -> str:
        """
        Construye el nombre del archivo para el evento del partido.
        
        Returns:
            str: Nombre del archivo
        """
        # Limpiar nombres para archivos
        safe_local = sanitize_dir_name(str(equipo_local))
        safe_visitante = sanitize_dir_name(str(equipo_visitante))
        safe_fecha = sanitize_dir_name(str(fecha)) if fecha else "sin_fecha"
        
        return f"{partido_id}_{safe_fecha}_{safe_local}_{safe_visitante}.json"
    
    def _build_api_url(self, partido_id: str) -> str:
        """
        Construye la URL de la API para obtener eventos del partido.
        
        Returns:
            str: URL de la API
        """
        return (
            f"{self.api_base_url}/{self.sdapi_outlet_key}/"
            f"{partido_id}?_rt=c&_lcl=en&_fmt=jsonp&sps=widgets&_clbk={self.callback_id}"
        )
    
    def _build_referer_url(self, competicion: str, temporada: str, torneo_id: str) -> str:
        """
        Construye la URL de referencia para la petición.
        
        Returns:
            str: URL de referencia
        """
        url_base = f'{self.base_url}/en_GB/soccer/'
        url_competicion = f"{quote(competicion)}-{quote(temporada)}/{torneo_id}"
        return f"{url_base}{url_competicion}/fixtures"
    
    def _extract_json_from_jsonp(self, content: str) -> Dict:
        """
        Extrae JSON puro de una respuesta JSONP.
        
        Args:
            content (str): Contenido de la respuesta JSONP
            
        Returns:
            Dict: Datos JSON extraídos
            
        Raises:
            Exception: Si no se puede extraer el JSON
        """
        inicio_json = content.find('(') + 1
        final_json = content.rfind(')')
        
        if inicio_json <= 0 or final_json <= 0 or inicio_json >= final_json:
            raise Exception("Formato de respuesta JSONP inesperado")
        
        return json.loads(content[inicio_json:final_json])
    
    def descargar_eventos_masivo(self, 
                                df_partidos: pd.DataFrame,
                                filters: Optional[Dict] = None,
                                skip_existing: bool = True,
                                start_index: int = 0,
                                limit: Optional[int] = None) -> Dict:
        """
        Descarga eventos para múltiples partidos.
        
        Args:
            df_partidos (pd.DataFrame): DataFrame con partidos
            filters (Dict, optional): Filtros para aplicar
            skip_existing (bool): Si saltar archivos existentes
            start_index (int): Índice de inicio
            limit (Optional[int]): Límite de partidos a procesar
            
        Returns:
            Dict: Estadísticas del procesamiento
        """
        # Reiniciar estadísticas
        self.reset_stats()
        start_time = time.time()
        
        # Aplicar filtros si se proporcionan
        df_filtered = self._apply_filters(df_partidos, filters)
        
        # Determinar rango de procesamiento
        end_index = len(df_filtered)
        if limit:
            end_index = min(start_index + limit, end_index)
        
        df_to_process = df_filtered.iloc[start_index:end_index]
        
        print(f"🚀 Iniciando descarga de eventos de partidos...")
        print(f"   - Partidos originales: {len(df_partidos)}")
        if filters:
            print(f"   - Partidos después de filtros: {len(df_filtered)}")
        print(f"   - Partidos a procesar: {len(df_to_process)}")
        print(f"   - Rango: {start_index} a {end_index-1}")
        print(f"   - Delay entre peticiones: {self.sleep_time} segundos")
        
        # Procesar cada partido
        for idx, (_, row) in enumerate(df_to_process.iterrows()):
            try:
                equipo_local = row.get('Equipo_Local') or row.get('Equipo Local', 'N/A')
                equipo_visitante = row.get('Equipo_Visitante') or row.get('Equipo Visitante', 'N/A')
                
                print(f"\n📋 [{self.procesados + 1}/{len(df_to_process)}] Procesando: {equipo_local} vs {equipo_visitante}")
                
                # Intentar descargar eventos
                self.descargar_evento_partido(row, skip_existing)
                self.procesados += 1
                
                # Mostrar progreso cada 20 elementos
                if (idx + 1) % 20 == 0:
                    self._print_progress(idx + 1, len(df_to_process), start_time)
                
            except KeyboardInterrupt:
                print(f"\n⚠️  Descarga interrumpida por el usuario")
                break
            except Exception as e:
                print(f"❌ Error inesperado en partido {idx}: {e}")
                self.fallos += 1
                self.procesados += 1
        
        # Calcular tiempo total
        duration = time.time() - start_time
        
        # Crear estadísticas finales
        stats = {
            'total_partidos': len(df_to_process),
            'procesados': self.procesados,
            'exitosos': self.exitos,
            'saltados': self.saltados,
            'fallos': self.fallos,
            'duration': duration,
            'partidos_por_minuto': (self.procesados / (duration / 60)) if duration > 0 else 0
        }
        
        # Imprimir resumen final
        self._print_final_summary(stats)
        
        return stats
    
    def _apply_filters(self, df: pd.DataFrame, filters: Optional[Dict]) -> pd.DataFrame:
        """
        Aplica filtros al DataFrame de partidos.
        
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
            # Manejar nombres de columnas con diferentes formatos
            column_variants = [
                column,
                column.replace('_', ' ').title(),
                column.replace(' ', '_').lower(),
                column.replace('_', ' ').title().replace(' ', '_')
            ]
            
            found_column = None
            for variant in column_variants:
                if variant in df_filtered.columns:
                    found_column = variant
                    break
            
            if found_column:
                if isinstance(value, list):
                    df_filtered = df_filtered[df_filtered[found_column].isin(value)]
                else:
                    df_filtered = df_filtered[df_filtered[found_column] == value]
                print(f"🔍 Filtro aplicado - {found_column}: {value} → {len(df_filtered)} partidos")
            else:
                print(f"⚠️  Columna '{column}' no encontrada. Columnas disponibles: {list(df_filtered.columns)}")
        
        return df_filtered
    
    def _print_progress(self, current: int, total: int, start_time: float) -> None:
        """
        Imprime el progreso del procesamiento.
        """
        elapsed = time.time() - start_time
        rate = self.procesados / (elapsed / 60) if elapsed > 0 else 0
        
        print(f"\n📊 Progreso: {current}/{total} ({current/total*100:.1f}%)")
        print(f"   ⚡ Velocidad: {rate:.2f} partidos/minuto")
        print(f"   ✅ Exitosos: {self.exitos}")
        print(f"   ⏭️  Saltados: {self.saltados}")
        print(f"   ❌ Fallos: {self.fallos}")
        
        if elapsed > 0:
            remaining = (total - current) * (elapsed / current)
            print(f"   ⏰ Tiempo estimado restante: {remaining/60:.1f} minutos")
    
    def _print_final_summary(self, stats: Dict) -> None:
        """
        Imprime el resumen final del procesamiento.
        """
        print(f"\n" + "="*60)
        print(f"📊 RESUMEN FINAL - DESCARGA DE EVENTOS")
        print(f"="*60)
        print(f"⏰ Tiempo total: {stats['duration']:.1f} segundos ({stats['duration']/60:.1f} minutos)")
        print(f"📋 Partidos procesados: {stats['procesados']}/{stats['total_partidos']}")
        print(f"✅ Descargas exitosas: {stats['exitosos']}")
        print(f"⏭️  Archivos saltados (ya existían): {stats['saltados']}")
        print(f"❌ Fallos: {stats['fallos']}")
        print(f"⚡ Velocidad promedio: {stats['partidos_por_minuto']:.2f} partidos/minuto")
        
        if stats['exitosos'] > 0:
            success_rate = (stats['exitosos'] / stats['procesados']) * 100
            print(f"📈 Tasa de éxito: {success_rate:.1f}%")


def find_event_resume_index(df_partidos: pd.DataFrame, filters: Optional[Dict] = None) -> int:
    """
    Encuentra el índice desde donde continuar basándose en eventos ya descargados.
    
    Args:
        df_partidos (pd.DataFrame): DataFrame con partidos
        filters (Dict, optional): Filtros aplicados
        
    Returns:
        int: Índice desde donde continuar
    """
    try:
        scraper = MatchEventScraper()
        
        # Aplicar filtros si se proporcionan
        df_filtered = scraper._apply_filters(df_partidos, filters) if filters else df_partidos
        
        # Revisar qué eventos ya existen
        for idx, row in df_filtered.iterrows():
            try:
                # Construir ruta esperada del archivo
                partido_id = row.get('Partido_ID') or row.get('Partido ID')
                continente = row.get('Continente') or row.get('continente')
                pais = row.get('Pais') or row.get('pais')
                competicion = row.get('Competicion') or row.get('competicion')
                id_competicion = row.get('ID_Competicion') or row.get('id_competicion')
                torneo_id = row.get('Torneo_ID') or row.get('torneo_id')
                equipo_local = row.get('Equipo_Local') or row.get('Equipo Local')
                equipo_visitante = row.get('Equipo_Visitante') or row.get('Equipo Visitante')
                fecha = row.get('Fecha')
                
                if not all([partido_id, continente, pais, competicion, id_competicion, torneo_id]):
                    continue
                
                # Construir ruta del archivo
                dir_path = scraper._create_match_directory(
                    continente, pais, competicion, id_competicion, torneo_id
                )
                filename = scraper._build_filename(
                    partido_id, fecha, equipo_local, equipo_visitante
                )
                json_path = os.path.join(dir_path, filename)
                
                if not os.path.exists(json_path):
                    print(f"🔍 Primer evento faltante encontrado en índice {idx}")
                    print(f"   Partido: {equipo_local} vs {equipo_visitante}")
                    print(f"   Competición: {competicion}")
                    return idx
                    
            except Exception as e:
                # Si hay error al construir la ruta, significa que falta
                print(f"⚠️  Error procesando partido {idx}: {e}")
                return idx
        
        print("✅ Todos los eventos ya fueron descargados")
        return len(df_filtered)
        
    except Exception as e:
        print(f"⚠️  Error al buscar punto de reanudación: {e}")
        return 0


# Funciones de conveniencia para usar directamente
def download_match_events(df_partidos: pd.DataFrame,
                         continente: Optional[str] = None,
                         pais: Optional[str] = None,
                         competicion: Optional[str] = None,
                         skip_existing: bool = True,
                         start_index: int = 0,
                         limit: Optional[int] = None,
                         sleep_time: float = 1.0,
                         **scraper_kwargs) -> Dict:
    """
    Descarga eventos de partidos aplicando filtros comunes.
    
    Args:
        df_partidos (pd.DataFrame): DataFrame con partidos
        continente (str, optional): Filtrar por continente
        pais (str, optional): Filtrar por país
        competicion (str, optional): Filtrar por competición
        skip_existing (bool): Si saltar archivos existentes
        start_index (int): Índice de inicio
        limit (Optional[int]): Límite de partidos a procesar
        sleep_time (float): Tiempo de espera entre peticiones
        **scraper_kwargs: Argumentos adicionales para MatchEventScraper
        
    Returns:
        Dict: Estadísticas del procesamiento
        
    Example:
        # Descargar eventos solo de Argentina
        stats = download_match_events(df_partidos, pais='Argentina', sleep_time=1.5)
    """
    # Crear filtros
    filters = {}
    if continente:
        filters['continente'] = continente
    if pais:
        filters['pais'] = pais
    if competicion:
        filters['competicion'] = competicion
    
    # Crear scraper
    scraper = MatchEventScraper(**scraper_kwargs)
    scraper.set_delay(sleep_time)
    
    # Procesar
    return scraper.descargar_eventos_masivo(
        df_partidos,
        filters=filters,
        skip_existing=skip_existing,
        start_index=start_index,
        limit=limit
    )


def smart_download_events(df_partidos: pd.DataFrame,
                         continente: Optional[str] = None,
                         pais: Optional[str] = None,
                         competicion: Optional[str] = None,
                         restart_from_zero: bool = False,
                         batch_size: int = 100,
                         sleep_time: float = 1.0,
                         **scraper_kwargs) -> Dict:
    """
    Función inteligente que detecta automáticamente desde dónde continuar la descarga de eventos.
    
    Args:
        df_partidos (pd.DataFrame): DataFrame con partidos
        continente (str, optional): Filtrar por continente
        pais (str, optional): Filtrar por país
        competicion (str, optional): Filtrar por competición
        restart_from_zero (bool): Si borrar eventos existentes y empezar de cero
        batch_size (int): Número de partidos a procesar en este lote
        sleep_time (float): Tiempo de espera entre peticiones
        **scraper_kwargs: Argumentos adicionales para MatchEventScraper
        
    Returns:
        Dict: Estadísticas del procesamiento
        
    Example:
        # Para continuar automáticamente:
        stats = smart_download_events(df_partidos, pais='Argentina')
        
        # Para empezar de cero:
        stats = smart_download_events(df_partidos, pais='Argentina', restart_from_zero=True)
    """
    # Crear filtros
    filters = {}
    if continente:
        filters['continente'] = continente
    if pais:
        filters['pais'] = pais
    if competicion:
        filters['competicion'] = competicion
    
    # Crear scraper
    scraper = MatchEventScraper(**scraper_kwargs)
    scraper.set_delay(sleep_time)
    
    # Aplicar filtros para obtener subconjunto
    df_filtered = scraper._apply_filters(df_partidos, filters) if filters else df_partidos
    
    print(f"🎯 Filtros aplicados: {filters if filters else 'Ninguno'}")
    print(f"📋 Partidos a procesar: {len(df_filtered)}")
    
    # Si restart_from_zero, borrar archivos existentes del filtro
    if restart_from_zero:
        deleted_count = 0
        print("🔥 Modo reinicio: Borrando eventos existentes...")
        
        for _, row in df_filtered.iterrows():
            try:
                # Construir ruta del archivo
                partido_id = row.get('Partido_ID') or row.get('Partido ID')
                continente_val = row.get('Continente') or row.get('continente')
                pais_val = row.get('Pais') or row.get('pais')
                competicion_val = row.get('Competicion') or row.get('competicion')
                id_competicion = row.get('ID_Competicion') or row.get('id_competicion')
                torneo_id = row.get('Torneo_ID') or row.get('torneo_id')
                equipo_local = row.get('Equipo_Local') or row.get('Equipo Local')
                equipo_visitante = row.get('Equipo_Visitante') or row.get('Equipo Visitante')
                fecha = row.get('Fecha')
                
                if not all([partido_id, continente_val, pais_val, competicion_val, id_competicion, torneo_id]):
                    continue
                
                dir_path = scraper._create_match_directory(
                    continente_val, pais_val, competicion_val, id_competicion, torneo_id
                )
                filename = scraper._build_filename(
                    partido_id, fecha, equipo_local, equipo_visitante
                )
                json_path = os.path.join(dir_path, filename)
                
                if os.path.exists(json_path):
                    os.remove(json_path)
                    deleted_count += 1
                    
            except Exception as e:
                print(f"⚠️  Error borrando archivo: {e}")
        
        print(f"🗑️  Eliminados {deleted_count} eventos existentes")
        start_index = 0
    else:
        # Encontrar desde dónde continuar
        start_index = find_event_resume_index(df_partidos, filters)
        
        if start_index >= len(df_filtered):
            print("✅ Todos los eventos ya están descargados")
            return {
                'total_partidos': len(df_filtered),
                'procesados': len(df_filtered),
                'exitosos': len(df_filtered),
                'saltados': len(df_filtered),
                'fallos': 0,
                'duration': 0,
                'partidos_por_minuto': 0
            }
    
    print(f"🚀 Empezando descarga desde partido {start_index + 1}/{len(df_filtered)}")
    
    # Procesar eventos
    return scraper.descargar_eventos_masivo(
        df_partidos,
        filters=filters,
        skip_existing=not restart_from_zero,
        start_index=start_index,
        limit=batch_size
    )


# Función de testing
def test_match_event_scraper():
    """
    Función de prueba para el scraper de eventos de partidos.
    """
    print("=== Testing Match Event Scraper ===")
    
    try:
        # Necesitaríamos un DataFrame de partidos para probar
        print("⚠️  Para probar este módulo, necesitas un DataFrame de partidos")
        print("   Ejemplo de uso:")
        print("   df_partidos = pd.read_parquet('data/partidos_argentina.parquet')")
        print("   stats = download_match_events(df_partidos, pais='Argentina', limit=5)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en testing: {e}")
        return False


if __name__ == "__main__":
    test_match_event_scraper()