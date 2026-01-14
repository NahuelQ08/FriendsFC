"""
Procesamiento de fixtures deportivos
===================================

Este módulo se encarga de procesar los archivos JSON de fixtures descargados,
extrayendo información detallada de cada partido y organizándola en DataFrames
para análisis posterior.

Autor: Tu nombre
Fecha: Julio 2025
"""

import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

# Importar funciones comunes
from utils_common import get_season_name_from_url, get_torneo_id, sanitize_dir_name


class FixtureProcessor:
    """
    Clase para procesar archivos JSON de fixtures y convertirlos en DataFrames estructurados.
    """
    
    def __init__(self, base_url: str = "https://www.scoresway.com", data_dir: str = 'data'):
        """
        Inicializa el procesador de fixtures.
        
        Args:
            base_url (str): URL base para construir enlaces de partidos
            data_dir (str): Directorio base donde están los datos
        """
        self.base_url = base_url
        self.data_dir = data_dir
        self.processed_files = 0
        self.skipped_files = 0
        self.error_files = 0
    
    def procesar_fixture_json(self, json_path: str, season_row: pd.Series) -> List[Dict]:
        """
        Procesa un archivo fixture.json y devuelve una lista de diccionarios con los partidos.
        
        Args:
            json_path (str): Ruta al archivo JSON de fixture
            season_row (pd.Series): Fila del DataFrame de temporadas con metadata
            
        Returns:
            List[Dict]: Lista de diccionarios con información de partidos
        """
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                fixture_json = json.load(f)
        except Exception as e:
            print(f"❌ Error al leer {json_path}: {e}")
            self.error_files += 1
            return []

        partidos = fixture_json.get('match', [])
        if not partidos:
            print(f"⚠️  No se encontraron partidos en {json_path}")
            self.skipped_files += 1
            return []

        datos_partidos = []
        
        # Extraer información base de la temporada
        torneo_id = self._extract_torneo_id_from_path(json_path)
        
        for partido in partidos:
            try:
                partido_data = self._extract_match_data(partido, season_row, torneo_id)
                if partido_data:
                    datos_partidos.append(partido_data)
            except Exception as e:
                print(f"⚠️  Error procesando partido en {json_path}: {e}")
                continue

        self.processed_files += 1
        print(f"✅ Procesado {json_path}: {len(datos_partidos)} partidos")
        return datos_partidos
    
    def _extract_torneo_id_from_path(self, json_path: str) -> Optional[str]:
        """
        Extrae el torneo_id del path del archivo.
        
        Args:
            json_path (str): Ruta al archivo JSON
            
        Returns:
            Optional[str]: ID del torneo o None si no se puede extraer
        """
        try:
            # El torneo_id debería ser el directorio padre del fixture.json
            return Path(json_path).parent.name
        except Exception as e:
            print(f"⚠️  No se pudo extraer torneo_id de {json_path}: {e}")
            return None
    
    def _extract_match_data(self, partido: Dict, season_row: pd.Series, torneo_id: str) -> Optional[Dict]:
        """
        Extrae datos de un partido individual.
        
        Args:
            partido (Dict): Datos del partido desde el JSON
            season_row (pd.Series): Información de la temporada
            torneo_id (str): ID del torneo
            
        Returns:
            Optional[Dict]: Diccionario con datos del partido o None si hay error
        """
        match_info = partido.get('matchInfo', {})
        if not match_info:
            return None

        contestants = match_info.get('contestant', [])
        if len(contestants) < 2:
            return None

        # Extraer información básica
        partido_id = match_info.get('id')
        fecha_raw = match_info.get('date')
        hora = match_info.get('time')
        
        # Procesar fecha
        fecha_procesada = self._process_date(fecha_raw)
        
        # Información de equipos
        equipo_local = contestants[0].get('name') if contestants[0] else None
        equipo_visitante = contestants[1].get('name') if len(contestants) > 1 else None
        
        # Información del estadio
        venue_info = match_info.get('venue', {})
        estadio = venue_info.get('shortName') or venue_info.get('longName')
        
        # Construir URL del partido
        url_partido = self._build_match_url(season_row, torneo_id, partido_id)
        
        # Información de estado del partido
        match_status = match_info.get('matchStatus')
        coverage_level = match_info.get('coverageLevel')
        last_updated = match_info.get('lastUpdated')
        
        # Información adicional si está disponible
        attendance = match_info.get('attendance')
        weather = match_info.get('weather', {})
        
        return {
            'Fecha': fecha_procesada,
            'Fecha_Raw': fecha_raw,
            'Hora': hora,
            'Equipo_Local': equipo_local,
            'Equipo_Visitante': equipo_visitante,
            'Estadio': estadio,
            'Partido_ID': partido_id,
            'Continente': season_row.get('continente'),
            'Pais': season_row.get('pais'),
            'Competicion': season_row.get('competicion'),
            'ID_Competicion': season_row.get('id_competicion'),
            'Torneo_ID': torneo_id,
            'Temporada': season_row.get('temporada'),
            'URL_Partido': url_partido,
            'Estado_Partido': match_status,
            'Nivel_Cobertura': coverage_level,
            'Ultima_Actualizacion': last_updated,
            'Asistencia': attendance,
            'Clima_Temperatura': weather.get('temperature'),
            'Clima_Condiciones': weather.get('conditions')
        }
    
    def _process_date(self, fecha_raw: str) -> Optional[str]:
        """
        Procesa y normaliza fechas.
        
        Args:
            fecha_raw (str): Fecha en formato raw desde JSON
            
        Returns:
            Optional[str]: Fecha procesada en formato YYYY-MM-DD o None
        """
        if not fecha_raw:
            return None
            
        try:
            # Manejar formato ISO con Z
            if fecha_raw.endswith('Z'):
                fecha_dt = datetime.fromisoformat(fecha_raw.replace("Z", ""))
                return fecha_dt.date().isoformat()
            
            # Manejar otros formatos comunes
            for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']:
                try:
                    fecha_dt = datetime.strptime(fecha_raw, fmt)
                    return fecha_dt.date().isoformat()
                except ValueError:
                    continue
            
            # Si no se puede parsear, devolver como string
            return str(fecha_raw)
            
        except Exception as e:
            print(f"⚠️  Error procesando fecha {fecha_raw}: {e}")
            return str(fecha_raw) if fecha_raw else None
    
    def _build_match_url(self, season_row: pd.Series, torneo_id: str, partido_id: str) -> str:
        """
        Construye la URL del partido.
        
        Args:
            season_row (pd.Series): Información de la temporada
            torneo_id (str): ID del torneo
            partido_id (str): ID del partido
            
        Returns:
            str: URL completa del partido
        """
        try:
            competicion_clean = str(season_row.get('competicion', '')).lower().replace(' ', '-')
            url = (
                f"{self.base_url}/en_GB/soccer/{competicion_clean}/"
                f"{torneo_id}/match/view/{partido_id}/player-stats"
            )
            return url
        except Exception as e:
            print(f"⚠️  Error construyendo URL: {e}")
            return ""
    
    def _get_fixture_path(self, season_row: pd.Series) -> str:
        """
        Construye la ruta al archivo fixture.json para una temporada.
        
        Args:
            season_row (pd.Series): Fila del DataFrame de temporadas
            
        Returns:
            str: Ruta al archivo fixture.json
        """
        try:
            # Obtener nombre de temporada
            season_name = get_season_name_from_url(season_row['url_resultados'])
            if not season_name:
                return ""
            
            # Crear nombres de directorio seguros
            continente_dir = sanitize_dir_name(season_row['continente'])
            pais_dir = sanitize_dir_name(season_row['pais'])
            competicion = sanitize_dir_name(season_row['competicion'])
            competicion_dir = f"{competicion}_{season_row['id_competicion']}"
            
            # Construir ruta completa
            dir_path = os.path.join(
                self.data_dir,
                continente_dir,
                pais_dir,
                competicion_dir,
                season_name
            )
            
            return os.path.join(dir_path, 'fixture.json')
            
        except Exception as e:
            print(f"❌ Error construyendo ruta para {season_row.get('competicion', 'N/A')}: {e}")
            return ""
    
    def crear_dataframe_partidos(self, 
                                df_seasons: pd.DataFrame,
                                filters: Optional[Dict] = None,
                                save_results: bool = True,
                                output_dir: Optional[str] = None,
                                save_individual: bool = False,
                                save_consolidated: bool = True) -> pd.DataFrame:
        """
        Crea un DataFrame con todos los partidos de los fixture.json encontrados.
        
        Args:
            df_seasons (pd.DataFrame): DataFrame con temporadas
            filters (Dict, optional): Filtros para aplicar (continente, pais, competicion)
            save_results (bool): Si guardar los resultados en archivos
            output_dir (str, optional): Directorio de salida personalizado
            save_individual (bool): Si guardar archivos individuales por temporada (junto al fixture.json)
            save_consolidated (bool): Si guardar archivo consolidado con todos los partidos
            
        Returns:
            pd.DataFrame: DataFrame con todos los partidos procesados
        """
        # Reiniciar contadores
        self.processed_files = 0
        self.skipped_files = 0
        self.error_files = 0
        
        # Aplicar filtros si se proporcionan
        df_filtered = self._apply_filters(df_seasons, filters)
        
        print(f"🚀 Iniciando procesamiento de fixtures...")
        print(f"   - Temporadas originales: {len(df_seasons)}")
        if filters:
            print(f"   - Temporadas después de filtros: {len(df_filtered)}")
        print(f"   - Temporadas a procesar: {len(df_filtered)}")
        
        todos_los_partidos = []
        
        for idx, row in df_filtered.iterrows():
            try:
                # Construir ruta del archivo JSON
                json_path = self._get_fixture_path(row)
                
                if not json_path:
                    self.skipped_files += 1
                    continue
                
                if not os.path.exists(json_path):
                    print(f"⚠️  Archivo no encontrado: {json_path}")
                    self.skipped_files += 1
                    continue
                
                # Procesar fixture
                print(f"📋 Procesando ({idx + 1}/{len(df_filtered)}): {row.get('competicion', 'N/A')} - {row.get('temporada', 'N/A')}")
                partidos = self.procesar_fixture_json(json_path, row)
                todos_los_partidos.extend(partidos)
                
                # Guardar archivo individual si se solicita
                if save_results and save_individual and partidos:
                    self._save_individual_results(partidos, json_path)
                
                # Mostrar progreso cada 10 archivos
                if (idx + 1) % 10 == 0:
                    self._print_progress(idx + 1, len(df_filtered), len(todos_los_partidos))
                
            except Exception as e:
                print(f"❌ Error procesando temporada {idx}: {e}")
                self.error_files += 1
                continue
        
        # Crear DataFrame final
        if todos_los_partidos:
            df_partidos = pd.DataFrame(todos_los_partidos)
            
            # Ordenar por fecha si es posible
            if 'Fecha' in df_partidos.columns:
                try:
                    df_partidos = df_partidos.sort_values('Fecha')
                except:
                    print("⚠️  No se pudo ordenar por fecha")
            
            # Imprimir resumen
            self._print_final_summary(df_partidos, filters)
            
            # Guardar resultados si se solicita
            if save_results and save_consolidated:
                self._save_results(df_partidos, filters, output_dir)
            
            return df_partidos
        else:
            print("❌ No se encontraron partidos para procesar")
            return pd.DataFrame()
    
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
    
    def _print_progress(self, current: int, total: int, total_matches: int) -> None:
        """
        Imprime el progreso del procesamiento.
        """
        percentage = (current / total) * 100
        print(f"\n📊 Progreso: {current}/{total} ({percentage:.1f}%)")
        print(f"   📋 Archivos procesados: {self.processed_files}")
        print(f"   ⚠️  Archivos omitidos: {self.skipped_files}")
        print(f"   ❌ Archivos con error: {self.error_files}")
        print(f"   ⚽ Total partidos: {total_matches}")
    
    def _print_final_summary(self, df_partidos: pd.DataFrame, filters: Optional[Dict]) -> None:
        """
        Imprime el resumen final del procesamiento.
        """
        print(f"\n" + "="*60)
        print(f"📊 RESUMEN FINAL DEL PROCESAMIENTO")
        print(f"="*60)
        print(f"📁 Archivos procesados: {self.processed_files}")
        print(f"⚠️  Archivos omitidos: {self.skipped_files}")
        print(f"❌ Archivos con error: {self.error_files}")
        print(f"⚽ Total partidos extraídos: {len(df_partidos)}")
        
        if not df_partidos.empty:
            print(f"\n📈 Estadísticas de partidos:")
            print(f"   - Competiciones únicas: {df_partidos['Competicion'].nunique()}")
            print(f"   - Países únicos: {df_partidos['Pais'].nunique()}")
            print(f"   - Equipos únicos: {pd.concat([df_partidos['Equipo_Local'], df_partidos['Equipo_Visitante']]).nunique()}")
            
            if 'Fecha' in df_partidos.columns and df_partidos['Fecha'].notna().any():
                try:
                    fechas_validas = df_partidos[df_partidos['Fecha'].notna()]
                    if not fechas_validas.empty:
                        print(f"   - Rango de fechas: {fechas_validas['Fecha'].min()} a {fechas_validas['Fecha'].max()}")
                except:
                    pass
        
        if filters:
            print(f"\n🔍 Filtros aplicados: {filters}")
    
    def _save_individual_results(self, partidos: List[Dict], json_path: str) -> None:
        """
        Guarda los partidos de una temporada individual junto a su fixture.json.
        
        Args:
            partidos (List[Dict]): Lista de partidos de esta temporada
            json_path (str): Ruta del archivo fixture.json
        """
        try:
            if not partidos:
                return
            
            # Crear DataFrame temporal para esta temporada
            df_temp = pd.DataFrame(partidos)
            
            # Obtener directorio del fixture.json
            fixture_dir = os.path.dirname(json_path)
            
            # Nombres de archivo
            base_name = "partidos"
            csv_path = os.path.join(fixture_dir, f"{base_name}.csv")
            parquet_path = os.path.join(fixture_dir, f"{base_name}.parquet")
            
            # Guardar CSV
            df_temp.to_csv(csv_path, index=False, encoding='utf-8-sig')
            
            # Guardar Parquet
            df_temp.to_parquet(parquet_path, index=False)
            
            print(f"💾 Guardado individual: {len(partidos)} partidos en {fixture_dir}")
            
        except Exception as e:
            print(f"❌ Error guardando archivo individual en {json_path}: {e}")

    def _save_results(self, df_partidos: pd.DataFrame, filters: Optional[Dict], output_dir: Optional[str]) -> None:
        """
        Guarda los resultados en archivos CSV y Parquet.
        """
        try:
            # Determinar directorio de salida
            if output_dir:
                save_dir = output_dir
            elif filters and 'continente' in filters:
                save_dir = os.path.join(self.data_dir, sanitize_dir_name(filters['continente']))
            elif filters and 'pais' in filters:
                save_dir = os.path.join(self.data_dir, sanitize_dir_name(filters['pais']))
            else:
                save_dir = self.data_dir
            
            # Crear directorio si no existe
            os.makedirs(save_dir, exist_ok=True)
            
            # Nombres de archivo
            base_filename = "todos_los_partidos"
            if filters:
                filter_parts = []
                for key, value in filters.items():
                    filter_parts.append(f"{key}_{sanitize_dir_name(str(value))}")
                if filter_parts:
                    base_filename = f"partidos_{'_'.join(filter_parts)}"
            
            # Guardar CSV
            csv_path = os.path.join(save_dir, f"{base_filename}.csv")
            df_partidos.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"✅ CSV guardado: {csv_path}")
            
            # Guardar Parquet
            parquet_path = os.path.join(save_dir, f"{base_filename}.parquet")
            df_partidos.to_parquet(parquet_path, index=False)
            print(f"✅ Parquet guardado: {parquet_path}")
            
        except Exception as e:
            print(f"❌ Error guardando resultados: {e}")


# Funciones de conveniencia para usar directamente
def process_matches_by_filters(df_seasons: pd.DataFrame,
                              continente: Optional[str] = None,
                              pais: Optional[str] = None,
                              competicion: Optional[str] = None,
                              save_results: bool = True,
                              output_dir: Optional[str] = None,
                              save_individual: bool = False,
                              save_consolidated: bool = True) -> pd.DataFrame:
    """
    Procesa partidos aplicando filtros comunes.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        continente (str, optional): Filtrar por continente
        pais (str, optional): Filtrar por país
        competicion (str, optional): Filtrar por competición
        save_results (bool): Si guardar resultados en archivos
        output_dir (str, optional): Directorio de salida personalizado
        save_individual (bool): Si guardar archivos individuales por temporada (junto al fixture.json)
        save_consolidated (bool): Si guardar archivo consolidado con todos los partidos
        
    Returns:
        pd.DataFrame: DataFrame con partidos procesados
        
    Example:
        # Guardar solo archivos individuales (junto a cada fixture.json):
        df = process_matches_by_filters(df_seasons, pais='Argentina', 
                                       save_individual=True, save_consolidated=False)
        
        # Guardar ambos (individual + consolidado):
        df = process_matches_by_filters(df_seasons, pais='Argentina', 
                                       save_individual=True, save_consolidated=True)
    """
    # Crear filtros
    filters = {}
    if continente:
        filters['continente'] = continente
    if pais:
        filters['pais'] = pais
    if competicion:
        filters['competicion'] = competicion
    
    # Procesar
    processor = FixtureProcessor()
    return processor.crear_dataframe_partidos(
        df_seasons, 
        filters=filters, 
        save_results=save_results,
        output_dir=output_dir,
        save_individual=save_individual,
        save_consolidated=save_consolidated
    )


def process_all_matches(df_seasons: pd.DataFrame, 
                       save_results: bool = True,
                       output_dir: Optional[str] = None,
                       save_individual: bool = False,
                       save_consolidated: bool = True) -> pd.DataFrame:
    """
    Procesa todos los partidos sin filtros.
    
    Args:
        df_seasons (pd.DataFrame): DataFrame con temporadas
        save_results (bool): Si guardar resultados en archivos
        output_dir (str, optional): Directorio de salida personalizado
        save_individual (bool): Si guardar archivos individuales por temporada
        save_consolidated (bool): Si guardar archivo consolidado
        
    Returns:
        pd.DataFrame: DataFrame con todos los partidos procesados
    """
    processor = FixtureProcessor()
    return processor.crear_dataframe_partidos(
        df_seasons, 
        save_results=save_results,
        output_dir=output_dir,
        save_individual=save_individual,
        save_consolidated=save_consolidated
    )


# Función de testing
def test_fixture_processor():
    """
    Función de prueba para el procesador de fixtures.
    """
    print("=== Testing Fixture Processor ===")
    
    try:
        # Cargar temporadas existentes
        from scraping_seasons import load_existing_seasons
        
        df_seasons = load_existing_seasons()
        print(f"📋 Temporadas cargadas: {len(df_seasons)}")
        
        # Probar con Argentina (pocas temporadas)
        print("🧪 Probando procesamiento con Argentina...")
        
        df_partidos = process_matches_by_filters(
            df_seasons,
            pais='Argentina',
            save_results=False,  # No guardar para testing
            save_individual=False,
            save_consolidated=False
        )
        
        print(f"\n📊 Resultado del test:")
        print(f"   - Partidos procesados: {len(df_partidos)}")
        
        if not df_partidos.empty:
            print(f"   - Competiciones: {df_partidos['Competicion'].nunique()}")
            print(f"   - Equipos únicos: {pd.concat([df_partidos['Equipo_Local'], df_partidos['Equipo_Visitante']]).nunique()}")
            print(f"\n🔝 Primeros 3 partidos:")
            print(df_partidos[['Fecha', 'Equipo_Local', 'Equipo_Visitante', 'Competicion']].head(3))
        
        return not df_partidos.empty
        
    except Exception as e:
        print(f"❌ Error en testing: {e}")
        return False


if __name__ == "__main__":
    test_fixture_processor()