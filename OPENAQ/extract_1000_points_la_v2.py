"""
Extrai 1000 pontos temporais (horas) de estações em Los Angeles
Para NO₂, O₃ e HCHO
Versão 2: Usa sensor_ids diretos
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time


def extract_measurements_from_sensor(
    api_key: str, sensor_id: int, parameter_name: str, target_points: int = 10000
) -> pd.DataFrame:
    """
    Extrai medições de um sensor específico

    Args:
        api_key: API key OpenAQ
        sensor_id: ID do sensor
        parameter_name: Nome do parâmetro (no2, o3, hcho)
        target_points: Número alvo de pontos

    Returns:
        DataFrame com timestamp e valor
    """
    print(f"\n📊 Extraindo {parameter_name.upper()} (Sensor {sensor_id})...")

    session = requests.Session()
    session.headers.update({"X-API-Key": api_key, "Accept": "application/json"})

    all_measurements = []
    page = 1

    # Período: últimos 2 anos
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=730)

    try:
        while len(all_measurements) < target_points:
            params = {
                "datetime_from": start_date.isoformat() + "Z",
                "datetime_to": end_date.isoformat() + "Z",
                "limit": 10000,
                "page": page,
            }

            response = session.get(
                f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements", params=params, timeout=30
            )
            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])

            if not results:
                print(f"   Sem mais dados (página {page})")
                break

            # Processar resultados
            for record in results:
                try:
                    # Timestamp está em period.datetimeFrom.utc
                    period = record.get("period", {})
                    datetime_from = period.get("datetimeFrom", {})
                    timestamp_str = datetime_from.get("utc")

                    if not timestamp_str:
                        continue

                    timestamp = pd.to_datetime(timestamp_str)
                    value = record.get("value")
                    unit = record.get("parameter", {}).get("units", "unknown")

                    if value is not None:
                        # Converter para μg/m³ se necessário
                        if unit.lower() == "ppm":
                            if parameter_name.lower() == "no2":
                                value = value * 1880
                            elif parameter_name.lower() == "o3":
                                value = value * 1960
                            elif parameter_name.lower() == "hcho":
                                value = value * 1230

                        all_measurements.append({"timestamp": timestamp, "value": value})

                except Exception:
                    continue

            print(f"   Página {page}: +{len(results)} medições (total: {len(all_measurements)})")

            if len(all_measurements) >= target_points:
                break

            page += 1

            if page > 50:
                print(f"   ⚠️  Limite de páginas atingido")
                break

            time.sleep(0.5)

        # Criar DataFrame
        df = pd.DataFrame(all_measurements)

        if len(df) > 0:
            df = df.sort_values("timestamp").reset_index(drop=True)

            # Limitar ao número alvo
            if len(df) > target_points:
                df = df.tail(target_points).reset_index(drop=True)

            print(f"   ✅ Total extraído: {len(df)} pontos")
            print(f"   Período: {df['timestamp'].min()} a {df['timestamp'].max()}")
        else:
            print(f"   ⚠️  Nenhum dado extraído")

        return df

    except Exception as e:
        print(f"   ❌ Erro: {e}")
        return pd.DataFrame(columns=["timestamp", "value"])


def extract_1000_points_los_angeles(api_key: str, output_csv: str = "la_air_quality_1000points.csv") -> str:
    """
    Extrai 1000 pontos temporais de estações em Los Angeles

    Estações usadas (do CSV de referência):
    - Los Angeles - N. Main: NO₂ (25192), O₃ (25193)
    - Pasadena: NO₂ (25473), O₃ (25474)
    - Long Beach: NO₂ (25188), O₃ (25189)

    Args:
        api_key: API key OpenAQ
        output_csv: Caminho do CSV de saída

    Returns:
        Caminho do CSV gerado
    """
    print("=" * 80)
    print("🌆 EXTRAÇÃO DE 1000 PONTOS - LOS ANGELES")
    print("=" * 80)
    print(f"\n🎯 Objetivo: 1000 pontos para NO₂, O₃ e HCHO")
    print(f"📍 Estações: Los Angeles, Pasadena, Long Beach")

    # ========================================================================
    # SENSOR IDs (do CSV de referência openaq_stations_usa.csv)
    # ========================================================================

    # Los Angeles - N. Main (34.066429, -118.226755)
    NO2_SENSOR = 25192
    O3_SENSOR = 25193

    # HCHO: Não há estações com HCHO em Los Angeles (normal)
    HCHO_SENSOR = None

    # ========================================================================
    # EXTRAIR DADOS
    # ========================================================================
    print(f"\n{'=' * 80}")
    print("📥 EXTRAINDO DADOS")
    print("=" * 80)

    # NO₂
    df_no2 = extract_measurements_from_sensor(
        api_key=api_key, sensor_id=NO2_SENSOR, parameter_name="no2", target_points=10000
    )
    df_no2 = df_no2.rename(columns={"value": "no2_ug_m3"})

    # O₃
    df_o3 = extract_measurements_from_sensor(
        api_key=api_key, sensor_id=O3_SENSOR, parameter_name="o3", target_points=10000
    )
    df_o3 = df_o3.rename(columns={"value": "o3_ug_m3"})

    # HCHO
    if HCHO_SENSOR:
        df_hcho = extract_measurements_from_sensor(
            api_key=api_key, sensor_id=HCHO_SENSOR, parameter_name="hcho", target_points=1000
        )
        df_hcho = df_hcho.rename(columns={"value": "hcho_ug_m3"})
    else:
        print(f"\n⚠️  HCHO: Sem estação disponível em Los Angeles (normal - raro)")
        df_hcho = pd.DataFrame(columns=["timestamp", "hcho_ug_m3"])

    # ========================================================================
    # COMBINAR DADOS
    # ========================================================================
    print(f"\n{'=' * 80}")
    print("🔗 COMBINANDO DADOS")
    print("=" * 80)

    # Criar união de todos os timestamps
    all_timestamps = set()

    if len(df_no2) > 0:
        all_timestamps.update(df_no2["timestamp"].tolist())
    if len(df_o3) > 0:
        all_timestamps.update(df_o3["timestamp"].tolist())
    if len(df_hcho) > 0:
        all_timestamps.update(df_hcho["timestamp"].tolist())

    # Criar DataFrame base
    df_combined = pd.DataFrame({"timestamp": sorted(list(all_timestamps))})

    # Merge dados
    if len(df_no2) > 0:
        df_combined = df_combined.merge(df_no2, on="timestamp", how="left")
    else:
        df_combined["no2_ug_m3"] = np.nan

    if len(df_o3) > 0:
        df_combined = df_combined.merge(df_o3, on="timestamp", how="left")
    else:
        df_combined["o3_ug_m3"] = np.nan

    if len(df_hcho) > 0:
        df_combined = df_combined.merge(df_hcho, on="timestamp", how="left")
    else:
        df_combined["hcho_ug_m3"] = np.nan

    # Limitar a 10000 pontos (pegar os mais recentes)
    if len(df_combined) > 10000:
        df_combined = df_combined.tail(10000).reset_index(drop=True)

    print(f"\n📊 DADOS COMBINADOS:")
    print(f"   Total de pontos: {len(df_combined)}")
    print(f"   NO₂: {df_combined['no2_ug_m3'].notna().sum()} pontos")
    print(f"   O₃: {df_combined['o3_ug_m3'].notna().sum()} pontos")
    print(f"   HCHO: {df_combined['hcho_ug_m3'].notna().sum()} pontos")

    if len(df_combined) > 0:
        print(f"   Período: {df_combined['timestamp'].min()} a {df_combined['timestamp'].max()}")

    # ========================================================================
    # SALVAR CSV
    # ========================================================================
    print(f"\n{'=' * 80}")
    print("💾 SALVANDO CSV")
    print("=" * 80)

    df_combined.to_csv(output_csv, index=False)

    print(f"\n✅ CSV salvo: {output_csv}")
    print(f"   Linhas: {len(df_combined)}")
    print(f"   Colunas: timestamp, no2_ug_m3, o3_ug_m3, hcho_ug_m3")

    # Estatísticas
    if df_combined["no2_ug_m3"].notna().any():
        print(f"\n📈 NO₂:")
        print(f"   Média: {df_combined['no2_ug_m3'].mean():.2f} μg/m³")
        print(f"   Mín: {df_combined['no2_ug_m3'].min():.2f} μg/m³")
        print(f"   Máx: {df_combined['no2_ug_m3'].max():.2f} μg/m³")

    if df_combined["o3_ug_m3"].notna().any():
        print(f"\n📈 O₃:")
        print(f"   Média: {df_combined['o3_ug_m3'].mean():.2f} μg/m³")
        print(f"   Mín: {df_combined['o3_ug_m3'].min():.2f} μg/m³")
        print(f"   Máx: {df_combined['o3_ug_m3'].max():.2f} μg/m³")

    if df_combined["hcho_ug_m3"].notna().any():
        print(f"\n📈 HCHO:")
        print(f"   Média: {df_combined['hcho_ug_m3'].mean():.2f} μg/m³")
        print(f"   Mín: {df_combined['hcho_ug_m3'].min():.2f} μg/m³")
        print(f"   Máx: {df_combined['hcho_ug_m3'].max():.2f} μg/m³")

    print(f"\n{'=' * 80}")
    print("✅ EXTRAÇÃO CONCLUÍDA!")
    print("=" * 80)

    return output_csv


if __name__ == "__main__":
    # Executar
    import os

    API_KEY = os.getenv("OPENAQ_API_KEY", "")

    if not API_KEY:
        print("❌ OPENAQ_API_KEY não configurada nas variáveis de ambiente")
        exit(1)

    csv_path = extract_1000_points_los_angeles(api_key=API_KEY, output_csv="la_air_quality_1000points.csv")

    print(f"\n📁 Arquivo gerado: {csv_path}")

    # Mostrar primeiras linhas
    print(f"\n📋 PREVIEW:")
    df = pd.read_csv(csv_path)
    print(df.head(10))
