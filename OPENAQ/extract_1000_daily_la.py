"""
Extrai 1000 pontos de MÉDIAS DIÁRIAS de NO₂ e O₃ de Los Angeles
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time


def extract_daily_measurements_from_sensor(
    api_key: str, sensor_id: int, parameter_name: str, target_points: int = 1000
) -> pd.DataFrame:
    """
    Extrai médias diárias de um sensor específico

    Args:
        api_key: API key OpenAQ
        sensor_id: ID do sensor
        parameter_name: Nome do parâmetro (no2, o3)
        target_points: Número alvo de pontos

    Returns:
        DataFrame com date e valor médio diário
    """
    print(f"\n📊 Extraindo {parameter_name.upper()} DIÁRIO (Sensor {sensor_id})...")

    session = requests.Session()
    session.headers.update({"X-API-Key": api_key, "Accept": "application/json"})

    all_measurements = []
    page = 1

    # Período: últimos ~3 anos para ter 1000 dias
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=1100)  # margem de segurança

    try:
        while len(all_measurements) < target_points:
            params = {
                "datetime_from": start_date.isoformat() + "Z",
                "datetime_to": end_date.isoformat() + "Z",
                "limit": 1000,
                "page": page,
            }

            # Usar endpoint /days para médias diárias
            response = session.get(f"https://api.openaq.org/v3/sensors/{sensor_id}/days", params=params, timeout=30)
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

                    # Para médias diárias, usar apenas a data
                    timestamp = pd.to_datetime(timestamp_str).date()

                    # Valor é a média diária
                    value = record.get("value")
                    unit = record.get("parameter", {}).get("units", "unknown")

                    if value is not None:
                        # Converter para μg/m³ se necessário
                        if unit.lower() == "ppm":
                            if parameter_name.lower() == "no2":
                                value = value * 1880  # NO₂: 1 ppm = 1880 μg/m³
                            elif parameter_name.lower() == "o3":
                                value = value * 1960  # O₃: 1 ppm = 1960 μg/m³

                        all_measurements.append({"date": timestamp, "value": value})

                except Exception:
                    continue

            print(f"   Página {page}: +{len(results)} dias (total: {len(all_measurements)})")

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
            df = df.sort_values("date").reset_index(drop=True)

            # Limitar ao número alvo
            if len(df) > target_points:
                df = df.tail(target_points).reset_index(drop=True)

            print(f"   ✅ Total extraído: {len(df)} dias")
            print(f"   Período: {df['date'].min()} a {df['date'].max()}")
        else:
            print(f"   ⚠️  Nenhum dado extraído")

        return df

    except Exception as e:
        print(f"   ❌ Erro: {e}")
        return pd.DataFrame(columns=["date", "value"])


def extract_1000_daily_points_los_angeles(api_key: str, output_csv: str = "la_air_quality_1000days.csv") -> str:
    """
    Extrai 1000 pontos de MÉDIAS DIÁRIAS de NO₂ e O₃ de Los Angeles

    Args:
        api_key: API key OpenAQ
        output_csv: Caminho do CSV de saída

    Returns:
        Caminho do CSV gerado
    """
    print("=" * 80)
    print("🌆 EXTRAÇÃO DE 1000 MÉDIAS DIÁRIAS - LOS ANGELES")
    print("=" * 80)
    print(f"\n🎯 Objetivo: 1000 pontos de médias diárias para NO₂ e O₃")
    print(f"📍 Estação: Los Angeles - N. Main")

    # ========================================================================
    # SENSOR IDs (Los Angeles - N. Main)
    # ========================================================================

    NO2_SENSOR = 25192
    O3_SENSOR = 25193

    # ========================================================================
    # EXTRAIR DADOS DIÁRIOS
    # ========================================================================
    print(f"\n{'=' * 80}")
    print("📥 EXTRAINDO MÉDIAS DIÁRIAS")
    print("=" * 80)

    # NO₂
    df_no2 = extract_daily_measurements_from_sensor(
        api_key=api_key, sensor_id=NO2_SENSOR, parameter_name="no2", target_points=1000
    )
    df_no2 = df_no2.rename(columns={"value": "no2_ug_m3"})

    # O₃
    df_o3 = extract_daily_measurements_from_sensor(
        api_key=api_key, sensor_id=O3_SENSOR, parameter_name="o3", target_points=1000
    )
    df_o3 = df_o3.rename(columns={"value": "o3_ug_m3"})

    # ========================================================================
    # COMBINAR DADOS
    # ========================================================================
    print(f"\n{'=' * 80}")
    print("🔗 COMBINANDO DADOS")
    print("=" * 80)

    # Criar união de todas as datas
    all_dates = set()

    if len(df_no2) > 0:
        all_dates.update(df_no2["date"].tolist())
    if len(df_o3) > 0:
        all_dates.update(df_o3["date"].tolist())

    # Criar DataFrame base
    df_combined = pd.DataFrame({"date": sorted(list(all_dates))})

    # Merge dados
    if len(df_no2) > 0:
        df_combined = df_combined.merge(df_no2, on="date", how="left")
    else:
        df_combined["no2_ug_m3"] = np.nan

    if len(df_o3) > 0:
        df_combined = df_combined.merge(df_o3, on="date", how="left")
    else:
        df_combined["o3_ug_m3"] = np.nan

    # Limitar a 1000 pontos (pegar os mais recentes)
    if len(df_combined) > 1000:
        df_combined = df_combined.tail(1000).reset_index(drop=True)

    print(f"\n📊 DADOS COMBINADOS:")
    print(f"   Total de dias: {len(df_combined)}")
    print(f"   NO₂: {df_combined['no2_ug_m3'].notna().sum()} dias")
    print(f"   O₃: {df_combined['o3_ug_m3'].notna().sum()} dias")

    if len(df_combined) > 0:
        print(f"   Período: {df_combined['date'].min()} a {df_combined['date'].max()}")

        # Calcular duração em anos
        duration_days = (df_combined["date"].max() - df_combined["date"].min()).days
        duration_years = duration_days / 365.25
        print(f"   Duração: {duration_days} dias (~{duration_years:.1f} anos)")

    # ========================================================================
    # SALVAR CSV
    # ========================================================================
    print(f"\n{'=' * 80}")
    print("💾 SALVANDO CSV")
    print("=" * 80)

    df_combined.to_csv(output_csv, index=False)

    print(f"\n✅ CSV salvo: {output_csv}")
    print(f"   Linhas: {len(df_combined)}")
    print(f"   Colunas: date, no2_ug_m3, o3_ug_m3")

    # Estatísticas
    if df_combined["no2_ug_m3"].notna().any():
        print(f"\n📈 NO₂ (Média Diária):")
        print(f"   Média: {df_combined['no2_ug_m3'].mean():.2f} μg/m³")
        print(f"   Mín: {df_combined['no2_ug_m3'].min():.2f} μg/m³")
        print(f"   Máx: {df_combined['no2_ug_m3'].max():.2f} μg/m³")
        print(f"   Desvio padrão: {df_combined['no2_ug_m3'].std():.2f} μg/m³")

    if df_combined["o3_ug_m3"].notna().any():
        print(f"\n📈 O₃ (Média Diária):")
        print(f"   Média: {df_combined['o3_ug_m3'].mean():.2f} μg/m³")
        print(f"   Mín: {df_combined['o3_ug_m3'].min():.2f} μg/m³")
        print(f"   Máx: {df_combined['o3_ug_m3'].max():.2f} μg/m³")
        print(f"   Desvio padrão: {df_combined['o3_ug_m3'].std():.2f} μg/m³")

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

    csv_path = extract_1000_daily_points_los_angeles(api_key=API_KEY, output_csv="la_air_quality_1000days.csv")

    print(f"\n📁 Arquivo gerado: {csv_path}")

    # Mostrar primeiras e últimas linhas
    print(f"\n📋 PREVIEW:")
    df = pd.read_csv(csv_path)
    print("\nPrimeiros 5 dias:")
    print(df.head(5))
    print("\nÚltimos 5 dias:")
    print(df.tail(5))
