"""
Create Daily NO2 Data from Hourly Data
Agrega dados horários de NO2 em médias diárias
"""

import pandas as pd
import os


def create_daily_no2():
    """
    Converte dados horários de NO2 em médias diárias
    """

    # Arquivo de entrada (dados horários)
    hourly_file = "extract_1000_daily_la.py"

    if not os.path.exists(hourly_file):
        print(f"❌ Arquivo não encontrado: {hourly_file}")
        return

    print(f"📖 Lendo dados horários: {hourly_file}")
    df = pd.read_csv(hourly_file, parse_dates=["timestamp"])

    print(f"   ✅ {len(df)} pontos horários carregados")
    print(f"   📅 Período: {df['timestamp'].min()} → {df['timestamp'].max()}")

    # Agrupar por data e calcular médias diárias
    df["date"] = df["timestamp"].dt.date
    daily_df = df.groupby("date").agg({"no2_ug_m3": "mean"}).reset_index()

    # Criar timestamp à meia-noite para cada dia
    daily_df["timestamp"] = pd.to_datetime(daily_df["date"])
    daily_df = daily_df[["timestamp", "no2_ug_m3"]].copy()

    # Arredondar para 3 casas decimais
    daily_df["no2_ug_m3"] = daily_df["no2_ug_m3"].round(3)

    # Salvar dados diários
    output_file = "MODELS/OPENAQ/no2/DAYS/la_no2_daily.csv"
    daily_df.to_csv(output_file, index=False)

    print(f"\n🌅 Dados diários criados:")
    print(f"   📁 Arquivo: {output_file}")
    print(f"   📊 {len(daily_df)} pontos diários")
    print(f"   📅 Período: {daily_df['timestamp'].min().date()} → {daily_df['timestamp'].max().date()}")

    print(f"\n📋 Amostra dos dados diários:")
    print(daily_df.head(10).to_string(index=False))

    print(f"\n📈 Estatísticas:")
    print(f"   Média diária média: {daily_df['no2_ug_m3'].mean():.2f} μg/m³")
    print(f"   Min: {daily_df['no2_ug_m3'].min():.2f} μg/m³")
    print(f"   Max: {daily_df['no2_ug_m3'].max():.2f} μg/m³")

    # Informações sobre agregação
    original_hours = len(df)
    daily_points = len(daily_df)
    hours_per_day_avg = original_hours / daily_points

    print(f"\n📊 Informações da agregação:")
    print(f"   Original: {original_hours} pontos horários")
    print(f"   Agregado: {daily_points} pontos diários")
    print(f"   Média de horas por dia: {hours_per_day_avg:.1f}")


if __name__ == "__main__":
    print("🌅 AGREGADOR DE DADOS NO2 - HORÁRIO → DIÁRIO")
    print("=" * 55)

    try:
        create_daily_no2()
        print("\n✅ Agregação concluída com sucesso!")

    except Exception as e:
        print(f"❌ Erro durante a agregação: {e}")
        import traceback

        traceback.print_exc()
