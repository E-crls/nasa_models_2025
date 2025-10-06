"""
Split LA Air Quality Data into Separate Pollutant Files
Separa o arquivo la_air_quality_1000points.csv em arquivos individuais por poluente
"""

import pandas as pd
import os


def split_air_quality_data():
    """
    Separa o arquivo principal de qualidade do ar em arquivos individuais
    por poluente (NO2 e O3)
    """

    # Arquivo de entrada
    input_file = "la_air_quality_1000points.csv"

    if not os.path.exists(input_file):
        print(f"❌ Arquivo não encontrado: {input_file}")
        return

    # Ler o arquivo original
    print(f"📖 Lendo arquivo: {input_file}")
    df = pd.read_csv(input_file)
    print(f"   ✅ {len(df)} linhas carregadas")
    print(f"   📊 Colunas: {list(df.columns)}")

    # Criar diretórios se não existirem
    os.makedirs("no2", exist_ok=True)
    os.makedirs("o3", exist_ok=True)

    # Criar dataset NO2
    print(f"\n🔬 Criando arquivo NO2...")
    no2_df = df[["timestamp", "no2_ug_m3"]].copy()
    no2_output = "no2/la_no2_1000points.csv"
    no2_df.to_csv(no2_output, index=False)
    print(f"   ✅ Criado: {no2_output} ({len(no2_df)} linhas)")
    print(f"   📋 Amostra:")
    print(f"      {no2_df.head(3).to_string(index=False)}")

    # Criar dataset O3
    print(f"\n🌍 Criando arquivo O3...")
    o3_df = df[["timestamp", "o3_ug_m3"]].copy()
    o3_output = "o3/la_o3_1000points.csv"
    o3_df.to_csv(o3_output, index=False)
    print(f"   ✅ Criado: {o3_output} ({len(o3_df)} linhas)")
    print(f"   📋 Amostra:")
    print(f"      {o3_df.head(3).to_string(index=False)}")

    print(f"\n🏁 Separação concluída com sucesso!")
    print(f"   📁 NO2: {no2_output}")
    print(f"   📁 O3: {o3_output}")


def verify_files():
    """Verifica se os arquivos foram criados corretamente"""
    files_to_check = ["no2/la_no2_1000points.csv", "o3/la_o3_1000points.csv"]

    print("\n🔍 Verificando arquivos criados:")
    for file_path in files_to_check:
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            df_check = pd.read_csv(file_path)
            print(f"   ✅ {file_path}: {len(df_check)} linhas, {file_size} bytes")
        else:
            print(f"   ❌ {file_path}: não encontrado")


if __name__ == "__main__":
    print("🚀 SEPARADOR DE DADOS DE QUALIDADE DO AR")
    print("=" * 50)

    try:
        split_air_quality_data()
        verify_files()

    except Exception as e:
        print(f"❌ Erro durante a separação: {e}")
        import traceback

        traceback.print_exc()
