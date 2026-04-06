# tests/test_limpieza.py
import pytest
import pandas as pd
from transformacion.limpieza import (
    limpiar_espacios,
    limpiar_duplicados,
    limpiar_fechas,
    limpiar_tabla,
)


@pytest.fixture
def df_espacios():
    return pd.DataFrame({
        "NOMBRE":    ["  Juan  ", "María ", " Pedro"],
        "CIUDAD":    [" Bogotá", "Medellín  ", "Cali"],
    })


@pytest.fixture
def df_duplicados():
    return pd.DataFrame({
        "PACIENTE_ID": [1, 2, 2, 3],
        "NOMBRE":      ["Juan", "María", "María", "Pedro"],
    })


@pytest.fixture
def df_fechas_invalidas():
    return pd.DataFrame({
        "PACIENTE_ID":      [1, 2, 3],
        "FECHA_NACIMIENTO": ["1990-01-01", "no-es-fecha", "2000-03-22"],
    })


@pytest.fixture
def df_nulos_requeridos():
    return pd.DataFrame({
        "PACIENTE_ID": [1, None, 3],
        "NOMBRE":      ["Juan", "María", "Pedro"],
        "FECHA_NACIMIENTO": ["1990-01-01", "1985-06-15", "2000-03-22"],
        "CREATED_AT":  ["2024-01-01", "2024-01-02", "2024-01-03"],
    })


# ── Tests limpiar_espacios ────────────────────────────────────────────────────
def test_elimina_espacios(df_espacios):
    df = limpiar_espacios(df_espacios)
    assert df["NOMBRE"][0] == "Juan"
    assert df["CIUDAD"][0] == "Bogotá"
    assert df["NOMBRE"][1] == "María"


# ── Tests limpiar_duplicados ──────────────────────────────────────────────────
def test_elimina_duplicados(df_duplicados):
    df_clean, dupl = limpiar_duplicados(df_duplicados, "PACIENTES")
    assert len(df_clean) == 3
    assert len(dupl) == 1


def test_sin_duplicados_no_elimina_filas(df_espacios):
    df_clean, dupl = limpiar_duplicados(df_espacios, "TEST")
    assert len(df_clean) == len(df_espacios)
    assert len(dupl) == 0


# ── Tests limpiar_fechas ──────────────────────────────────────────────────────
def test_detecta_fechas_invalidas(df_fechas_invalidas):
    df, problemas = limpiar_fechas(df_fechas_invalidas, ["FECHA_NACIMIENTO"])
    assert len(problemas) == 1


def test_convierte_fechas_validas(df_fechas_invalidas):
    df, _ = limpiar_fechas(df_fechas_invalidas, ["FECHA_NACIMIENTO"])
    assert pd.api.types.is_datetime64_any_dtype(df["FECHA_NACIMIENTO"])


# ── Tests limpiar_tabla ───────────────────────────────────────────────────────
def test_limpiar_tabla_elimina_nulos_requeridos(df_nulos_requeridos):
    df_clean, problemas = limpiar_tabla("PACIENTES", df_nulos_requeridos)
    assert df_clean["PACIENTE_ID"].isna().sum() == 0
    assert len(problemas) > 0


def test_limpiar_tabla_retorna_dataframes(df_nulos_requeridos):
    df_clean, problemas = limpiar_tabla("PACIENTES", df_nulos_requeridos)
    assert isinstance(df_clean, pd.DataFrame)
    assert isinstance(problemas, pd.DataFrame)


def test_limpiar_tabla_datos_perfectos():
    df = pd.DataFrame({
        "PACIENTE_ID": [1, 2, 3],
        "NOMBRE":      ["Juan", "María", "Pedro"],
        "FECHA_NACIMIENTO": ["1990-01-01", "1985-06-15", "2000-03-22"],
        "CREATED_AT":  ["2024-01-01", "2024-01-02", "2024-01-03"],
    })
    df_clean, problemas = limpiar_tabla("PACIENTES", df)
    assert len(df_clean) == 3
    assert len(problemas) == 0