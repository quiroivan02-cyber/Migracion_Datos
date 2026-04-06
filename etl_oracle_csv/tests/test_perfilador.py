# tests/test_perfilador.py
import pytest
import pandas as pd
from transformacion.perfilador import perfilar_tabla


@pytest.fixture
def df_limpio():
    return pd.DataFrame({
        "PACIENTE_ID":      [1, 2, 3],
        "NOMBRE":           ["Juan", "María", "Pedro"],
        "FECHA_NACIMIENTO": ["1990-01-01", "1985-06-15", "2000-03-22"],
        "CREATED_AT":       ["2024-01-01", "2024-01-02", "2024-01-03"],
    })


@pytest.fixture
def df_con_nulos():
    return pd.DataFrame({
        "PACIENTE_ID":      [1, None, 3],
        "NOMBRE":           ["Juan", "María", None],
        "FECHA_NACIMIENTO": ["1990-01-01", None, "2000-03-22"],
        "CREATED_AT":       ["2024-01-01", "2024-01-02", "2024-01-03"],
    })


def test_perfil_retorna_dict(df_limpio):
    perfil = perfilar_tabla("PACIENTES", df_limpio)
    assert isinstance(perfil, dict)


def test_perfil_contiene_claves_requeridas(df_limpio):
    perfil = perfilar_tabla("PACIENTES", df_limpio)
    assert "tabla"       in perfil
    assert "total_filas" in perfil
    assert "total_cols"  in perfil
    assert "columnas"    in perfil


def test_perfil_cuenta_filas_correctamente(df_limpio):
    perfil = perfilar_tabla("PACIENTES", df_limpio)
    assert perfil["total_filas"] == 3


def test_perfil_detecta_nulos(df_con_nulos):
    perfil = perfilar_tabla("PACIENTES", df_con_nulos)
    assert perfil["columnas"]["PACIENTE_ID"]["nulos"] == 1
    assert perfil["columnas"]["NOMBRE"]["nulos"] == 1
    assert perfil["columnas"]["FECHA_NACIMIENTO"]["nulos"] == 1


def test_perfil_porcentaje_nulos(df_con_nulos):
    perfil = perfilar_tabla("PACIENTES", df_con_nulos)
    pct = perfil["columnas"]["PACIENTE_ID"]["pct_nulos"]
    assert round(pct, 2) == 33.33


def test_perfil_df_vacio():
    df = pd.DataFrame(columns=["PACIENTE_ID", "NOMBRE"])
    perfil = perfilar_tabla("PACIENTES", df)
    assert perfil["total_filas"] == 0