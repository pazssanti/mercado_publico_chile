"""
Módulo de Normalización de RUTs Chilenos
========================================
Funciones para normalizar y validar RUTs en datasets de Mercado Público Chile.

Campos aplicables:
- RutUnidadCompra, RutSucursal (Órdenes de Compra)
- RutUnidad, RutProveedor (Licitaciones)
- rut_raw / rut_normalizado (Oferentes)
- rut, dv (SII)
"""

from typing import Optional, Tuple
import re


def normalizar_rut(rut_input: str) -> Tuple[Optional[int], Optional[str]]:
    """
    Normaliza un RUT chilensis desde múltiples formatos de entrada.

    Args:
        rut_input: RUT en formato string (puede contener puntos, guiones, etc.)

    Returns:
        Tuple[cuerpo_numrico (Int64), dv (str)] o (None, None) si es inválido

    Formatos aceptados:
        - "76063553-7"
        - "76.063.553-7"
        - "760635537"
        - "76063553K"
        - "K" (solo dv)
    """
    if rut_input is None or (isinstance(rut_input, str) and rut_input.strip() == ""):
        return None, None

    rut_input = str(rut_input).strip().upper()

    # Remover caracteres no válidos excepto K
    rut_input = re.sub(r'[^0-9K]', '', rut_input)

    if not rut_input:
        return None, None

    # Detectar si es solo el dígito verificador
    if len(rut_input) == 1 and rut_input in "0123456789K":
        return None, rut_input

    # Separar cuerpo del dv
    if len(rut_input) == 1:
        return None, rut_input

    # El último carácter es el dv
    dv = rut_input[-1]

    if dv.isdigit():
        dv = dv
    elif dv == "K":
        dv = "K"
    else:
        return None, None

    cuerpo_str = rut_input[:-1]

    # Validar que el cuerpo sea numérico
    if not cuerpo_str.isdigit():
        return None, None

    try:
        cuerpo = int(cuerpo_str)
    except ValueError:
        return None, None

    return cuerpo, dv


def normalizar_rut_completo(rut_input: str) -> Optional[str]:
    """
    Retorna el RUT normalizado en formato puramente numérico sin puntos ni guion.

    Args:
        rut_input: RUT en formato string

    Returns:
        String numérico sin puntos ni guion (rellenado con ceros a la izquierda si es necesario)
        o None si es inválido
    """
    cuerpo, dv = normalizar_rut(rut_input)

    if cuerpo is None or dv is None:
        return None

    # Rellenar con ceros a la izquierda (cuerpo debe tener al menos 5 dígitos según normativa chilena)
    cuerpo_str = str(cuerpo).zfill(5)

    return f"{cuerpo_str}-{dv}"


def validar_dv(cuerpo: int, dv: str) -> bool:
    """
    Valida que el dígito verificador sea correcto para un cuerpo de RUT.

    Args:
        cuerpo: Cuerpo numérico del RUT (sin dv)
        dv: Dígito verificador a validar

    Returns:
        True si el dv es válido, False en caso contrario
    """
    if dv is None:
        return False

    dv = str(dv).upper()

    if dv not in "0123456789K":
        return False

    # Algoritmo Módulo 11
    reversed_digits = map(int, str(cuerpo))
    coefficients = [2, 3, 4, 5, 6, 7, 2, 3, 4, 5, 6, 7]

    total = sum(d * c for d, c in zip(reversed_digits, coefficients))

    remainder = total % 11
    expected_dv = 11 - remainder

    if expected_dv == 11:
        expected_dv_str = "0"
    elif expected_dv == 10:
        expected_dv_str = "K"
    else:
        expected_dv_str = str(expected_dv)

    return dv == expected_dv_str.upper()


# Ejemplo de uso
if __name__ == "__main__":
    test_ruts = [
        "76063553-7",
        "76.063.553-7",
        "760635537",
        "76063553K",
        "76670811-0",
        "invalid",
        "",
        None
    ]

    print("=== PRUEBA DE NORMALIZACIÓN DE RUTs ===\n")

    for rut in test_ruts:
        cuerpo, dv = normalizar_rut(rut)
        completo = normalizar_rut_completo(rut)

        print(f"Input: {repr(rut)}")
        print(f"  -> Cuerpo: {cuerpo}, DV: {dv}")
        print(f"  -> Completo: {completo}")

        if cuerpo is not None and dv is not None:
            es_valido = validar_dv(cuerpo, dv)
            print(f"  -> DV válido: {es_valido}")

        print()