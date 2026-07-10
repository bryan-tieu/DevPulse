def compute_residual(raw: int, hour_rows: int, quarantine: int) -> int:
    return raw - hour_rows - quarantine


def residual_ok(residual: int, raw: int, threshold: float) -> bool:
    # Residual falls below threshold, Return True
    # Residual is above threshold, Return False

    # DivisionByZero if raw is ever 0
    if raw <= 0:
        return False
    return (residual / raw) <= threshold and residual >= 0
