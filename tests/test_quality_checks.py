from quality.checks import compute_residual, residual_ok


def test_residual_canonical_hour():
    assert compute_residual(180387, 180386, 0) == 1


def test_residual_ok_canonical():
    assert residual_ok(1, 180387, 0.0001)


def test_identity_holds_with_quarantine():
    assert compute_residual(180388, 180386, 2) == 0


def test_negative_residual_fails():
    assert not residual_ok(-5, 180387, 0.0001)


def test_threshold_boundary():
    assert residual_ok(10, 100000, 0.0001)
    assert not residual_ok(11, 100000, 0.0001)


def test_zero_raw():
    assert not residual_ok(100000, 0, 0.0001)
