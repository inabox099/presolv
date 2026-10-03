def test_native_module_importable():
    from presolv import _presolv

    assert _presolv.native_version() == "0.1.0"
