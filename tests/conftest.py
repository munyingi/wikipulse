import pytest

from wikipulse.spark import delta_available, local_spark


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    s = local_spark(warehouse=str(tmp_path_factory.mktemp("warehouse")))
    yield s
    s.stop()


needs_delta = pytest.mark.skipif(not delta_available(), reason="delta-spark not installed (runs in CI)")
