from pathlib import Path

import awkward as ak
import graphed_histogram as gh
import numpy as np
import pytest
from graphed_executors.local import ThreadExecutor

pytest.importorskip("coffea")

from baselines.atlas_hyy_coffea import run
from examples.atlas_hyy import build_plan


@pytest.mark.parametrize("n_files", [1, 2])
def test_virtual_map_reduce_matches_graphed(tmp_path: Path, n_files: int):
    pt0 = [60.0, 60.0, 40.0, 60.0, 60.0, 80.0]
    pt1 = [60.0, 60.0, 60.0, 60.0, 60.0, 80.0]
    events = ak.Array(
        {
            "pt_0": pt0,
            "pt_1": pt1,
            "e_0": pt0,
            "e_1": pt1,
            "eta_0": [0.0] * 6,
            "eta_1": [0.0] * 6,
            "phi_0": [0.0] * 6,
            "phi_1": [np.pi, np.pi, np.pi, np.pi, 0.0, np.pi],
            "isTightID_0": [True, False, True, True, True, True],
            "isTightID_1": [True] * 6,
            "ptcone20_0": [0.0, 0.0, 0.0, 4.0, 0.0, 0.0],
            "ptcone20_1": [0.0] * 6,
        }
    )
    paths = [tmp_path / f"part-{i}.parquet" for i in range(n_files)]
    for path in paths:
        ak.to_parquet(events, path)
    actual = run(paths, workers=2)
    result = ThreadExecutor(max_workers=2).run(build_plan(paths))
    expected = gh.unpack(result.value)["diphoton_mass"]
    np.testing.assert_array_equal(actual.values(flow=True), expected.values(flow=True))
    assert actual.sum() == n_files
    assert actual.values(flow=True)[-1] == n_files
