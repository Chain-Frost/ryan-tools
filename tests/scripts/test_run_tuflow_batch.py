import sys
import importlib.util
from pathlib import Path
import pytest
from typing import Any

# --- Module Loading Helper ---
@pytest.fixture(scope="module")
def rtb():
    """Load the run_tuflow_batch module dynamically."""
    repo_root = Path(__file__).parents[2]
    script_path = repo_root / "ryan-scripts" / "TUFLOW-python" / "run_tuflow_batch.py"
    
    if not script_path.exists():
        pytest.fail(f"Could not find script at {script_path}")

    spec = importlib.util.spec_from_file_location("run_tuflow_batch", script_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_tuflow_batch"] = module
    spec.loader.exec_module(module)
    return module

# --- Fixtures ---
@pytest.fixture
def mock_paths(tmp_path):
    """Create dummy TCF and EXE files."""
    tcf = tmp_path / "test_model_~s1~_~e1~.tcf"
    tcf.touch()
    exe = tmp_path / "TUFLOW_iSP_w64.exe"
    exe.touch()
    return tcf, exe

@pytest.fixture
def core_params(rtb, mock_paths):
    """Return a valid CoreParameters instance."""
    tcf, exe = mock_paths
    return rtb.CoreParameters(
        tcf=tcf,
        tuflowexe=exe,
        gpu_devices=None,
        smart_mode="parameter_product"
    )

# --- Tests ---

def test_core_params_defaults(rtb, mock_paths):
    """Test CoreParameters default values."""
    tcf, exe = mock_paths
    cp = rtb.CoreParameters(tcf=tcf, tuflowexe=exe)
    assert cp.batch_commands == "-x"
    assert cp.computational_priority == "NORMAL"
    assert cp.run_simulations is True
    assert cp.gpu_devices is None

def test_check_and_set_defaults_valid(rtb, core_params):
    """Test validation with valid parameters."""
    params = rtb.Parameters(core_params=core_params, run_variables={})
    # Should not raise
    rtb.check_and_set_defaults(params)

def test_check_and_set_defaults_missing_files(rtb, tmp_path):
    """Test validation fails if files don't exist."""
    tcf = tmp_path / "non_existent.tcf"
    exe = tmp_path / "non_existent.exe"
    cp = rtb.CoreParameters(tcf=tcf, tuflowexe=exe)
    params = rtb.Parameters(core_params=cp, run_variables={})
    
    with pytest.raises(FileNotFoundError, match="TCF file not found"):
        rtb.check_and_set_defaults(params)

def test_check_and_set_defaults_invalid_priority(rtb, core_params):
    """Test validation fails with invalid priority."""
    core_params.computational_priority = "SUPER_HIGH"
    params = rtb.Parameters(core_params=core_params, run_variables={}  )
    with pytest.raises(ValueError, match="Invalid priority: SUPER_HIGH. Must be one of:"):
        rtb.check_and_set_defaults(params)

def test_check_and_set_defaults_invalid_gpu(rtb, core_params):
    """Test validation fails with invalid GPU flags."""
    core_params.gpu_devices = ["-puA"] # Invalid format
    params = rtb.Parameters(core_params=core_params, run_variables={})
    with pytest.raises(ValueError, match="Invalid GPU flag"):
        rtb.check_and_set_defaults(params)

def test_check_and_set_defaults_invalid_run_var_key(rtb, core_params):
    """Test validation fails with invalid run variable keys."""
    params = rtb.Parameters(core_params=core_params, run_variables={"x1": ["val"]})
    with pytest.raises(ValueError, match="Invalid run variable key"):
        rtb.check_and_set_defaults(params)

def test_compute_simulations_product(rtb, core_params):
    """Test simulation generation using parameter product."""
    run_vars = {
        "s1": ["A", "B"],
        "e1": ["10", "20"]
    }
    params = rtb.Parameters(core_params=core_params, run_variables=run_vars)
    
    sims = rtb.compute_simulations(params)
    
    # Should have 2 * 2 = 4 simulations
    assert len(sims) == 4
    
    # Check args of first sim
    # Order depends on priority_order or insertion order. 
    # Default is insertion order: s1 then e1.
    # Sim 1: s1=A, e1=10
    args0 = sims[0].args_for_python
    assert "-s1" in args0
    assert "A" in args0
    assert "-e1" in args0
    assert "10" in args0

def test_compute_simulations_filtering(rtb, core_params, tmp_path):
    """Test that variables not in TCF filename are ignored/warned (depending on implementation).
    Actually the implementation says: "We can return flags that are not present in the tcf filename."
    But `filter_parameters` warns. `_enforce_placeholders` is used in textfiles mode?
    Wait, `compute_simulations` calls `filter_parameters`.
    """
    # TCF name is "test_model_~s1~_~e1~.tcf" (from mock_paths)
    # Add an extra variable "s2"
    run_vars = {
        "s1": ["A"],
        "e1": ["10"],
        "s2": ["Extra"]
    }
    params = rtb.Parameters(core_params=core_params, run_variables=run_vars)
    
    # filter_parameters will warn but return non_empty.
    # compute_simulations uses filtered_vars.
    
    sims = rtb.compute_simulations(params)
    assert len(sims) == 1
    args = sims[0].args_for_python
    # s2 should be present because filter_parameters only filters empty values, 
    # it just warns about extras.
    assert "-s2" in args 

def test_parse_input_files(rtb, tmp_path):
    """Test parsing of text files."""
    f = tmp_path / "runs.txt"
    content = """
    -s1 A -e1 10
    -s1 B -e1 20  ! Comment
    # Comment line
    -s1 A -e1 10  ! Duplicate
    """
    f.write_text(content, encoding="utf-8")
    
    combos, first_seen = rtb.parse_input_files([f])
    
    assert len(combos) == 2 # A-10 and B-20. Duplicate A-10 ignored.
    assert combos[0] == {"s1": "A", "e1": "10"}
    assert combos[1] == {"s1": "B", "e1": "20"}

def test_get_batch_flags_conflict(rtb, core_params):
    """Test conflict detection between batch_commands and gpu_devices."""
    core_params.batch_commands = "-b -pu0"
    core_params.gpu_devices = ["-pu1"]
    
    with pytest.raises(ValueError, match="GPU flags were specified in BOTH places"):
        rtb.get_batch_flags(core=core_params)

def test_generate_arg_for_batch(rtb, core_params):
    """Test batch command generation."""
    cmd = rtb.generate_arg_for_batch(
        priority="NORMAL",
        tuflowexe=core_params.tuflowexe,
        batch=["-b"],
        tcf=core_params.tcf,
        keys=["s1"],
        combo=("val1",),
        max_lengths={"s1": 4}
    )
    
    assert "START /NORMAL /WAIT" in cmd
    assert str(core_params.tuflowexe) in cmd
    assert "-b" in cmd
    assert "-s1" in cmd
    assert "val1" in cmd
    assert str(core_params.tcf) in cmd

# --- Additional Tests ---

def test_format_duration(rtb):
    """Test duration formatting utility."""
    assert rtb.format_duration(0) == "00:00:00"
    assert rtb.format_duration(59) == "00:00:59"
    assert rtb.format_duration(60) == "00:01:00"
    assert rtb.format_duration(3600) == "01:00:00"
    assert rtb.format_duration(3661) == "01:01:01"
    assert rtb.format_duration(86400) == "24:00:00"

def test_split_input_strings(rtb):
    """Test input string splitting."""
    # String input
    assert rtb.split_input_strings("a b c") == ["a", "b", "c"]
    assert rtb.split_input_strings("  a   b  ") == ["a", "b"]
    assert rtb.split_input_strings("") == []
    
    # List input
    assert rtb.split_input_strings(["a", "b"]) == ["a", "b"]
    assert rtb.split_input_strings(["  a  ", "b"]) == ["a", "b"]

def test_parse_input_files_gpu_stripped(rtb, tmp_path):
    """Test that -puN flags are stripped from parsed input files."""
    f = tmp_path / "runs.txt"
    f.write_text("-s1 A -pu0 -e1 10 -pu1\n-s1 B -e1 20", encoding="utf-8")
    
    combos, first_seen = rtb.parse_input_files([f])
    
    # GPU flags should be stripped, only s1 and e1 remain
    assert len(combos) == 2
    assert combos[0] == {"s1": "A", "e1": "10"}
    assert combos[1] == {"s1": "B", "e1": "20"}

def test_parse_input_files_various_comments(rtb, tmp_path):
    """Test parsing handles various comment styles."""
    f = tmp_path / "runs.txt"
    content = """
    REM This is a comment
    :: Another comment
    # Python style
    ; Semicolon
    // C++ style
    -s1 A -e1 10
    """
    f.write_text(content, encoding="utf-8")
    
    combos, _ = rtb.parse_input_files([f])
    assert len(combos) == 1
    assert combos[0] == {"s1": "A", "e1": "10"}

def test_priority_order_custom(rtb, core_params):
    """Test custom priority_order affects key ordering in command."""
    core_params.priority_order = "e1 s1"
    run_vars = {"s1": ["A"], "e1": ["10"]}
    params = rtb.Parameters(core_params=core_params, run_variables=run_vars)
    
    sims = rtb.compute_simulations(params)
    args = sims[0].args_for_python
    
    # Find positions of flags
    e1_idx = args.index("-e1")
    s1_idx = args.index("-s1")
    
    # e1 should come before s1 due to priority_order
    assert e1_idx < s1_idx

def test_empty_run_variables(rtb, core_params):
    """Test compute_simulations with no run variables."""
    params = rtb.Parameters(core_params=core_params, run_variables={})
    sims = rtb.compute_simulations(params)
    assert len(sims) == 0

def test_simulation_equality_and_hash(rtb):
    """Test Simulation equality ignores GPU assignment."""
    # Create two identical simulations
    sim1 = rtb.Simulation(
        args_for_python=["exe", "-b", "-s1", "A", "tcf.tcf"],
        command_for_batch="START /NORMAL ...",
        index=1
    )
    sim2 = rtb.Simulation(
        args_for_python=["exe", "-b", "-s1", "A", "tcf.tcf"],
        command_for_batch="START /NORMAL ...",
        index=2
    )
    
    # Should be equal despite different index
    assert sim1 == sim2
    assert hash(sim1) == hash(sim2)

def test_enforce_placeholders(rtb, core_params):
    """Test placeholder enforcement in textfiles mode."""
    # TCF expects ~s1~ and ~e1~
    combos = [
        {"s1": "A", "e1": "10"},       # Valid
        {"s1": "B"},                   # Missing e1 - should be filtered
        {"e1": "20"},                  # Missing s1 - should be filtered
        {"s1": "C", "e1": "30", "s2": "X"}  # Has extra s2, but has required
    ]
    
    filtered = rtb._enforce_placeholders(core=core_params, combos=combos)
    
    # Should only keep combos with both s1 and e1
    assert len(filtered) == 2
    # Note: _enforce_placeholders trims to only required keys
    assert filtered[0] == {"s1": "A", "e1": "10"}
    assert filtered[1] == {"s1": "C", "e1": "30"}
