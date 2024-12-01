import sys
from pathlib import Path

# Import the main function from the library
from ryan_library.scripts.tuflow_results_styling import apply_styles

# User Overrides: Define your custom QML paths here
# To override a QML path, uncomment and modify the following dictionary
# Example:
# user_qml_overrides = {
#     "d_Max": "/path/to/custom/depth_for_legend_max2m.qml",
#     "h_Max": "/path/to/custom/hmax.qml",
#     # Add other overrides as needed
# }

user_qml_overrides: dict = {
    # "d_Max": "/path/to/custom/depth_for_legend_max2m.qml",
    # "h_Max": "/path/to/custom/hmax.qml",
    # "V_Max": "/path/to/custom/velocities_scour_protection_mrwa.qml",
    # "DEM_Z": "/path/to/custom/hillshade.qml",
    # "1d_ccA_L": "/path/to/custom/1d_ccA.qml",
    # "DIFF_P2-P1": "/path/to/custom/Depth_Diff_GOOOD.qml",
    # "Results1D": "/path/to/custom/1d_ccA.qml",
}


def main() -> None:
    """
    Entry point for the TUFLOWResultsStyling script.
    """
    try:
        if any(user_qml_overrides.values()):
            # Remove entries that are not overridden
            overrides = {k: v for k, v in user_qml_overrides.items() if v}
            apply_styles(user_qml_overrides=overrides)
        else:
            apply_styles()
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
