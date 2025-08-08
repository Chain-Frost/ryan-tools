import pandas as pd
import matplotlib.pyplot as plt
import math
import warnings

# Parameters ---------------------------------------------------------------

# Define the region names
region_names: list[str] = [
    "East Coast North",
    "Semi-arid Inland QLD",
    "Tasmania",
    "SW WA",
    "Central NSW",
    "SE Coast",
    "Southern Semi-arid",
    "Southern Temperate",
    "Northern Coastal",
    "Inland Arid",
]

# Define the parameters for each region
params_data: dict[str, list[float]] = {
    "East Coast North": [
        0.327,
        0.241,
        0.448,
        0.36,
        0.00096,
        0.48,
        -0.21,
        0.012,
        -0.0013,
    ],
    "Semi-arid Inland QLD": [0.159, 0.283, 0.25, 0.308, 7.3e-07, 1, 0.039, 0, 0],
    "Tasmania": [0.0605, 0.347, 0.2, 0.283, 0.00076, 0.347, 0.0877, 0.012, -0.00033],
    "SW WA": [0.183, 0.259, 0.271, 0.33, 3.845e-06, 0.41, 0.55, 0.00817, -0.00045],
    "Central NSW": [
        0.265,
        0.241,
        0.505,
        0.321,
        0.00056,
        0.414,
        -0.021,
        0.015,
        -0.00033,
    ],
    "SE Coast": [0.06, 0.361, 0, 0.317, 8.11e-05, 0.651, 0, 0, 0],
    "Southern Semi-arid": [0.254, 0.247, 0.403, 0.351, 0.0013, 0.302, 0.058, 0, 0],
    "Southern Temperate": [
        0.158,
        0.276,
        0.372,
        0.315,
        0.000141,
        0.41,
        0.15,
        0.01,
        -0.0027,
    ],
    "Northern Coastal": [
        0.326,
        0.223,
        0.442,
        0.323,
        0.0013,
        0.58,
        -0.374,
        0.013,
        -0.0015,
    ],
    "Inland Arid": [0.297, 0.234, 0.449, 0.344, 0.00142, 0.216, 0.129, 0, 0],
}

# Create a DataFrame for parameters
params = pd.DataFrame(params_data, index=list("abcdefghi"))

# Functions ---------------------------------------------------------------


def ARF_long(area, duration, aep, region, params_df) -> None | int:
    """
    Calculate the long-duration ARF.

    Parameters:
    - area (float): Area in km-squared
    - duration (float): Duration in minutes
    - aep (float): Annual Exceedance Probability (fraction, e.g., 0.01)
    - region (str): Region name
    - params_df (pd.DataFrame): DataFrame containing parameters a-i for all regions

    Returns:
    - float: ARF value
    """
    # Validate region
    all_regions = params_df.columns.tolist()
    if region not in all_regions:
        valid_regions = ", ".join(all_regions)
        warnings.warn(f'Invalid region. You input "{region}". Valid regions are {valid_regions}')
        return None

    # Assign parameters a-i
    try:
        a, b, c, d, e, f, g, h, i = params_df[region].values
        # print(a, b, c, d, e, f, g, h, i)
    except Exception as ex:
        warnings.warn(f'Error retrieving parameters for region "{region}": {ex}')
        return None

    # Compute ARF
    try:
        term1 = a * (area**b - c * math.log10(duration)) * duration**-d
        term2 = e * (area**f) * (duration**g) * (0.3 + math.log10(aep))
        term3 = h * (10 ** (i * area * duration / 1440)) * (0.3 + math.log10(aep))
        arf = 1 - term1 + term2 + term3
        return min(1, arf)
    except ValueError as ve:
        warnings.warn(f"Error in ARF_long calculation: {ve}")
        return None
    except Exception as ex:
        warnings.warn(f"Unexpected error in ARF_long calculation: {ex}")
        return None


def ARF_short(area, duration, aep) -> int | None:
    """
    Calculate the short-duration ARF.

    Parameters:
    - area (float): Area in km-squared
    - duration (float): Duration in minutes
    - aep (float): Annual Exceedance Probability (fraction, e.g., 0.01)

    Returns:
    - float: ARF value
    """
    # Define constants
    a = 0.287
    b = 0.265
    c = 0.439
    d = 0.36
    e = 0.00226
    f = 0.226
    g = 0.125
    h = 0.0141
    i = -0.021
    j = 0.213

    try:
        term1 = a * (area**b - c * math.log10(duration)) * duration**-d
        term2 = e * (area**f) * (duration**g) * (0.3 + math.log10(aep))
        term3 = h * (area**j) * (10 ** (i * (1 / 1440) * (duration - 180) ** 2)) * (0.3 + math.log10(aep))
        arf = 1 - term1 + term2 + term3
        return min(1, arf)
    except ValueError as ve:
        warnings.warn(f"Error in ARF_short calculation: {ve}")
        return None
    except Exception as ex:
        warnings.warn(f"Unexpected error in ARF_short calculation: {ex}")
        return None


def ARF(area, duration, aep, region=None, params_df=None, neg_to_zero=True):
    """
    Calculate the Areal Reduction Factor (ARF).

    Parameters:
    - area (float): Area in km-squared
    - duration (float): Duration in minutes
    - aep (float): Annual Exceedance Probability (percentage, e.g., 1 for 1%)
    - region (str, optional): Region name
    - params_df (pd.DataFrame, optional): DataFrame containing parameters a-i for all regions
    - neg_to_zero (bool): If True, set negative ARF values to zero

    Returns:
    - float or None: ARF value or None if inputs are invalid
    """
    # Convert AEP from percentage to fraction
    aep_frac = aep / 100.0

    # Input validation
    if area < 0 or area > 30000:
        warnings.warn("Area must be between zero and 30,000 km-squared")
        return None

    if aep_frac > 0.5 or aep_frac < 0.005:
        warnings.warn("AEP must be between 0.5% and 50%, returning NA")
        return None

    if duration > 7 * 24 * 60 or duration < 0:
        warnings.warn("Duration must be positive and less than 10080 min (7 days)")
        return None

    if duration <= 720 and area > 1000:
        warnings.warn(
            "Generalized equations are not applicable for short durations when catchment areas exceed 1000 km-squared. "
            "If area > 1000, duration must be greater than 12 hours (720 mins)"
        )
        return None

    if area <= 1:
        return 1.0

    # Long Duration: duration >= 1440 minutes (24 hours)
    if duration >= 1440:
        if area >= 10:
            if region is None or params_df is None:
                warnings.warn(
                    "Region and params must be provided for long duration calculations with area >= 10 km-squared."
                )
                return None
            arf_long = ARF_long(area, duration, aep_frac, region, params_df)
            return arf_long

        else:
            # area < 10: interpolate between ARF_long for 10 km² and 1
            if region is None or params_df is None:
                warnings.warn("Region and params must be provided for interpolation.")
                return None
            arf_long_10 = ARF_long(10, duration, aep_frac, region, params_df)
            if arf_long_10 is None:
                return None
            arf = 1 - 0.6614 * (1 - arf_long_10) * (area**0.4 - 1)
            return arf

    # Short Duration: duration <= 720 minutes (12 hours)
    if duration <= 720:
        if area >= 10:
            if area > 1000:
                warnings.warn(
                    "Generalised equations are not applicable for short duration events on areas > 1000 km-squared."
                )
                return None
            arf_short = ARF_short(area, duration, aep_frac)
            if arf_short is None:
                return None
            if neg_to_zero:
                arf_short = max(0, arf_short)
            return arf_short

        else:
            # area < 10: interpolate between ARF_short for 10 km² and 1
            arf_short_10 = ARF_short(10, duration, aep_frac)
            if arf_short_10 is None:
                return None
            arf = 1 - 0.6614 * (1 - arf_short_10) * (area**0.4 - 1)
            return arf

    # Duration between 720 and 1440 minutes
    if 720 < duration < 1440:
        if 1 < area < 10:
            # Interpolate ARF for duration between 720 and 1440
            arf_long_24 = ARF_long(10, 1440, aep_frac, region, params_df)
            arf_short_12 = ARF_short(10, 720, aep_frac)
            if arf_long_24 is None or arf_short_12 is None:
                return None
            arf_interp_10 = arf_short_12 + (arf_long_24 - arf_short_12) * (duration - 720) / 720
            arf = 1 - 0.6614 * (1 - arf_interp_10) * (area**0.4 - 1)
            return arf
        elif area >= 10:
            # Interpolate ARF for duration between 720 and 1440 for area >= 10
            arf_long_24 = ARF_long(area, 1440, aep_frac, region, params_df)
            arf_short_12 = ARF_short(area, 720, aep_frac)
            if arf_long_24 is None or arf_short_12 is None:
                return None
            arf = arf_short_12 + (arf_long_24 - arf_short_12) * (duration - 720) / 720
            return arf

    # If none of the above conditions are met
    warnings.warn("Error in ARF calculations: Undefined case.")
    return None


def plot_arf_for_northern_coastal(params_df: pd.DataFrame) -> None:
    """
    Plot ARF (Areal Reduction Factor) against area for varying AEPs and discrete durations for 'Northern Coastal' region.

    Parameters:
    - params_df (pd.DataFrame): DataFrame containing parameters a-i for all regions
    """
    # Define values
    region = "Northern Coastal"
    aep_values = [1, 2, 5, 10, 20, 50]
    duration_values = [30, 60, 120, 180, 360, 720, 1440]  # from 30 mins to 1440 mins
    area_values = range(1, 350)  # area from 1 to 20 km²

    # Prepare plot
    plt.figure(figsize=(12, 8))

    for aep in aep_values:
        arf_data = []
        for area in area_values:
            arf_for_durations = [
                ARF(area, duration, aep, region=region, params_df=params_df) for duration in duration_values
            ]
            # Collect ARF data for maximum value across durations at each area and AEP
            arf_data.append(max(filter(lambda x: x is not None, arf_for_durations), default=0))

        plt.plot(area_values, arf_data, label=f"AEP {aep}%")

    # Label plot
    plt.xlabel("Area (km²)")
    plt.ylabel("ARF")
    plt.title(f"ARF vs Area for 'Northern Coastal' Region (Varying AEP, Discrete Durations)")
    plt.legend(title="AEP (%)")
    plt.grid(True)
    plt.show()


def plot_arf_for_northern_coastal_by_duration(params_df: pd.DataFrame) -> None:
    """
    Plot ARF (Areal Reduction Factor) against area for varying durations and a fixed AEP for 'Northern Coastal' region.

    Parameters:
    - params_df (pd.DataFrame): DataFrame containing parameters a-i for all regions
    """
    # Define values
    region = "Northern Coastal"
    aep = 2  # fixed AEP at 5%
    duration_values = [30, 60, 120, 180, 360, 720, 1440]  # discrete durations
    area_values = range(1, 350)  # area from 1 to 20 km²

    # Prepare plot
    plt.figure(figsize=(12, 8))

    for duration in duration_values:
        arf_data = []
        for area in area_values:
            arf_value = ARF(area, duration, aep, region=region, params_df=params_df)
            arf_data.append(arf_value if arf_value is not None else 0)

        plt.plot(area_values, arf_data, label=f"Duration {duration} mins")

    # Label plot
    plt.xlabel("Area (km²)")
    plt.ylabel("ARF")
    plt.title(f"ARF vs Area for 'Northern Coastal' Region (Fixed AEP = 5%, Varying Durations)")
    plt.legend(title="Duration (mins)")
    plt.grid(True)
    plt.show()


# Example Usage ------------------------------------------------------------

if __name__ == "__main__":
    # Example parameters
    example_area = 150  # km-squared
    example_duration = 720  # minutes (30 hours)
    example_aep = 2  # percent
    example_region = "Northern Coastal"

    arf_value = ARF(
        area=example_area,
        duration=example_duration,
        aep=example_aep,
        region=example_region,
        params_df=params,
        neg_to_zero=True,  # Set to True to mimic the default behavior in R
    )
    if arf_value is not None:
        print(f"The ARF is: {arf_value:.3f}")
    else:
        print("The ARF could not be calculated due to invalid inputs or an error in computation.")
    plot_arf_for_northern_coastal_by_duration(params)
    plot_arf_for_northern_coastal(params)

# based on https://gist.github.com/TonyLadson/fc870cf7ebfe39ea3d1a812bcc53c8fb

region_names: list[str] = [
    "East Coast North",
    "Semi-arid Inland QLD",
    "Tasmania",
    "SW WA",
    "Central NSW",
    "SE Coast",
    "Southern Semi-arid",
    "Southern Temperate",
    "Northern Coastal",
    "Inland Arid",
]
