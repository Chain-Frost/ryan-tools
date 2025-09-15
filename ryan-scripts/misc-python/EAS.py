# https://tonyladson.wordpress.com/2017/03/04/on-the-calculation-of-equal-area-slope/
# https://gist.github.com/TonyLadson/bf787b9c3d4851b1caef778ee3d1a59f

"""
calc_slope_ea.py

Description:
    This script calculates the equal area slope (slope_ea) for a given stream profile.
    It generates a random stream profile for testing, visualizes the profile along with
    the equal area slope, defines polygons to verify equal areas, and saves the plots
    to the script's directory.

Usage:
    python calc_slope_ea.py

Dependencies:
    - numpy
    - pandas
    - matplotlib
    - scipy
    - shapely
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d
from scipy.optimize import brentq
from shapely.geometry import Polygon
from pathlib import Path


def calc_slope_ea(x: np.ndarray, y: np.ndarray) -> float:
    """
    Calculate the equal area slope.

    Parameters:
        x (np.ndarray): Distance along the stream in km
        y (np.ndarray): Elevation along the stream in m

    Returns:
        float: Equal area slope ordinate (h) in m/km
    """
    y_normalized = y - np.min(y)

    # Calculate area under the curve using the trapezoidal rule
    A = np.trapezoid(y_normalized, x)  # Units: m·km

    # Total length in km
    L = x[-1] - x[0]

    # Calculate equal area slope
    h = 2 * A / (L**2)  # Units: m/m

    return h


def generate_random_profile(seed: int = 11, num_points: int = 100) -> pd.DataFrame:
    """
    Generate a random stream profile for testing.

    Parameters:
        seed (int): Seed for random number generator
        num_points (int): Number of points in the profile

    Returns:
        pd.DataFrame: DataFrame containing 'x' and 'y' columns
    """
    np.random.seed(seed)

    # Generate cumulative sum of random distances (km)
    x_steps = np.random.uniform(0.1, 0.4, num_points)
    x = np.cumsum(x_steps)

    # Generate elevation steps:
    # First 5 steps: 0.1 to 0.3 m
    # Next 3 steps: 0.3 to 1 m
    # Last 2 steps: 2 to 4 m
    steps1 = int(num_points * 0.5)
    steps2 = int(num_points * 0.3)
    steps3 = num_points - steps1 - steps2
    y_steps_part1 = np.random.uniform(0.1, 0.3, steps1)
    y_steps_part2 = np.random.uniform(0.3, 1.0, steps2)
    y_steps_part3 = np.random.uniform(2.0, 4.0, steps3)
    y_steps = np.concatenate([y_steps_part1, y_steps_part2, y_steps_part3])
    y = np.cumsum(y_steps)

    print(x, y)
    profile_df = pd.DataFrame({"x": x, "y": y})

    # Adjust y so that the elevation of the outlet is zero
    profile_df["y"] -= profile_df["y"].min()

    return profile_df


def plot_stream_profile(profile_df: pd.DataFrame, save_path: Path):
    """
    Plot the stream profile and save the plot.

    Parameters:
        profile_df (pd.DataFrame): DataFrame containing 'x' and 'y' columns
        save_path (Path): Path to save the plot image
    """
    plt.figure(figsize=(8, 6))
    plt.plot(profile_df["x"], profile_df["y"], marker="o", linestyle="-")
    plt.xlabel("Distance (km)")
    plt.ylabel("Elevation (m)")
    plt.title("Stream Profile")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()
    print(f"Stream profile plot saved to {save_path}")


def plot_stream_profile_with_slope(profile_df: pd.DataFrame, h: float, save_path: Path):
    """
    Plot the stream profile with the equal area slope and save the plot.

    Parameters:
        profile_df (pd.DataFrame): DataFrame containing 'x' and 'y' columns
        h (float): Equal area slope ordinate (m/km)
        save_path (Path): Path to save the plot image
    """
    L = profile_df["x"].max() - profile_df["x"].min()
    C_y = profile_df["y"].min() + h * L

    plt.figure(figsize=(8, 6))
    plt.plot(profile_df["x"], profile_df["y"], marker="o", linestyle="-", label="Profile")
    plt.scatter(profile_df["x"].max(), C_y, color="blue", s=50, label="Equal Area Slope Point")
    plt.plot(
        [profile_df["x"].min(), profile_df["x"].max()],
        [profile_df["y"].min(), C_y],
        linestyle="--",
        color="blue",
        label="Equal Area Slope Line",
    )
    plt.xlabel("Distance (km)")
    plt.ylabel("Elevation (m)")
    plt.title("Stream Profile with Equal Area Slope")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()
    print(f"Stream profile with equal area slope plot saved to {save_path}")


def plot_stream_profile_with_polygons(
    profile_df: pd.DataFrame,
    h: float,
    intersection: tuple,
    poly1_df: pd.DataFrame,
    poly2_df: pd.DataFrame,
    save_path: Path,
):
    """
    Plot the stream profile with equal area slope, intersection, and polygons, then save the plot.

    Parameters:
        profile_df (pd.DataFrame): DataFrame containing 'x' and 'y' columns
        h (float): Equal area slope ordinate (m/km)
        intersection (tuple): (x_int, y_int) intersection point
        poly1_df (pd.DataFrame): DataFrame for Polygon A1
        poly2_df (pd.DataFrame): DataFrame for Polygon A2
        save_path (Path): Path to save the plot image
    """
    L = profile_df["x"].max() - profile_df["x"].min()
    C_y = profile_df["y"].min() + h * L

    plt.figure(figsize=(8, 6))
    plt.plot(profile_df["x"], profile_df["y"], marker="o", linestyle="-", label="Profile")
    plt.scatter(profile_df["x"].max(), C_y, color="blue", s=50, label="Equal Area Slope Point")
    plt.plot(
        [profile_df["x"].min(), profile_df["x"].max()],
        [profile_df["y"].min(), C_y],
        linestyle="--",
        color="blue",
        label="Equal Area Slope Line",
    )

    if intersection[0] is not None:
        # Plot intersection point
        plt.scatter(
            intersection[0],
            intersection[1],
            color="red",
            s=50,
            label="Intersection Point",
        )

        # Plot polygons
        polygon1 = Polygon(zip(poly1_df["x"], poly1_df["y"]))
        polygon2 = Polygon(zip(poly2_df["x"], poly2_df["y"]))

        x_poly1, y_poly1 = polygon1.exterior.xy
        x_poly2, y_poly2 = polygon2.exterior.xy

        plt.fill(x_poly1, y_poly1, color="blue", alpha=0.2, label="Polygon A1")
        plt.fill(x_poly2, y_poly2, color="green", alpha=0.2, label="Polygon A2")

    plt.xlabel("Distance (km)")
    plt.ylabel("Elevation (m)")
    plt.title("Stream Profile with Equal Area Slope and Polygons")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()
    print(f"Stream profile with polygons plot saved to {save_path}")


def find_intersection(profile_df: pd.DataFrame, h: float) -> tuple:
    """
    Find the intersection point between the stream profile and the equal area slope line.

    Parameters:
        profile_df (pd.DataFrame): DataFrame containing 'x' and 'y' columns
        h (float): Equal area slope ordinate (m/km)

    Returns:
        tuple: (x_int, y_int) intersection point or (None, None) if no intersection found
    """
    # Define interpolated functions
    L = profile_df["x"].max() - profile_df["x"].min()
    slope_ea_y_func = interp1d(
        [profile_df["x"].min(), profile_df["x"].max()],
        [profile_df["y"].min(), profile_df["y"].min() + h * L],
        kind="linear",
        fill_value="extrapolate",
    )

    profile_y_func = interp1d(profile_df["x"], profile_df["y"], kind="linear", fill_value="extrapolate")

    # Define the difference function
    def calc_ydiff(x_val):
        return profile_y_func(x_val) - slope_ea_y_func(x_val)

    # Attempt to find the root (intersection)
    try:
        x_int = brentq(calc_ydiff, profile_df["x"].iloc[1], profile_df["x"].max())
        y_int = slope_ea_y_func(x_int)
        print(f"Intersection found at x = {x_int:.4f} km, y = {y_int:.4f} m")
        return x_int, y_int
    except ValueError:
        print("No intersection found between profile and slope line.")
        return None, None


def define_polygons(profile_df: pd.DataFrame, intersection: tuple, h: float) -> tuple:
    """
    Define the two polygons based on the intersection point.

    Parameters:
        profile_df (pd.DataFrame): DataFrame containing 'x' and 'y' columns
        intersection (tuple): (x_int, y_int) intersection point
        h (float): Equal area slope ordinate (m/km)

    Returns:
        tuple: (poly1_df, poly2_df) DataFrames for Polygon A1 and Polygon A2
    """
    if intersection[0] is None:
        return pd.DataFrame(), pd.DataFrame()

    x_int, y_int = intersection
    L = profile_df["x"].max() - profile_df["x"].min()

    # Polygon A1: Below the intersection
    mask1 = profile_df["x"] < x_int
    x1 = profile_df.loc[mask1, "x"].tolist()
    y1 = profile_df.loc[mask1, "y"].tolist()
    x1.append(x_int)
    y1.append(y_int)
    x1.append(profile_df["x"].min())
    y1.append(profile_df["y"].min())

    poly1_df = pd.DataFrame({"x": x1, "y": y1})

    # Polygon A2: Above the intersection
    mask2 = profile_df["x"] > x_int
    x2 = [x_int] + profile_df.loc[mask2, "x"].tolist() + [profile_df["x"].max(), x_int]
    y2 = [y_int] + profile_df.loc[mask2, "y"].tolist() + [profile_df["y"].min() + h * L, y_int]

    poly2_df = pd.DataFrame({"x": x2, "y": y2})

    return poly1_df, poly2_df


def calculate_polygon_areas(poly1_df: pd.DataFrame, poly2_df: pd.DataFrame) -> tuple:
    """
    Calculate the areas of the two polygons.

    Parameters:
        poly1_df (pd.DataFrame): DataFrame for Polygon A1
        poly2_df (pd.DataFrame): DataFrame for Polygon A2

    Returns:
        tuple: (A1, A2) areas of Polygon A1 and Polygon A2
    """
    if poly1_df.empty or poly2_df.empty:
        return 0.0, 0.0

    polygon1 = Polygon(zip(poly1_df["x"], poly1_df["y"]))
    polygon2 = Polygon(zip(poly2_df["x"], poly2_df["y"]))

    A1 = polygon1.area
    A2 = polygon2.area

    return A1, A2


def main():
    # Determine the script's directory
    script_dir = Path(__file__).absolute().parent

    # Generate a random stream profile
    profile_df = generate_random_profile()

    # Save the stream profile plot
    profile_plot_path = script_dir / "stream_profile.png"
    plot_stream_profile(profile_df, profile_plot_path)

    # Calculate the equal area slope
    slope_ea = calc_slope_ea(profile_df["x"].values, profile_df["y"].values)
    print(f"Calculated Equal Area Slope (h): {slope_ea:.6f} m/km")

    # Save the stream profile with equal area slope plot
    profile_slope_plot_path = script_dir / "stream_profile_with_slope.png"
    plot_stream_profile_with_slope(profile_df, slope_ea, profile_slope_plot_path)

    # Find the intersection point
    intersection = find_intersection(profile_df, slope_ea)

    # Define polygons based on the intersection
    poly1_df, poly2_df = define_polygons(profile_df, intersection, slope_ea)

    if intersection[0] is not None:
        # Save the stream profile with polygons plot
        profile_polygons_plot_path = script_dir / "stream_profile_with_polygons.png"
        plot_stream_profile_with_polygons(
            profile_df,
            slope_ea,
            intersection,
            poly1_df,
            poly2_df,
            profile_polygons_plot_path,
        )

        # Calculate areas of the polygons
        A1, A2 = calculate_polygon_areas(poly1_df, poly2_df)
        print(f"Area A1: {A1:.6f} (km·m)")
        print(f"Area A2: {A2:.6f} (km·m)")
        print(f"A1 and A2 are equal: {np.isclose(A1, A2, atol=1e-6)}")

    # Calculate test slope
    A_total = np.trapz(profile_df["y"] - profile_df["y"].min(), profile_df["x"])
    L = profile_df["x"].iloc[-1] - profile_df["x"].iloc[0]
    test_slope_ea = (2 * A_total) / (1000 * L**2)  # Correct formula with 1000 factor
    print(f"Test Slope EA: {test_slope_ea:.6f} m/km")

    # Compare test_slope_ea with calculated slope_ea
    print(f"All equal (test_slope_ea == slope_ea): {np.isclose(test_slope_ea, slope_ea, atol=1e-6)}")


if __name__ == "__main__":
    main()
