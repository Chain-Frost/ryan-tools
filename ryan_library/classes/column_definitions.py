"""Central registry for column descriptions used across exports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar
from _collections_abc import Iterable, Mapping


@dataclass(frozen=True, slots=True)
class ColumnDefinition:
    """Describe the intent of a DataFrame column."""

    name: str
    description: str
    value_type: str | None = None


class ColumnMetadataRegistry:
    """Registry providing consistent column descriptions across exports."""

    _BASE_DEFINITIONS: ClassVar[Mapping[str, ColumnDefinition]]
    _SHEET_SPECIFIC_DEFINITIONS: ClassVar[Mapping[str, Mapping[str, ColumnDefinition]]]

    def __init__(
        self,
        base_definitions: Mapping[str, ColumnDefinition] | None = None,
        sheet_specific: Mapping[str, Mapping[str, ColumnDefinition]] | None = None,
    ) -> None:
        self._base_definitions: Mapping[str, ColumnDefinition] = base_definitions or {}
        self._sheet_specific: Mapping[str, Mapping[str, ColumnDefinition]] = sheet_specific or {}

    def definition_for(self, column_name: str, sheet_name: str | None = None) -> ColumnDefinition:
        """Return a :class:`ColumnDefinition` for ``column_name``.

        Sheet-specific definitions override base definitions. If no definition
        exists a placeholder entry is returned so that missing descriptions are
        easy to spot in the exported workbook.
        """

        if sheet_name and sheet_name in self._sheet_specific:
            sheet_def: Mapping[str, ColumnDefinition] = self._sheet_specific[sheet_name]
            if column_name in sheet_def:
                return sheet_def[column_name]

        if column_name in self._base_definitions:
            return self._base_definitions[column_name]

        return ColumnDefinition(
            name=column_name,
            description=f"TODO: add description for '{column_name}'.",
            value_type=None,
        )

    def iter_definitions(self, column_names: Iterable[str], sheet_name: str | None = None) -> list[ColumnDefinition]:
        """Return definitions for ``column_names`` preserving order."""

        return [self.definition_for(column_name=col, sheet_name=sheet_name) for col in column_names]

    @classmethod
    def default(cls) -> "ColumnMetadataRegistry":
        """Return the default registry instance."""

        if not hasattr(cls, "_INSTANCE"):
            base_definitions: dict[str, ColumnDefinition] = {
                "AbsMax": ColumnDefinition(
                    name="AbsMax",
                    description="Absolute maximum magnitude observed within the event time-series.",
                    value_type="float",
                ),
                "AbsValue": ColumnDefinition(
                    name="AbsValue",
                    description="Absolute value of the 2d_po timeseries result (ignoring sign).",
                    value_type="float",
                ),
                "SignedAbsMax": ColumnDefinition(
                    name="SignedAbsMax",
                    description="Absolute maximum magnitude preserving the original sign (positive/negative).",
                    value_type="float",
                ),
                "Max": ColumnDefinition(
                    name="Max",
                    description="Maximum value in the event window.",
                    value_type="float",
                ),
                "Min": ColumnDefinition(
                    name="Min",
                    description="Minimum value in the event window.",
                    value_type="float",
                ),
                "Area_Culv": ColumnDefinition(
                    name="Area_Culv",
                    description="Full cross-sectional area of the culvert barrel reported by ccA outputs.",
                    value_type="float",
                ),
                "Area_Max": ColumnDefinition(
                    name="Area_Max",
                    description="Maximum wetted area recorded for the culvert during the event (ccA output).",
                    value_type="float",
                ),
                "Tmax": ColumnDefinition(
                    name="Tmax",
                    description="Time (hours) at which the maximum value occurs.",
                    value_type="float",
                ),
                "Tmin": ColumnDefinition(
                    name="Tmin",
                    description="Time (hours) at which the minimum value occurs.",
                    value_type="float",
                ),
                "Location": ColumnDefinition(
                    name="Location",
                    description="Model result location identifier from the 2d_po file.",
                    value_type="string",
                ),
                "Chan ID": ColumnDefinition(
                    name="Chan ID",
                    description="Channel identifier from the 1d_nwk file.",
                    value_type="string",
                ),
                "ID": ColumnDefinition(
                    name="ID",
                    description="Reporting Location Line identifier from the RLL outputs.",
                    value_type="string",
                ),
                "Location ID": ColumnDefinition(
                    name="Location ID",
                    description=(
                        "Normalized location identifier used when grouping maximums across different source types."
                    ),
                    value_type="string",
                ),
                "Time": ColumnDefinition(
                    name="Time",
                    description="Simulation time (hours) corresponding to the recorded statistic.",
                    value_type="float",
                ),
                "Q": ColumnDefinition(
                    name="Q",
                    description="Discharge/flow rate reported for the location.",
                    value_type="float",
                ),
                "V": ColumnDefinition(
                    name="V",
                    description="Velocity reported for the location.",
                    value_type="float",
                ),
                "Type": ColumnDefinition(
                    name="Type",
                    description="2d_po quantity type (for example Flow, Water Level, Velocity).",
                    value_type="string",
                ),
                "DS_h": ColumnDefinition(
                    name="DS_h",
                    description="Downstream water level extracted from maximum-result CSVs (usually metres AHD).",
                    value_type="float",
                ),
                "US_h": ColumnDefinition(
                    name="US_h",
                    description="Upstream water level extracted from maximum-result CSVs (usually metres AHD).",
                    value_type="float",
                ),
                "DS_H": ColumnDefinition(
                    name="DS_H",
                    description="Downstream water level taken from the 1d_H timeseries output.",
                    value_type="float",
                ),
                "US_H": ColumnDefinition(
                    name="US_H",
                    description="Upstream water level taken from the 1d_H timeseries output.",
                    value_type="float",
                ),
                "US Invert": ColumnDefinition(
                    name="US Invert",
                    description="Invert elevation at the upstream end of the culvert/channel.",
                    value_type="float",
                ),
                "DS Invert": ColumnDefinition(
                    name="DS Invert",
                    description="Invert elevation at the downstream end of the culvert/channel.",
                    value_type="float",
                ),
                "US Obvert": ColumnDefinition(
                    name="US Obvert",
                    description="Crown/obvert elevation at the upstream end of the culvert/channel.",
                    value_type="float",
                ),
                "Height": ColumnDefinition(
                    name="Height",
                    description="Barrel height or diameter used for the culvert/channel definition.",
                    value_type="float",
                ),
                "num_barrels": ColumnDefinition(
                    name="num_barrels",
                    description="Estimated number of circular barrels derived from Area_Culv (1d_ccA_) and Height for C-type culverts.",
                    value_type="int",
                ),
                "Length": ColumnDefinition(
                    name="Length",
                    description="Culvert or channel length taken from the 1d_Chan file.",
                    value_type="float",
                ),
                "n or Cd": ColumnDefinition(
                    name="n or Cd",
                    description="Manning's n roughness (for open channels) or discharge coefficient (for culverts).",
                    value_type="float",
                ),
                "pSlope": ColumnDefinition(
                    name="pSlope",
                    description="Design slope for the structure (percent).",
                    value_type="float",
                ),
                "pBlockage": ColumnDefinition(
                    name="pBlockage",
                    description="Blockage percentage applied to the culvert/barrel.",
                    value_type="float",
                ),
                "Flags": ColumnDefinition(
                    name="Flags",
                    description="Source flags describing channel or culvert configuration issues.",
                    value_type="string",
                ),
                "H": ColumnDefinition(
                    name="H",
                    description="Water level reported for the location at the time of the maximum flow event.",
                    value_type="float",
                ),
                "dQ": ColumnDefinition(
                    name="dQ",
                    description="Differential flow reported by the RLL maximum output.",
                    value_type="float",
                ),
                "Time dQ": ColumnDefinition(
                    name="Time dQ",
                    description="Time associated with the differential flow reported by the RLL output.",
                    value_type="float",
                ),
                "aep_text": ColumnDefinition(
                    name="aep_text",
                    description="Annual exceedance probability label parsed from the run code (e.g. '01p').",
                    value_type="string",
                ),
                "aep_numeric": ColumnDefinition(
                    name="aep_numeric",
                    description="Annual exceedance probability represented as a numeric percentage e.g 1.",
                    value_type="float",
                ),
                "duration_text": ColumnDefinition(
                    name="duration_text",
                    description="Storm duration label parsed from the run code (e.g. '00030m').",
                    value_type="string",
                ),
                "duration_numeric": ColumnDefinition(
                    name="duration_numeric",
                    description="Storm duration represented as a numeric value (mins - tuflow style).",
                    value_type="float",
                ),
                "tp_text": ColumnDefinition(
                    name="tp_text",
                    description="Temporal pattern identifier parsed from the run code. e.g. TP07",
                    value_type="string",
                ),
                "tp_numeric": ColumnDefinition(
                    name="tp_numeric",
                    description="Temporal pattern identifier represented as a numeric value. e.g. 1",
                    value_type="int",
                ),
                "trim_runcode": ColumnDefinition(
                    name="trim_runcode",
                    description="Run code without the AEP, TP and Duration component. Used to group comparable scenarios.",
                    value_type="string",
                ),
                "internalName": ColumnDefinition(
                    name="internalName",
                    description="Full run code derived from the source file name.",
                    value_type="string",
                ),
                "file": ColumnDefinition(
                    name="file",
                    description="Name of the source CSV file that contributed the row.",
                    value_type="string",
                ),
                "path": ColumnDefinition(
                    name="path",
                    description="Absolute path to the source CSV file.",
                    value_type="string",
                ),
                "rel_path": ColumnDefinition(
                    name="rel_path",
                    description="Source CSV path relative to the working directory when processing.",
                    value_type="string",
                ),
                "directory_path": ColumnDefinition(
                    name="directory_path",
                    description="Absolute directory containing the source CSV file.",
                    value_type="string",
                ),
                "rel_directory": ColumnDefinition(
                    name="rel_directory",
                    description="Directory containing the source CSV file relative to the working directory.",
                    value_type="string",
                ),
                "R01": ColumnDefinition(
                    name="R01",
                    description="First segment of the run code.",
                    value_type="string",
                ),
                "R02": ColumnDefinition(
                    name="R02",
                    description="Second segment of the run code.",
                    value_type="string",
                ),
                "R03": ColumnDefinition(
                    name="R03",
                    description="Third segment of the run code.",
                    value_type="string",
                ),
                "R04": ColumnDefinition(
                    name="R04",
                    description="Fourth segment of the run code.",
                    value_type="string",
                ),
                "R05": ColumnDefinition(
                    name="R05",
                    description="Fifth segment of the run code.",
                    value_type="string",
                ),
                "Value": ColumnDefinition(
                    name="Value",
                    description="Raw 2d_po metric captured at the reporting location.",
                    value_type="float",
                ),
                "pFull_Max": ColumnDefinition(
                    name="pFull_Max",
                    description="Maximum percent full reported by ccA.",
                    value_type="float",
                ),
                "pTime_Full": ColumnDefinition(
                    name="pTime_Full",
                    description="Time (hours) at which ccA reports the culvert as maximum percent full.",
                    value_type="float",
                ),
                "Dur_Full": ColumnDefinition(
                    name="Dur_Full",
                    description="Duration (hours) the culvert remained full according to ccA.",
                    value_type="float",
                ),
                "Dur_10pFull": ColumnDefinition(
                    name="Dur_10pFull",
                    description="Duration (hours) the culvert remained above 10 percent full (ccA output).",
                    value_type="float",
                ),
                "Sur_CD": ColumnDefinition(
                    name="Sur_CD",
                    description="Surcharge coefficient/depth reported by ccA when the culvert is surcharged.",
                    value_type="float",
                ),
                "Dur_Sur": ColumnDefinition(
                    name="Dur_Sur",
                    description="Duration (hours) the culvert experienced surcharge conditions.",
                    value_type="float",
                ),
                "pTime_Sur": ColumnDefinition(
                    name="pTime_Sur",
                    description="Time (hours) at which the surcharge condition peaked.",
                    value_type="float",
                ),
                "TFirst_Sur": ColumnDefinition(
                    name="TFirst_Sur",
                    description="Time (hours) when the surcharge condition first began.",
                    value_type="float",
                ),
                "MedianAbsMax": ColumnDefinition(
                    name="MedianAbsMax",
                    description="Absolute maxima across median of temporal patterns for the group.",
                    value_type="float",
                ),
                "median_duration": ColumnDefinition(
                    name="median_duration",
                    description="Duration associated with the MedianAbsMax.",
                    value_type="string",
                ),
                "median_TP": ColumnDefinition(
                    name="median_TP",
                    description="Temporal pattern associated with the MedianAbsMax.",
                    value_type="string",
                ),
                "mean_including_zeroes": ColumnDefinition(
                    name="mean_including_zeroes",
                    description="Mean of the statistic including zero values within the group.",
                    value_type="float",
                ),
                "mean_excluding_zeroes": ColumnDefinition(
                    name="mean_excluding_zeroes",
                    description="Mean of the statistic excluding zero values within the group.",
                    value_type="float",
                ),
                "mean_PeakFlow": ColumnDefinition(
                    name="mean_PeakFlow",
                    description=(
                        "Peak flow from the event whose statistic is nearest to the group's arithmetic mean; "
                        "not an averaged peak flow. Uses mean_including_zeroes."
                    ),
                    value_type="float",
                ),
                "mean_Duration": ColumnDefinition(
                    name="mean_Duration",
                    description=("Duration taken from the same nearest-to-mean event used for mean_PeakFlow."),
                    value_type="string",
                ),
                "mean_TP": ColumnDefinition(
                    name="mean_TP",
                    description=("Temporal pattern taken from the same nearest-to-mean event used for mean_PeakFlow."),
                    value_type="string",
                ),
                "low": ColumnDefinition(
                    name="low",
                    description="Minimum statistic encountered across all temporal patterns in the group.",
                    value_type="float",
                ),
                "high": ColumnDefinition(
                    name="high",
                    description="Maximum statistic encountered across all temporal patterns in the group.",
                    value_type="float",
                ),
                "count": ColumnDefinition(
                    name="count",
                    description="Number of rows contributing to the mean/median statistics for the selected duration.",
                    value_type="int",
                ),
                "count_bin": ColumnDefinition(
                    name="count_bin",
                    description="Total number of records considered across all durations for the group.",
                    value_type="int",
                ),
                "mean_storm_is_median_storm": ColumnDefinition(
                    name="mean_storm_is_median_storm",
                    description="Deprecated. Don't use. Indicates whether the mean storm matches the median storm selection.",
                    value_type="boolean",
                ),
                "aep_dur_bin": ColumnDefinition(
                    name="aep_dur_bin",
                    description="Count of records in the original AEP/Duration/Location/Type/run combination.",
                    value_type="int",
                ),
                "aep_bin": ColumnDefinition(
                    name="aep_bin",
                    description="Count of records in the original AEP/Location/Type/run combination.",
                    value_type="int",
                ),
            }

            sheet_specific: dict[str, dict[str, ColumnDefinition]] = {
                "aep-dur-max": {
                    "AbsMax": ColumnDefinition(
                        name="AbsMax",
                        description="Peaks for the AEP/Duration/Location/Type/run grouping.",
                        value_type="float",
                    ),
                },
                "aep-max": {
                    "AbsMax": ColumnDefinition(
                        name="AbsMax",
                        description="Peaks for the AEP/Location/Type/run grouping.",
                        value_type="float",
                    ),
                },
                "aep-dur-med": {
                    "MedianAbsMax": ColumnDefinition(
                        name="MedianAbsMax",
                        description="Medians for the specific AEP/Duration/Location/Type/run grouping.",
                        value_type="float",
                    ),
                },
                "aep-med-max": {
                    "MedianAbsMax": ColumnDefinition(
                        name="MedianAbsMax",
                        description="Medians the maximum median per AEP/Location/Type/run grouping.",
                        value_type="float",
                    ),
                },
            }

            cls._INSTANCE = cls(
                base_definitions=base_definitions,
                sheet_specific=sheet_specific,
            )
        return cls._INSTANCE


__all__: list[str] = ["ColumnDefinition", "ColumnMetadataRegistry"]
