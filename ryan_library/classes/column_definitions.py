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
                "Type": ColumnDefinition(
                    name="Type",
                    description="2d_po quantity type (for example Flow, Water Level, Velocity).",
                    value_type="string",
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
