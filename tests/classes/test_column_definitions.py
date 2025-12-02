"""Unit tests for ryan_library.classes.column_definitions."""

import pytest
from ryan_library.classes.column_definitions import ColumnDefinition, ColumnMetadataRegistry


class TestColumnDefinition:
    """Tests for the ColumnDefinition dataclass."""

    def test_initialization(self):
        """Test that ColumnDefinition initializes correctly."""
        col_def = ColumnDefinition(name="TestCol", description="A test column", value_type="float")
        assert col_def.name == "TestCol"
        assert col_def.description == "A test column"
        assert col_def.value_type == "float"

    def test_default_value_type(self):
        """Test that value_type defaults to None."""
        col_def = ColumnDefinition(name="TestCol", description="A test column")
        assert col_def.value_type is None


class TestColumnMetadataRegistry:
    """Tests for the ColumnMetadataRegistry class."""

    def test_initialization(self):
        """Test initialization with custom definitions."""
        base_defs = {
            "ColA": ColumnDefinition("ColA", "Description A")
        }
        sheet_specific = {
            "Sheet1": {"ColA": ColumnDefinition("ColA", "Sheet1 Description A")}
        }
        registry = ColumnMetadataRegistry(base_definitions=base_defs, sheet_specific=sheet_specific)
        
        # Check base definition
        assert registry.definition_for("ColA").description == "Description A"
        
        # Check sheet specific override
        assert registry.definition_for("ColA", sheet_name="Sheet1").description == "Sheet1 Description A"

    def test_definition_for_base(self):
        """Test retrieval of base definitions."""
        registry = ColumnMetadataRegistry(base_definitions={"ColA": ColumnDefinition("ColA", "Desc A")})
        def_obj = registry.definition_for("ColA")
        assert def_obj.name == "ColA"
        assert def_obj.description == "Desc A"

    def test_definition_for_sheet_override(self):
        """Test that sheet-specific definitions override base ones."""
        registry = ColumnMetadataRegistry(
            base_definitions={"ColA": ColumnDefinition("ColA", "Base Desc")},
            sheet_specific={"Sheet1": {"ColA": ColumnDefinition("ColA", "Sheet Desc")}}
        )
        
        # Base
        assert registry.definition_for("ColA").description == "Base Desc"
        # Override
        assert registry.definition_for("ColA", sheet_name="Sheet1").description == "Sheet Desc"
        # Unrelated sheet should use base
        assert registry.definition_for("ColA", sheet_name="Sheet2").description == "Base Desc"

    def test_definition_for_fallback(self):
        """Test fallback for unknown columns."""
        registry = ColumnMetadataRegistry()
        def_obj = registry.definition_for("UnknownCol")
        assert def_obj.name == "UnknownCol"
        assert "TODO" in def_obj.description
        assert def_obj.value_type is None

    def test_iter_definitions(self):
        """Test retrieving multiple definitions in order."""
        registry = ColumnMetadataRegistry(base_definitions={
            "A": ColumnDefinition("A", "Desc A"),
            "B": ColumnDefinition("B", "Desc B")
        })
        
        defs = registry.iter_definitions(["B", "A", "C"])
        assert len(defs) == 3
        assert defs[0].name == "B"
        assert defs[1].name == "A"
        assert defs[2].name == "C"  # Fallback
        assert "TODO" in defs[2].description

    def test_default_singleton(self):
        """Test the default singleton instance."""
        reg1 = ColumnMetadataRegistry.default()
        reg2 = ColumnMetadataRegistry.default()
        assert reg1 is reg2
        
        # Check for some known standard columns
        assert reg1.definition_for("Time").value_type == "float"
        assert reg1.definition_for("Q").description is not None
        
        # Check for the recently updated H columns
        assert reg1.definition_for("US_H").name == "US_H"
        assert reg1.definition_for("DS_H").name == "DS_H"
