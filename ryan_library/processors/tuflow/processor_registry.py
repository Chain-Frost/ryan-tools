# ryan_library/processors/tuflow/processor_registry.py
from typing import Dict, Type, Optional
from .base_processor import BaseProcessor
from loguru import logger


class ProcessorRegistry:
    """
    Registry to map data types to their corresponding processor classes.
    """

    _registry: dict[str, Type[BaseProcessor]] = {}

    @classmethod
    def register_processor(cls, data_type: str):
        """
        Class decorator to register a processor class with a specific data type.

        Args:
            data_type (str): The data type the processor handles.
        """

        def decorator(processor_cls: Type[BaseProcessor]):
            data_type_lower = data_type.lower()
            if data_type_lower in cls._registry:
                logger.warning(
                    f"Processor for data type '{data_type}' is already registered. Overwriting with '{processor_cls.__name__}'."
                )
            cls._registry[data_type_lower] = processor_cls
            logger.debug(
                f"Registered processor '{processor_cls.__name__}' for data type '{data_type}'."
            )
            return processor_cls

        return decorator

    @classmethod
    def get_processor(cls, data_type: str) -> Optional[Type[BaseProcessor]]:
        """
        Retrieve the processor class for a given data type.

        Args:
            data_type (str): The data type of the file.

        Returns:
            Optional[Type[BaseProcessor]]: The processor class if found, else None.
        """
        return cls._registry.get(data_type.lower())

    @classmethod
    def list_registered_processors(cls) -> dict[str, Type[BaseProcessor]]:
        """
        List all registered processors.

        Returns:
            dict[str, Type[BaseProcessor]]: Mapping of data types to processor classes.
        """
        return cls._registry.copy()
