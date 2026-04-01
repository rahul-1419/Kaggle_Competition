from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataExtractionConfig:
    root_dir: Path
    file_id: str
    local_data_file: Path
    unzip_dir: Path
    

@dataclass(frozen=True)
class DataTransformationConfig:
    root_dir: Path
    inventory: Path
    product: Path
    promotion: Path
    sale_q1: Path
    sale_q2: Path
    sale_q3: Path
    sale_q4: Path
    inventory_t: Path
    products_t: Path
    promotion_t: Path
    sales_t: Path


@dataclass(frozen=True)
class DataLoadingConfig():
    root_dir: Path
    inventory: Path
    products: Path
    promotion:Path
    sales: Path
    password: str