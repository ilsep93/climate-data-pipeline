from pathlib import Path

from climatology import ChelsaProduct
from functions import yearly_table_generator


def process_yearly_table(product: ChelsaProduct,
                         zonal_dir: Path,
                         out_path: Path,
                         sort_values: list[str]):
    
    yearly_table = yearly_table_generator(product=product,
                                          zonal_dir=zonal_dir,
                                          sort_values=sort_values)
    
    yearly_table.to_csv(out_path, encoding='utf-8', index=False)