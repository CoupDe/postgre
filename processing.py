import json
from typing import List, Tuple
from pydantic import BaseModel
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, asdict


@dataclass
class Estimate:  # Можно попробовать реализовать через Dataclass, но тогда неопходимо переопределить
    # наследование от Basemodel
    local_num: str
    workdoc_code: str
    type_work: str
    total_price: float
    rub: str
    construction_object: str
    price_year: str
    inventory_num: str
    date_parse: str
    estimate_path: dict
    id_estimate: int
    new_path: str


folder = r"C:\Users\Вадим\Desktop\Estimates"
dt_estimate = []


def get_data():
    estimate = [x for x in Path(folder).glob("**/*.json")]
    print("Файлов для обработки не найдено" if len(estimate) == 0 else f"Найдено {len(estimate)} файлов для обработки")
    for file in estimate:
        with open(str(file), "r", encoding="utf-8") as est:
            dt_estimate.append(json.load(est))
    print("Не все файлы обработаны" if len(estimate) != len(dt_estimate) else f"Обработаны все файлы")
    estimates: List[Estimate] = [Estimate(**dt) for dt in dt_estimate]
    return estimates
